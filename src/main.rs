#![allow(unused)]
#![warn(unused_must_use)]

extern crate fuse;
extern crate time;
extern crate rand;
extern crate chan_signal;

use std::env;
use std::path::Path;
use time::Timespec;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyWrite};
use std::time::Instant;
use std::time::Duration;

use std::sync::{Condvar, Mutex, MutexGuard};
use std::sync::Arc;
use std::io::{Write,Read,Seek,SeekFrom};
use std::cell::RefCell;
use rand::{Rng,thread_rng};
use std::cell::Cell;

type BlockIndex = u64;


trait LikeFile : Read + Write + Seek {}
impl<T> LikeFile for T where T : Read + Write + Seek {}

#[derive(Eq,PartialEq)]
struct DelayedWriteback {
    to_be_written_at : Instant,
    block_index : BlockIndex,
}

type BlockCache = std::collections::BTreeMap<BlockIndex, Vec<u8>>;
type Queue = std::collections::BinaryHeap<DelayedWriteback>;

#[derive(Default)]
struct CacheState {
    cache: BlockCache,
    queue: Queue,
}

struct WritebackThread<F : LikeFile> {
    s : Mutex<CacheState>,
    attention: Condvar,
    writeback_completed: Condvar,
    please_stop: Mutex<bool>,
    f: Mutex<F>,
    blocksize: u64,
    
}

struct VirtualFile<'a, F: LikeFile + 'a> {
    cursor: u64,
    maxdirtyblocks: u64,
    mindelay: Duration,
    maxdelay: Duration,
    w: &'a WritebackThread<F>,
}

impl<F> WritebackThread<F> where F : LikeFile {
    fn new(file: F, blocksize: u64) -> WritebackThread<F> { 
        WritebackThread {
            s: Mutex::new(Default::default()),
            attention: Condvar::new(),
            writeback_completed: Condvar::new(),
            please_stop: Mutex::new(false),
            f: Mutex::new(file),
            blocksize: blocksize,
        }
    }
    
    fn into_file(self) -> F { self.f.into_inner().unwrap() }
    
    fn get_virtual_file<'a>(&'a self, maxdirtyblocks: u64, mindelay: Duration, maxdelay: Duration) -> VirtualFile<'a, F> {
        VirtualFile {
            cursor: 0,
            maxdirtyblocks: maxdirtyblocks,
            mindelay: mindelay,
            maxdelay: maxdelay,
            w: self,
        }
    }
    
    fn run(&self) {
        
        let mut writeback : Option<(SeekFrom, Vec<u8>)>;
        
        writeback = None;
        'outer: loop {
            if let Some((seekpos, data)) = writeback {
                let mut file = self.f.lock().unwrap();
                file.seek(seekpos).expect("seek failed");
                file.write(data.as_ref()).expect("write failed");
                writeback = None;
                self.writeback_completed.notify_one();
            }
            
            let mut g = self.s.lock().unwrap();
            
           'inner: loop {
                if *self.please_stop.lock().unwrap() { break 'outer; }
                
                let timetowait : Option<Duration>;
                
                let needwriteback : bool;
                if let Some(p) = g.queue.peek() {
                    let now = Instant::now();
                    
                    if p.to_be_written_at <= now {
                        needwriteback = true;
                        timetowait = None; // actually should not be necessary
                    } else {
                        needwriteback = false;
                        timetowait = Some(p.to_be_written_at.duration_since(now));
                    }
                } else {
                    needwriteback = false;
                    timetowait = None;
                }
                
                if needwriteback {
                    let p = g.queue.pop().unwrap();
                    let data = g.cache.remove(&p.block_index).expect("inconsistency detected");
                    writeback = Some((SeekFrom::Start(self.blocksize * p.block_index), data));
                    break 'inner; // release lock and go to outer loop
                } 
            
                if let Some(ttw) = timetowait {
                    let (g2, _) = self.attention.wait_timeout(g, ttw).unwrap();
                    g = g2;
                } else {
                    g = self.attention.wait(g).unwrap();
                }
            }
        }
    }
    
    fn checkblock(&self, bi: BlockIndex) -> bool {
        let mut g = self.s.lock().unwrap();
        return g.cache.contains_key(&bi);
    }
    
    fn useblock<G>(&self, bi: BlockIndex, closure: G) where G: FnOnce(Option<&Vec<u8>>) {
        let mut g = self.s.lock().unwrap();
        closure(g.cache.get(&bi));
    }
    
    fn writeblock(&self, bi: BlockIndex, data: Vec<u8>, maxblocks: usize, mindelay: Duration, maxdelay: Duration) {
        let mut g = self.s.lock().unwrap();
        if g.cache.contains_key(&bi) {
            g.cache.insert(bi, data);
            return;
        }
        while g.cache.len() >= maxblocks {
            g = self.writeback_completed.wait(g).unwrap();
        }
        
        g.cache.insert(bi, data);
        
        let min_nanos = mindelay.as_secs() * 1000_000_000 + (mindelay.subsec_nanos() as u64);
        let max_nanos = maxdelay.as_secs() * 1000_000_000 + (maxdelay.subsec_nanos() as u64);
        let r = thread_rng().gen_range(min_nanos, max_nanos+1);
        let writeback_delay = Duration::new(r / 1000_000_000, (r % 1000_000_000) as u32);
        
        g.queue.push( DelayedWriteback {
            to_be_written_at: Instant::now() + writeback_delay,
            block_index : bi
            });
        self.attention.notify_all();
    }
    
    fn stop(&self) -> usize {
        *self.please_stop.lock().unwrap() = true;
        self.attention.notify_all();
        let mut g = self.s.lock().unwrap();
        g.cache.len()
        
    }
}


impl Ord for DelayedWriteback {
    fn cmp(&self, peer:&DelayedWriteback) -> ::std::cmp::Ordering {
        use std::cmp::Ordering::{Greater,Less,Equal};
        // flipped ordering for the time
        let o1 = peer.to_be_written_at.cmp(&self.to_be_written_at);
        if o1 != Equal { return o1; }
        
        // fall-back ordering by index
        return self.block_index.cmp(&peer.block_index);
    }
}
impl PartialOrd for DelayedWriteback {
    fn partial_cmp(&self, other: &DelayedWriteback) -> Option<::std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[test]
fn test_writeback_thread() {
    use std::io::Cursor;
    let mut v = Cursor::new(vec![0; 10]);
    
    let mut wt = Arc::new(WritebackThread::new(v, 2));
    
    let waiter = {
        let mut wt2 = wt.clone();
        ::std::thread::spawn(move || {
            wt2.run();
        })
    };
    
    wt.writeblock(3, vec![33,33], 10, Duration::from_secs(3), Duration::from_secs(4));
    wt.writeblock(1, vec![5,1], 10, Duration::from_secs(1), Duration::from_secs(1));
    wt.writeblock(4, vec![7,3], 10, Duration::from_secs(1), Duration::from_secs(1));
    wt.writeblock(2, vec![2,1], 10, Duration::from_secs(1), Duration::from_secs(1));
    wt.writeblock(0, vec![22,22], 10, Duration::from_secs(3), Duration::from_secs(4));
    
    ::std::thread::sleep(Duration::from_secs(2));
    
    assert_eq!(wt.stop(), 2);
    
    waiter.join().unwrap();
    v = Arc::try_unwrap(wt).or_else(|_|Err("thread expected to finish already")).unwrap().into_file();
    assert_eq!(v.get_ref(), &vec![0,0,5,1,2,1,0,0,7,3]);
}

impl<'a, F> Read for VirtualFile<'a, F> where F : LikeFile + 'a {
    fn read(&mut self, b: &mut [u8]) -> Result<usize,::std::io::Error> {
        let mut file = self.w.f.lock().unwrap();
        file.read(b)
    }
}

impl<'a, F> Write for VirtualFile<'a, F> where F : LikeFile + 'a {
    fn write(&mut self, b: &[u8]) -> Result<usize,::std::io::Error> {
        let mut file = self.w.f.lock().unwrap();
        file.write(b)
    }
    
    fn flush(&mut self) -> Result<(), ::std::io::Error> {
        Ok(())
    }
}

impl<'a, F> Seek for VirtualFile<'a, F> where F : LikeFile + 'a {
    fn seek(&mut self, s: ::std::io::SeekFrom) -> Result<u64,::std::io::Error> {
        let mut file = self.w.f.lock().unwrap();
        file.seek(s)
    }
}

#[test]
fn virtual_file_consistency() {
    use std::io::Cursor;
    let mut v = Cursor::new(vec![0; 10]);
    
    let mut wt = Arc::new(WritebackThread::new(v, 2));
    
    let waiter = {
        let mut wt2 = wt.clone();
        ::std::thread::spawn(move || {
            wt2.run();
        })
    };
    
    let mut buf1 = [0; 1];
    let mut buf2 = [0; 2];
    let mut buf3 = [0; 3];
    
    let mut vf = wt.get_virtual_file(10, Duration::from_secs(0), Duration::from_secs(2));
    assert_eq!(vf.write(&vec![4]  ).unwrap(), 1);
    assert_eq!(vf.write(&vec![5,8]).unwrap(), 2);
    assert_eq!(vf.write(&vec![1,3,9]).unwrap(), 3);
    assert_eq!(vf.read(&mut buf3).unwrap(), 3); 
        assert_eq!(&[0,0,0], &buf3);
    
    assert_eq!(vf.seek(SeekFrom::Start(0)).unwrap(), 0);
    assert_eq!(vf.read(&mut buf3).unwrap(), 3); 
        assert_eq!(&[4,5,8], &buf3);
    assert_eq!(vf.read(&mut buf2).unwrap(), 2); 
        assert_eq!(&[1,3], &buf2);
    assert_eq!(vf.read(&mut buf1).unwrap(), 1); 
        assert_eq!(&[9], &buf1);
    
    ::std::thread::sleep(Duration::from_secs(1));
    
    assert_eq!(vf.seek(SeekFrom::Start(0)).unwrap(), 0);
    assert_eq!(vf.read(&mut buf2).unwrap(), 2); 
        assert_eq!(&[4,5], &buf2);
    assert_eq!(vf.read(&mut buf1).unwrap(), 1); 
        assert_eq!(&[8], &buf1);
    assert_eq!(vf.read(&mut buf3).unwrap(), 3); 
        assert_eq!(&[1,3,9], &buf3);
        
    ::std::thread::sleep(Duration::from_secs(2));
    
    assert_eq!(vf.seek(SeekFrom::Start(0)).unwrap(), 0);
    assert_eq!(vf.read(&mut buf1).unwrap(), 1); 
        assert_eq!(&[4], &buf1);
    assert_eq!(vf.read(&mut buf2).unwrap(), 2); 
        assert_eq!(&[5,8], &buf2);
    assert_eq!(vf.read(&mut buf3).unwrap(), 3); 
        assert_eq!(&[1,3,9], &buf3);
    
    wt.stop();
}


////////////////////////////////////////////////////

const TTL: Timespec = Timespec { sec: 10, nsec: 0 };
const CREATE_TIME: Timespec = Timespec { sec: 1458180306, nsec: 0 }; //FIXME

const HELLO_TXT_CONTENT: &'static str = "Hello World!\n";

struct BunchOfTraitsAsFs<'a, F : LikeFile + 'a> {
    file: &'a mut F,
    fa: FileAttr,
}

impl<'a, F> BunchOfTraitsAsFs<'a, F> where F : LikeFile {
    fn new(f: &'a mut F, bs: u64) -> BunchOfTraitsAsFs<F> {
        let len = f.seek(SeekFrom::End(0)).expect("File not seekable");
        let blocks = ((len-1) / bs) + 1;
    
        BunchOfTraitsAsFs { 
            file: f,
            fa: FileAttr {
                ino: 1,
                size: len,
                blocks: blocks,
                atime: CREATE_TIME,
                mtime: CREATE_TIME,
                ctime: CREATE_TIME,
                crtime: CREATE_TIME,
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
            }
        }
    }
}
const ENOENT : i32 = 2;

impl<'a, F> Filesystem for BunchOfTraitsAsFs<'a, F>  where F : LikeFile {
    fn lookup (&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        reply.entry(&TTL, &self.fa, 0);
    }

    fn getattr (&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        reply.attr(&TTL, &self.fa);
    }

    fn read (&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, _size: u32, reply: ReplyData) {
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        
        let mut buf = vec![0; _size as usize];
        let ret = self.file.read(&mut buf).unwrap();
        buf.truncate(ret);
        
        reply.data(buf.as_slice());
    }
    
    fn write (&mut self, _req: &Request, _ino: u64, _fh: u64, _offset: u64, _data: &[u8], _flags: u32, reply: ReplyWrite) {
        self.file.seek(SeekFrom::Start(_offset)).unwrap();
        
        self.file.write_all(_data).unwrap();
        //self.file.flush().unwrap();
        
        reply.written(_data.len() as u32);
    }
}

fn main () {
    let argv = env::args_os();
    if argv.count() != 6 {
        println!("Usage: outoforderfs source_file mountpoint_file blocksize maxtime_ms maxdirtyblocks");
        println!(" outoforderfs 'mirrors' source file to mountpoint_file,");
        println!(" but writes to mountpoint_file get delivered");
        println!(" to source_file after a random delay, in random order.");
        println!(" The reason is to see what happens of some other FS in case of");
        println!(" surprise removal (or sudden shutdown) of the storage.");
        println!("Example session (approximate):");
        println!(" 1$ dd if=/dev/zero bs=4096 count=2560 of=sf.dat");
        println!(" 1$ touch mp.dat");
        println!(" 1$ outoforderfs sf.dat mp.dat 4096 10000 100");
        println!(" 1$ (switch to a new tab)");
        println!(" 2$   mkfs mp.dat");
        println!(" 2$   mount mp.dat -o loop mnt");
        println!(" 2$   start using mnt (filling with logs, creating sqlite, etc)");
        println!(" 1$ ^C");
        println!(" 1$ outoforderfs sf.dat mp.dat 4096 10000 100");
        println!(" 2$   umount mnt");
        println!(" 2$   mount mp.dat -o loop mnt");
        println!(" 2$   inspect mnt for breakage, incomplete/zeroed files, etc");
        
        ::std::process::exit(1);
    }
    let source_file_s     = env::args_os().nth(1).unwrap();
    let mountpoint_file_s = env::args_os().nth(2).unwrap();
    let blocksize_s       = env::args_os().nth(3).unwrap();
    let maxtime_s         = env::args_os().nth(4).unwrap();
    let maxblocks_s       = env::args_os().nth(5).unwrap();

    let mut source_file = ::std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .truncate(false)
                        .create(false)
                        .open(&source_file_s)
                        .expect("Can't open source file");
    // FIXME :
    let mut source_file_2 = ::std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .truncate(false)
                        .create(false)
                        .open(&source_file_s)
                        .expect("Can't open source file");
                        
    let blocksize : u64 = blocksize_s.to_str().unwrap().parse().unwrap();
    let maxtime   : u64 = maxtime_s.to_str().unwrap().parse().unwrap();
    let maxblocks : u64 = maxblocks_s.to_str().unwrap().parse().unwrap();
                        
    
    let mut wt = Arc::new(WritebackThread::new(source_file, blocksize));
    let wt2 = wt.clone();
    
    use chan_signal::Signal;
    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);
    
    let waiter = {
        ::std::thread::spawn(move || {
            wt2.run();
        })
    };
    
    let fs = BunchOfTraitsAsFs::new(&mut source_file_2, blocksize);
    let guard = unsafe { fuse::spawn_mount(fs, &mountpoint_file_s, &[]) };
    
    signal.recv().unwrap();
    
    let outstanding_blocks = wt.stop();
    println!("");
    println!("{} dirty blocks lost", outstanding_blocks);
    
    waiter.join().unwrap();
}
