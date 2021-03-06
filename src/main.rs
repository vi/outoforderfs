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

trait MyReadEx : Read {
    // Based on https://doc.rust-lang.org/src/std/io/mod.rs.html#620
    fn read_exact2(&mut self, mut buf: &mut [u8]) -> ::std::io::Result<usize> {
        let mut successfully_read = 0;
        while !buf.is_empty() {
            match self.read(buf) {
                Ok(0) => break,
                Ok(n) => { successfully_read+=n; let tmp = buf; buf = &mut tmp[n..]; }
                Err(ref e) if e.kind() == ::std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(successfully_read)
    }
}
impl<T:Read> MyReadEx for T{}

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
    blocksize: usize,
    
}

struct VirtualFile<'a, F: LikeFile + 'a> {
    cursor: u64,
    maxdirtyblocks: usize,
    mindelay: Duration,
    maxdelay: Duration,
    w: &'a WritebackThread<F>,
}

impl<F> WritebackThread<F> where F : LikeFile {
    fn new(file: F, blocksize: usize) -> WritebackThread<F> { 
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
    
    fn get_virtual_file<'a>(&'a self, maxdirtyblocks: usize, mindelay: Duration, maxdelay: Duration) -> VirtualFile<'a, F> {
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
                    writeback = Some((SeekFrom::Start((self.blocksize as u64) * p.block_index), data));
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
    
    fn useblock<G,R>(&self, bi: BlockIndex, closure: G) -> R where G: FnOnce(Option<&Vec<u8>>) -> R {
        let mut g = self.s.lock().unwrap();
        closure(g.cache.get(&bi))
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
    fn read(&mut self, mut b: &mut [u8]) -> Result<usize,::std::io::Error> {
        let cursor = self.cursor;
        let bs = self.w.blocksize;
        let current_block = self.cursor / (bs as u64);
        let position_within_block = (cursor - current_block * (bs as u64)) as usize;
        
        // Don't let read request span more than one block
        if b.len() > bs - position_within_block {
            let mut tmp = b;
            b = &mut tmp[..(bs - position_within_block)] 
        };
        
        self.w.useblock(current_block, |bl| {
            if let Some(x) = bl {
            
                let blen = b.len();
                b.clone_from_slice(&x[position_within_block..(position_within_block+blen)]);
                self.seek(SeekFrom::Start(cursor + (blen as u64))).unwrap();
                // cursor gets shifted automatically
                Ok(blen)
            } else {
                // read from file
                
                let mut file = self.w.f.lock().unwrap();
                file.seek(SeekFrom::Start(self.cursor)).unwrap();
                file.read(b).map(|x|{self.cursor+=x as u64; x})
            }
        })
    }
}

impl<'a, F> Write for VirtualFile<'a, F> where F : LikeFile + 'a {
    fn write(&mut self, mut b: &[u8]) -> Result<usize,::std::io::Error> {
        
        let cursor = self.cursor;
        let bs = self.w.blocksize;
        let current_block = cursor / (bs as u64);
        let position_within_block = (cursor - current_block * (bs as u64)) as usize;
        
        let mut block_to_be_written = vec![0; bs];
        
        if b.len() + position_within_block > bs  { let mut tmp = b; b = &tmp[..(bs-position_within_block)]; }
        
        if b.len() < bs || position_within_block > 0 {
            self.seek(SeekFrom::Start(current_block * (bs as u64))).unwrap();
            self.read_exact(&mut block_to_be_written[..]).unwrap();
        }
        block_to_be_written[position_within_block..(position_within_block+b.len())].clone_from_slice(b); // copy_from_slice?
        
        self.w.writeblock(current_block, block_to_be_written, self.maxdirtyblocks, self.mindelay, self.maxdelay);
        
        self.seek(SeekFrom::Start(cursor + (b.len() as u64))).unwrap();
        // cursor gets shifted automatically
        
        Ok(b.len())
    }
    
    fn flush(&mut self) -> Result<(), ::std::io::Error> {
        // Explicitly ignored
        Ok(())
    }
}

impl<'a, F> Seek for VirtualFile<'a, F> where F : LikeFile + 'a {
    fn seek(&mut self, s: ::std::io::SeekFrom) -> Result<u64,::std::io::Error> {
        let mut file = self.w.f.lock().unwrap();
        // TODO use cursor as base, not current file's value
        file.seek(s).map(|x| {self.cursor = x; x})
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
    vf.write_all(&vec![4]  ).unwrap();
    vf.write_all(&vec![5,8]).unwrap();
    vf.write_all(&vec![1,3,9]).unwrap();
    assert_eq!(vf.read_exact2(&mut buf3).unwrap(), 3); 
        assert_eq!(&[0,0,0], &buf3);
    
    assert_eq!(vf.seek(SeekFrom::Start(0)).unwrap(), 0);
    assert_eq!(vf.read_exact2(&mut buf3).unwrap(), 3); 
        assert_eq!(&[4,5,8], &buf3);
    assert_eq!(vf.read_exact2(&mut buf2).unwrap(), 2); 
        assert_eq!(&[1,3], &buf2);
    assert_eq!(vf.read_exact2(&mut buf1).unwrap(), 1); 
        assert_eq!(&[9], &buf1);
    
    ::std::thread::sleep(Duration::from_secs(1));
    
    assert_eq!(vf.seek(SeekFrom::Start(0)).unwrap(), 0);
    assert_eq!(vf.read_exact2(&mut buf2).unwrap(), 2); 
        assert_eq!(&[4,5], &buf2);
    assert_eq!(vf.read_exact2(&mut buf1).unwrap(), 1); 
        assert_eq!(&[8], &buf1);
    assert_eq!(vf.read_exact2(&mut buf3).unwrap(), 3); 
        assert_eq!(&[1,3,9], &buf3);
        
    ::std::thread::sleep(Duration::from_secs(2));
    
    assert_eq!(vf.seek(SeekFrom::Start(0)).unwrap(), 0);
    assert_eq!(vf.read_exact2(&mut buf1).unwrap(), 1); 
        assert_eq!(&[4], &buf1);
    assert_eq!(vf.read_exact2(&mut buf2).unwrap(), 2); 
        assert_eq!(&[5,8], &buf2);
    assert_eq!(vf.read_exact2(&mut buf3).unwrap(), 3); 
        assert_eq!(&[1,3,9], &buf3);
    
    wt.stop();
}


////////////////////////////////////////////////////

const TTL: Timespec = Timespec { sec: 10, nsec: 0 };
const CREATE_TIME: Timespec = Timespec { sec: 1458180306, nsec: 0 }; //FIXME

const HELLO_TXT_CONTENT: &'static str = "Hello World!\n";

struct BunchOfTraitsAsFs<F : LikeFile> {
    file: Mutex<F>,
    fa: FileAttr,
}

impl<F> BunchOfTraitsAsFs<F> where F : LikeFile {
    fn new(mut f: F, bs: usize) -> BunchOfTraitsAsFs<F> {
        let len = f.seek(SeekFrom::End(0)).expect("File not seekable");
        let blocks = ((len-1) / (bs as u64)) + 1;
    
        BunchOfTraitsAsFs { 
            file: Mutex::new(f),
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

impl<F> Filesystem for BunchOfTraitsAsFs<F>  where F : LikeFile {
    fn lookup (&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        reply.entry(&TTL, &self.fa, 0);
    }

    fn getattr (&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        reply.attr(&TTL, &self.fa);
    }

    fn read (&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, _size: u32, reply: ReplyData) {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        
        let mut buf = vec![0; _size as usize];
        let ret = file.read_exact2(&mut buf).unwrap();
        buf.truncate(ret);
        
        reply.data(buf.as_slice());
    }
    
    fn write (&mut self, _req: &Request, _ino: u64, _fh: u64, _offset: u64, _data: &[u8], _flags: u32, reply: ReplyWrite) {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(_offset)).unwrap();
        
        file.write_all(_data).unwrap();
        //file.flush().unwrap();
        
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
                        
    let blocksize : usize = blocksize_s.to_str().unwrap().parse().unwrap();
    let maxtime   : u64   =   maxtime_s.to_str().unwrap().parse().unwrap();
    let maxblocks : usize = maxblocks_s.to_str().unwrap().parse().unwrap();
                        
    
    let mut wt = Arc::new(WritebackThread::new(source_file, blocksize));
    let wt2 = wt.clone();
    
    use chan_signal::Signal;
    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);
    
    let waiter = {
        ::std::thread::spawn(move || {
            wt2.run();
        })
    };
    
    let mut vf = wt.get_virtual_file(maxblocks, Duration::from_millis(0), Duration::from_millis(maxtime));
    
    let fs = BunchOfTraitsAsFs::new(&mut vf, blocksize);
    let guard = unsafe { fuse::spawn_mount(fs, &mountpoint_file_s, &[]) };
    
    signal.recv().unwrap();
    
    let outstanding_blocks = wt.stop();
    println!("");
    println!("{} dirty blocks lost", outstanding_blocks);
    
    waiter.join().unwrap();
}
