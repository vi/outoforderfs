#![allow(unused)]
#![warn(unused_must_use)]

extern crate fuse;
extern crate time;
extern crate nix;
extern crate rand;

use std::env;
use std::path::Path;
use time::Timespec;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};
use std::time::Instant;
use std::time::Duration;

use nix::sys::signal;

use std::sync::{Condvar, Mutex, MutexGuard};
use std::sync::Arc;
use std::io::{Write,Read,Seek,SeekFrom};
use std::cell::RefCell;
use rand::{Rng,thread_rng};
use std::cell::Cell;

type BlockIndex = u64;

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

struct WritebackThread {
    s : Mutex<CacheState>,
    attention: Condvar,
    writeback_completed: Condvar,
    please_stop: Mutex<bool>,
}

impl Default for WritebackThread { fn default() -> Self { WritebackThread {
        s: Mutex::new(Default::default()),
        attention: Condvar::new(), // https://github.com/rust-lang/rust/issues/31865
        writeback_completed: Condvar::new(),
        please_stop: Mutex::new(false),
    } } }
    
impl WritebackThread {
    fn new() -> WritebackThread { Default::default() }

    fn run<W>(&self, mut file: &mut W, blocksize: u64) where W : Write + Seek  {
        
        let mut writeback : Option<(SeekFrom, Vec<u8>)>;
        
        writeback = None;
        'outer: loop {
            if let Some((seekpos, data)) = writeback {
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
                    writeback = Some((SeekFrom::Start(blocksize * p.block_index), data));
                    break; // release lock and go to outer loop
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
    
    fn useblock<F>(&self, bi: BlockIndex, closure: F) where F : FnOnce(Option<&Vec<u8>>) {
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
    
    fn stop(&self) {
        *self.please_stop.lock().unwrap() = true;
        self.attention.notify_all();
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
    
    let mut wt = Arc::new(WritebackThread::new());
    
    let waiter = {
        let mut wt2 = wt.clone();
        ::std::thread::spawn(move || {
            wt2.run(&mut v, 2);
            v
        })
    };
    
    wt.writeblock(3, vec![33,33], 10, Duration::from_secs(3), Duration::from_secs(4));
    wt.writeblock(1, vec![5,1], 10, Duration::from_secs(1), Duration::from_secs(1));
    wt.writeblock(4, vec![7,3], 10, Duration::from_secs(1), Duration::from_secs(1));
    wt.writeblock(2, vec![2,1], 10, Duration::from_secs(1), Duration::from_secs(1));
    wt.writeblock(0, vec![22,22], 10, Duration::from_secs(3), Duration::from_secs(4));
    
    ::std::thread::sleep(Duration::from_secs(2));
    
    wt.stop();
    
    let r = waiter.join().unwrap();
    assert_eq!(r.get_ref(), &vec![0,0,5,1,2,1,0,0,7,3]);
}



extern fn handle_sigint(_:i32) {
    let outstanding_blocks = 0;
    println!("Throwing away {} dirty blocks.", outstanding_blocks);
    ::std::process::exit(0);
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
    let source_file     = env::args_os().nth(1).unwrap();
    let mountpoint_file = env::args_os().nth(2).unwrap();
    let blocksize       = env::args_os().nth(3).unwrap();
    let maxtime         = env::args_os().nth(4).unwrap();
    let maxblocks       = env::args_os().nth(5).unwrap();
    //fuse::mount(OutoforderFs, &mountpoint, &[]);
    
    
    let sig_action = signal::SigAction::new(handle_sigint,
                                          signal::SockFlag::empty(),
                                          signal::SigSet::empty());
    unsafe { signal::sigaction(signal::SIGINT, &sig_action).unwrap(); }
    
    //let sf = 
    
    ::std::thread::sleep(Duration::from_secs(1));
}
