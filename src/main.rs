#![feature(time2)]

#![allow(unused)]
#![warn(unused_must_use)]

extern crate fuse;
extern crate time;
extern crate nix;

use std::env;
use std::path::Path;
use time::Timespec;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};
use std::time::Instant;
use std::time::Duration;

use nix::sys::signal;

use std::sync::{Condvar, Mutex, MutexGuard};

type BlockIndex = u64;

#[derive(Eq,PartialEq)]
struct DelayedWriteback {
    to_be_written_at : Instant,
    block_index : BlockIndex,
}

type BlockCache = std::collections::BTreeMap<BlockIndex, Vec<u64>>;
type Queue = std::collections::BinaryHeap<DelayedWriteback>;

struct State {
    cache: BlockCache,
    queue: Queue,
    attention_of_writeback_thread: Condvar,
}

impl Default for State { fn default() -> Self { State {
        cache: Default::default(),
        queue: Default::default(),
        attention_of_writeback_thread: Condvar::new(), // https://github.com/rust-lang/rust/issues/31865
    } } }


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
    
    ::std::thread::sleep(Duration::from_secs(1));
}
