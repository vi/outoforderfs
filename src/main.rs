extern crate fuse;
extern crate time;
extern crate nix;

use std::env;
use std::path::Path;
use time::Timespec;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};

use nix::sys::signal;

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
        return;
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
    unsafe { signal::sigaction(signal::SIGINT, &sig_action); }
    
    ::std::thread::sleep_ms(1000);
}
