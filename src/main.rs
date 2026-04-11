use std::{
    env::args,
    io::{self, ErrorKind, Write},
};

use getargs::{Opt, Options};
use hashbrown::HashSet;
use libc;
use memchr::memchr;
use memmap2::MmapOptions;
use twox_hash::XxHash3_128;

const DEFAULT_CAPACITY: usize = 1_000_000;

fn print_help() {
    eprintln!(
        "Usage: xuniq [OPTIONS]
Superfast line deduplicator

  -c, --capacity  expected number of lines (default: {})
  -h, --help      display this help and exit",
        DEFAULT_CAPACITY
    );
}

#[inline]
fn trim_newline(buf: &[u8]) -> &[u8] {
    let mut end = buf.len();
    if end > 0 && buf[end - 1] == b'\n' {
        end -= 1;
    }
    if end > 0 && buf[end - 1] == b'\r' {
        end -= 1;
    }
    &buf[..end]
}

fn main() {
    let args: Vec<String> = args().skip(1).collect();
    let mut capacity = DEFAULT_CAPACITY;

    let mut opts = Options::new(args.iter().map(String::as_str));

    while let Some(opt) = opts.next_opt().expect("argument parsing error") {
        match opt {
            Opt::Short('h') | Opt::Long("help") => {
                print_help();
                return;
            }
            Opt::Short('c') | Opt::Long("capacity") => {
                let val = opts.value().expect("capacity requires a value");
                capacity = val.parse().expect("capacity must be a number");
            }
            _ => {
                eprintln!("Unknown option: {:?}", opt);
                print_help();
                return;
            }
        }
    }

    let stdin = io::stdin();
    let reader = stdin.lock();
    let mut stdout = io::BufWriter::new(io::stdout().lock());

    let mut seen = HashSet::with_capacity(capacity);

    let mut process_line = |raw: &[u8]| -> bool {
        let hash = XxHash3_128::oneshot(trim_newline(raw));
        if seen.insert(hash)
            && let Err(e) = stdout.write_all(raw)
        {
            if e.kind() != ErrorKind::BrokenPipe {
                eprintln!("xuniq: {e}");
            }
            return false;
        }
        true
    };

    match unsafe { MmapOptions::new().map(&reader) } {
        Ok(mmap) => {
            let data: &[u8] = &mmap;
            let mut pos = 0;
            while pos < data.len() {
                let next = memchr(b'\n', &data[pos..])
                    .map(|i| pos + i + 1)
                    .unwrap_or(data.len());
                if !process_line(&data[pos..next]) {
                    break;
                }
                pos = next;
            }
        }
        Err(_) => {
            const BUF_SIZE: usize = 64 * 1024;
            let mut buf = vec![0u8; BUF_SIZE];
            let mut leftover = 0usize;

            loop {
                let n = unsafe {
                    libc::read(
                        libc::STDIN_FILENO,
                        buf[leftover..].as_mut_ptr() as *mut libc::c_void,
                        buf.len() - leftover,
                    )
                };
                if n < 0 {
                    eprintln!("xuniq: read error");
                    break;
                } else if n == 0 {
                    if leftover > 0 {
                        process_line(&buf[..leftover]);
                    }
                    break;
                }
                let filled = leftover + n as usize;
                let mut pos = 0;
                while let Some(i) = memchr(b'\n', &buf[pos..filled]) {
                    if !process_line(&buf[pos..pos + i + 1]) {
                        return;
                    }
                    pos += i + 1;
                }
                leftover = filled - pos;
                buf.copy_within(pos..filled, 0);
                if leftover == buf.len() {
                    buf.resize(buf.len() * 2, 0);
                }
            }
        }
    }
}
