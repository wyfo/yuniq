use std::io::{self, Write};

use clap::Parser;
use hashbrown::HashSet;
use memchr::memchr;
use memmap2::MmapOptions;
use twox_hash::XxHash3_128;

const DEFAULT_CAPACITY: usize = 1024 * 1024;
const READ_BUF_SIZE: usize = 64 * 1024;

#[derive(Parser)]
#[command(about = "Superfast line deduplicator")]
struct Args {
    /// Expected number of lines
    #[arg(short, long, default_value_t = DEFAULT_CAPACITY)]
    capacity: usize,
}

fn write_all(mut slice: &[u8]) -> io::Result<()> {
    while !slice.is_empty() {
        // SAFETY: slice is a valid readable buffer for slice.len() bytes.
        let n = unsafe { libc::write(libc::STDOUT_FILENO, slice.as_ptr().cast(), slice.len()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        slice = &slice[n as usize..];
    }
    Ok(())
}

fn read(buf: &mut [u8]) -> io::Result<usize> {
    // SAFETY: buf is a valid writable buffer for buf.len() bytes.
    let n = unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as usize)
}

struct Deduplicator {
    seen: HashSet<u128>,
}

impl Deduplicator {
    fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::with_capacity(capacity),
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn is_duplicate(&mut self, line: &[u8]) -> bool {
        let mut end = line.len();
        if end > 0 && line[end - 1] == b'\n' {
            end -= 1;
        }
        if end > 0 && line[end - 1] == b'\r' {
            end -= 1;
        }
        !self.seen.insert(XxHash3_128::oneshot(&line[..end]))
    }
}

fn process_mmap(data: &[u8], mut dedup: Deduplicator) -> io::Result<()> {
    let mut pos = 0;
    let mut write_start = 0;
    while pos < data.len() {
        let next = memchr(b'\n', &data[pos..]).map_or(data.len(), |i| pos + i + 1);
        if dedup.is_duplicate(&data[pos..next]) {
            write_all(&data[write_start..pos])?;
            write_start = next;
        }
        pos = next;
    }
    write_all(&data[write_start..])
}

fn process_stream(mut dedup: Deduplicator) -> io::Result<()> {
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let mut buf = vec![0u8; READ_BUF_SIZE];
    let mut leftover = 0usize;
    while let n = read(&mut buf[leftover..])?
        && n > 0
    {
        let filled = leftover + n;
        let mut pos = 0;
        while let Some(i) = memchr(b'\n', &buf[pos..filled]) {
            let line = &buf[pos..pos + i + 1];
            if !dedup.is_duplicate(line) {
                stdout.write_all(line)?;
            }
            pos += i + 1;
        }
        leftover = filled - pos;
        buf.copy_within(pos..filled, 0);
        if leftover == buf.len() {
            buf.resize(buf.len() * 2, 0);
        }
    }
    if leftover > 0 && !dedup.is_duplicate(&buf[..leftover]) {
        stdout.write_all(&buf[..leftover])?;
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let dedup = Deduplicator::new(args.capacity);
    // SAFETY: we do not mutate the mapped file while the mapping is live.
    match unsafe { MmapOptions::new().map(&io::stdin().lock()) } {
        Ok(mmap) => process_mmap(&mmap, dedup),
        Err(_) => process_stream(dedup),
    }
}
