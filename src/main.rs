use std::{io, num::NonZero};

use clap::Parser;
use hashbrown::{HashSet, HashTable};
use memchr::memchr;
use memmap2::MmapOptions;
use twox_hash::{XxHash3_64, XxHash3_128};

#[cfg(test)]
mod tests;

const DEFAULT_CAPACITY: usize = 1024 * 1024;
const DEFAULT_BUF_SIZE: usize = 64 * 1024;

#[derive(Parser)]
#[command(about = "Hyperfast line deduplicator")]
struct Args {
    /// Expected number of lines
    #[arg(short, long, default_value_t = DEFAULT_CAPACITY)]
    capacity: usize,
    /// Read buffer size in bytes
    #[arg(short, long, default_value_t = DEFAULT_BUF_SIZE)]
    buf_size: usize,
    /// Use 64-bit hashing (faster, negligible collision risk)
    #[arg(long)]
    fast: bool,
    /// Only compare the first N characters of each line
    #[arg(short = 'w', long)]
    check_chars: Option<usize>,
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

fn read(buf: &mut [u8]) -> io::Result<Option<NonZero<usize>>> {
    // SAFETY: buf is a valid writable buffer for buf.len() bytes.
    let n = unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(NonZero::new(n as usize))
}

enum DeduplicatorSeen {
    Fast(HashTable<u64>),
    Default(HashSet<u128>),
}

struct Deduplicator {
    seen: DeduplicatorSeen,
    check_chars: Option<usize>,
}

impl Deduplicator {
    fn new(capacity: usize, fast: bool, check_chars: Option<usize>) -> Self {
        let seen = if fast {
            DeduplicatorSeen::Fast(HashTable::with_capacity(capacity))
        } else {
            DeduplicatorSeen::Default(HashSet::with_capacity(capacity))
        };
        Self { seen, check_chars }
    }

    #[allow(clippy::wrong_self_convention)]
    fn is_duplicate(&mut self, line: &[u8]) -> bool {
        let key = match line {
            [l @ .., b'\r', b'\n'] => l,
            [l @ .., b'\n'] => l,
            l => l,
        };
        let key = (self.check_chars).map_or(key, |limit| &key[..limit.min(key.len())]);
        match &mut self.seen {
            DeduplicatorSeen::Fast(seen) => {
                let hash = XxHash3_64::oneshot(key);
                if seen.find(hash, |&k| k == hash).is_some() {
                    return true;
                }
                seen.insert_unique(hash, hash, |&k| k);
                false
            }
            DeduplicatorSeen::Default(seen) => !seen.insert(XxHash3_128::oneshot(key)),
        }
    }
}

fn process_chunk(
    data: &[u8],
    dedup: &mut Deduplicator,
    is_final: bool,
    mut write: impl FnMut(&[u8]) -> io::Result<()>,
) -> io::Result<usize> {
    let mut pos = 0;
    let mut write_start = 0;
    while pos < data.len() {
        let next = match memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i + 1,
            None if is_final => data.len(),
            None => break,
        };
        if dedup.is_duplicate(&data[pos..next]) {
            write(&data[write_start..pos])?;
            write_start = next;
        }
        pos = next;
    }
    write(&data[write_start..pos])?;
    Ok(data.len() - pos)
}

fn process_mmap(data: &[u8], mut dedup: Deduplicator) -> io::Result<()> {
    process_chunk(data, &mut dedup, true, write_all)?;
    Ok(())
}

fn process_stream(mut dedup: Deduplicator, buf_size: usize) -> io::Result<()> {
    let mut buf = vec![0u8; buf_size];
    let mut leftover = 0usize;
    while let Some(n) = read(&mut buf[leftover..])? {
        let filled = leftover + n.get();
        leftover = process_chunk(&buf[..filled], &mut dedup, false, write_all)?;
        if leftover > 0 {
            buf.copy_within(filled - leftover..filled, 0);
            if leftover == buf.len() {
                buf.resize(buf.len() * 2, 0);
            }
        }
    }
    process_chunk(&buf[..leftover], &mut dedup, true, write_all)?;
    Ok(())
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let dedup = Deduplicator::new(args.capacity, args.fast, args.check_chars);
    // SAFETY: we do not mutate the mapped file while the mapping is live.
    match unsafe { MmapOptions::new().map(&io::stdin().lock()) } {
        Ok(mmap) => process_mmap(&mmap, dedup),
        Err(_) => process_stream(dedup, args.buf_size),
    }
}
