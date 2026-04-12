use std::{
    io::{self, ErrorKind, Write},
    num::NonZero,
};

use clap::Parser;
use hashbrown::{HashSet, HashTable};
use memchr::memchr;
use memmap2::MmapOptions;
use twox_hash::{XxHash3_64, XxHash3_128};

const DEFAULT_CAPACITY: usize = 1024 * 1024;
const READ_BUF_SIZE: usize = 64 * 1024;

#[derive(Parser)]
#[command(about = "Hyperfast line deduplicator")]
struct Args {
    /// Expected number of lines
    #[arg(short, long, default_value_t = DEFAULT_CAPACITY)]
    capacity: usize,
    /// Use 64-bit hashing (faster, negligible collision risk)
    #[arg(long)]
    fast: bool,
    /// Only compare the first N characters of each line
    #[arg(short = 'w', long)]
    check_chars: Option<usize>,
    /// Skip the first N characters of each line before comparing
    #[arg(short = 's', long)]
    skip_chars: Option<usize>,
}

fn read(buf: &mut [u8]) -> io::Result<Option<NonZero<usize>>> {
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

impl DeduplicatorSeen {
    fn new(capacity: usize, fast: bool) -> Self {
        if fast {
            Self::Fast(HashTable::with_capacity(capacity))
        } else {
            Self::Default(HashSet::with_capacity(capacity))
        }
    }
}

struct Deduplicator {
    seen: DeduplicatorSeen,
    skip_chars: Option<usize>,
    check_chars: Option<usize>,
}

impl Deduplicator {
    #[allow(clippy::wrong_self_convention)]
    fn is_duplicate(&mut self, line: &[u8]) -> bool {
        let key = match line {
            [l @ .., b'\r', b'\n'] => l,
            [l @ .., b'\n'] => l,
            l => l,
        };
        let key = self.skip_chars.map_or(key, |n| &key[n.min(key.len())..]);
        let key = self.check_chars.map_or(key, |n| &key[..n.min(key.len())]);
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

// Returns Ok(Some(leftover)) on success, Ok(None) on broken pipe.
fn process_chunk(
    data: &[u8],
    dedup: &mut Deduplicator,
    is_final: bool,
    mut write: impl FnMut(&[u8]) -> io::Result<()>,
) -> io::Result<Option<usize>> {
    let mut pos = 0;
    let mut write_start = 0;
    while pos < data.len() {
        let next = match memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i + 1,
            None if is_final => data.len(),
            None => break,
        };
        if dedup.is_duplicate(&data[pos..next]) {
            match write(&data[write_start..pos]) {
                Err(e) if e.kind() == ErrorKind::BrokenPipe => return Ok(None),
                other => other?,
            }
            write_start = next;
        }
        pos = next;
    }
    match write(&data[write_start..pos]) {
        Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(None),
        other => other.map(|_| Some(data.len() - pos)),
    }
}

fn process_mmap(
    data: &[u8],
    mut dedup: Deduplicator,
    writer: &mut io::BufWriter<io::StdoutLock<'_>>,
) -> io::Result<()> {
    process_chunk(data, &mut dedup, true, |s| writer.write_all(s))?;
    Ok(())
}

fn process_stream(
    mut dedup: Deduplicator,
    writer: &mut io::BufWriter<io::StdoutLock<'_>>,
) -> io::Result<()> {
    let mut buf = vec![0u8; READ_BUF_SIZE];
    let mut leftover = 0usize;
    while let Some(n) = read(&mut buf[leftover..])? {
        let filled = leftover + n.get();
        match process_chunk(&buf[..filled], &mut dedup, false, |s| writer.write_all(s))? {
            Some(l) => leftover = l,
            None => return Ok(()),
        };
        if leftover > 0 {
            buf.copy_within(filled - leftover..filled, 0);
            if leftover == buf.len() {
                buf.resize(buf.len() * 2, 0);
            }
        }
    }
    process_chunk(&buf[..leftover], &mut dedup, true, |s| writer.write_all(s))?;
    Ok(())
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let dedup = Deduplicator {
        seen: DeduplicatorSeen::new(args.capacity, args.fast),
        skip_chars: args.skip_chars,
        check_chars: args.check_chars,
    };
    let mut writer = io::BufWriter::new(io::stdout().lock());
    // SAFETY: we do not mutate the mapped file while the mapping is live.
    match unsafe { MmapOptions::new().map(&io::stdin().lock()) } {
        Ok(mmap) => process_mmap(&mmap, dedup, &mut writer)?,
        Err(_) => process_stream(dedup, &mut writer)?,
    }
    match writer.flush() {
        Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(()),
        other => other,
    }
}
