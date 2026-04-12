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

struct RawStdout;

impl Write for RawStdout {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = unsafe { libc::write(libc::STDOUT_FILENO, buf.as_ptr().cast(), buf.len()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(n as usize)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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
    skip_chars: Option<NonZero<usize>>,
    check_chars: Option<NonZero<usize>>,
}

impl Deduplicator {
    #[allow(clippy::wrong_self_convention)]
    fn is_duplicate(&mut self, line: &[u8]) -> bool {
        let mut key = match line {
            [l @ .., b'\r', b'\n'] => l,
            [l @ .., b'\n'] => l,
            l => l,
        };
        if let Some(skip) = self.skip_chars {
            key = &key[skip.get().min(key.len())..];
        }
        if let Some(check) = self.check_chars {
            key = &key[..check.get().min(key.len())];
        }
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
    writer: &mut io::BufWriter<RawStdout>,
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
            writer.write_all(&data[write_start..pos])?;
            write_start = next;
        }
        pos = next;
    }
    writer.write_all(&data[write_start..pos])?;
    Ok(data.len() - pos)
}

fn process_mmap(
    data: &[u8],
    mut dedup: Deduplicator,
    writer: &mut io::BufWriter<RawStdout>,
) -> io::Result<()> {
    process_chunk(data, &mut dedup, true, writer)?;
    Ok(())
}

fn process_stream(
    mut dedup: Deduplicator,
    writer: &mut io::BufWriter<RawStdout>,
) -> io::Result<()> {
    let mut buf = vec![0u8; READ_BUF_SIZE];
    let mut leftover = 0usize;
    while let Some(n) = read(&mut buf[leftover..])? {
        let filled = leftover + n.get();
        leftover = process_chunk(&buf[..filled], &mut dedup, false, writer)?;
        if leftover > 0 {
            buf.copy_within(filled - leftover..filled, 0);
            if leftover == buf.len() {
                buf.resize(buf.len() * 2, 0);
            }
        }
    }
    process_chunk(&buf[..leftover], &mut dedup, true, writer)?;
    Ok(())
}

fn deduplicate(args: Args) -> io::Result<()> {
    // Dumb case, no chars are checked, every line is a duplicate, so just print the first line.
    if args.check_chars == Some(0) {
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        return io::stdout().write_all(line.as_bytes());
    }
    let dedup = Deduplicator {
        seen: DeduplicatorSeen::new(args.capacity, args.fast),
        skip_chars: args.skip_chars.and_then(NonZero::new),
        check_chars: args.check_chars.map(|c| NonZero::new(c).unwrap()),
    };
    let mut writer = io::BufWriter::new(RawStdout);
    // SAFETY: we do not mutate the mapped file while the mapping is live.
    match unsafe { MmapOptions::new().map(&io::stdin().lock()) } {
        Ok(mmap) => process_mmap(&mmap, dedup, &mut writer)?,
        Err(_) => process_stream(dedup, &mut writer)?,
    }
    writer.flush()
}

fn main() -> io::Result<()> {
    match deduplicate(Args::parse()) {
        Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(()),
        res => res,
    }
}
