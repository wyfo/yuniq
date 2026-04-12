use std::{
    io::{self, ErrorKind, Write},
    mem::MaybeUninit,
    num::NonZero,
    ptr::NonNull,
};

use clap::Parser;
use hashbrown::{HashSet, HashTable, hash_table::Entry};
use memchr::{memchr, memchr2};
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
    /// Store exact line bytes for collision-free deduplication (slower)
    #[arg(long, alias = "slow", conflicts_with = "fast")]
    safe: bool,
    /// Only compare the first N characters of each line
    #[arg(short = 'w', long)]
    check_chars: Option<usize>,
    /// Skip the first N characters of each line before comparing
    #[arg(short = 's', long)]
    skip_chars: Option<usize>,
    /// Skip the first N whitespace-delimited fields of each line before comparing
    #[arg(short = 'f', long)]
    skip_fields: Option<usize>,
}

fn read(buf: &mut [MaybeUninit<u8>]) -> io::Result<Option<NonZero<usize>>> {
    match unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr().cast(), buf.len()) } {
        n if n >= 0 => Ok(NonZero::new(n as usize)),
        _ => Err(io::Error::last_os_error()),
    }
}

struct RawStdout;

impl Write for RawStdout {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match unsafe { libc::write(libc::STDOUT_FILENO, buf.as_ptr().cast(), buf.len()) } {
            n if n >= 0 => Ok(n as usize),
            _ => Err(io::Error::last_os_error()),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum DeduplicatorSeen {
    Fast(HashTable<u64>),
    Default(HashSet<u128>),
    Safe {
        table: HashTable<(u64, NonNull<[u8]>)>,
        buffers: Vec<Vec<u8>>,
    },
}

impl DeduplicatorSeen {
    fn new(capacity: usize, fast: bool, safe: bool) -> Self {
        if safe {
            Self::Safe {
                table: HashTable::with_capacity(capacity),
                buffers: Vec::new(),
            }
        } else if fast {
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
    skip_fields: Option<NonZero<usize>>,
}

impl Deduplicator {
    #[allow(clippy::wrong_self_convention)]
    fn is_duplicate(&mut self, line: &[u8]) -> bool {
        let mut key = match line {
            [l @ .., b'\r', b'\n'] => l,
            [l @ .., b'\n'] => l,
            l => l,
        };
        if let Some(n) = self.skip_fields {
            for _ in 0..n.get() {
                // skip leading blanks — simple loop, typically 0–2 bytes at field boundary
                let i = key
                    .iter()
                    .position(|&b| b != b' ' && b != b'\t')
                    .unwrap_or(key.len());
                key = &key[i..];
                // skip non-blank field — memchr2 uses SIMD for arbitrarily long fields
                let i = memchr2(b' ', b'\t', key).unwrap_or(key.len());
                key = &key[i..];
            }
        }
        if let Some(skip) = self.skip_chars {
            key = &key[skip.get().min(key.len())..];
        }
        if let Some(check) = self.check_chars {
            key = &key[..check.get().min(key.len())];
        }
        match &mut self.seen {
            DeduplicatorSeen::Fast(table) => {
                let hash = XxHash3_64::oneshot(key);
                match table.entry(hash, |h| *h == hash, |h| *h) {
                    Entry::Occupied(_) => true,
                    Entry::Vacant(entry) => {
                        entry.insert(hash);
                        false
                    }
                }
            }
            DeduplicatorSeen::Default(set) => !set.insert(XxHash3_128::oneshot(key)),
            DeduplicatorSeen::Safe { table, .. } => {
                let hash = XxHash3_64::oneshot(key);
                match table.entry(hash, |(_, k)| unsafe { k.as_ref() } == key, |(h, _)| *h) {
                    Entry::Occupied(_) => true,
                    Entry::Vacant(entry) => {
                        entry.insert((hash, key.into()));
                        false
                    }
                }
            }
        }
    }

    fn buffers(&mut self) -> Option<&mut Vec<Vec<u8>>> {
        match &mut self.seen {
            DeduplicatorSeen::Safe { buffers, .. } => Some(buffers),
            _ => None,
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
    let mut buf = Vec::with_capacity(READ_BUF_SIZE);
    while let Some(n) = read(buf.spare_capacity_mut())? {
        unsafe { buf.set_len(buf.len() + n.get()) };
        let leftover = process_chunk(&buf, &mut dedup, false, writer)?;
        if leftover > 0 {
            if leftover == buf.len() {
                buf.reserve(buf.len());
            } else if let Some(buffers) = dedup.buffers() {
                let mut new_buf = Vec::with_capacity(READ_BUF_SIZE);
                new_buf.extend_from_slice(&buf[buf.len() - leftover..]);
                buffers.push(buf);
                buf = new_buf;
            } else {
                buf.drain(..buf.len() - leftover);
            }
        } else if let Some(buffers) = dedup.buffers() {
            buffers.push(buf);
            buf = Vec::with_capacity(READ_BUF_SIZE);
        } else {
            buf.clear();
        }
    }
    process_chunk(&buf, &mut dedup, true, writer)?;
    if let Some(buffers) = dedup.buffers() {
        // Store the buffer for consistency.
        buffers.push(buf);
    }
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
        seen: DeduplicatorSeen::new(args.capacity, args.fast, args.safe),
        skip_chars: args.skip_chars.and_then(NonZero::new),
        check_chars: args.check_chars.map(|c| NonZero::new(c).unwrap()),
        skip_fields: args.skip_fields.and_then(NonZero::new),
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
