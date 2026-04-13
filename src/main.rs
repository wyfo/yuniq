use std::{
    hint::assert_unchecked,
    io::{self, ErrorKind, Write},
    mem::MaybeUninit,
    num::NonZero,
    ptr::NonNull,
};

use clap::Parser;
use hashbrown::{HashSet, HashTable, hash_table::Entry};
use memchr::{memchr, memchr2};
use memmap2::Mmap;
use twox_hash::{XxHash3_64, XxHash3_128};

const DEFAULT_CAPACITY: usize = 1024 * 1024;
const READ_BUF_SIZE: usize = 64 * 1024;

#[derive(Parser)]
#[command(about = "Hyperfast line deduplicator")]
struct Args {
    /// Expected number of unique lines (used to pre-size internal structures)
    #[arg(long, default_value_t = DEFAULT_CAPACITY)]
    size_hint: usize,
    /// Use 64-bit hashing (faster, negligible collision risk)
    #[arg(long)]
    fast: bool,
    /// Store exact line bytes for collision-free deduplication (slower)
    #[arg(long, alias = "slow", conflicts_with = "fast")]
    safe: bool,
    /// Prefix each line with its global occurrence count
    #[arg(short = 'c', long, conflicts_with_all = ["fast", "safe"])]
    count: bool,
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
    // The hash itself is stored as the value; a hash collision is treated as a
    // duplicate (false positive). The collision risk grows with the number of
    // distinct lines but remains low given XxHash3_64's 64-bit output space.
    Fast(HashTable<u64>),
    // 128-bit hash makes collisions negligible without storing the raw bytes.
    Default(HashSet<u128>),
    Safe {
        // Each NonNull<[u8]> points into one of the owned `buffers` vecs below.
        // The table must not outlive those vecs.
        table: HashTable<(u64, NonNull<[u8]>)>,
        buffers: Vec<Vec<u8>>,
    },
    Count {
        // (hash, key_ptr, order_idx): key_ptr for equality, order_idx for O(1) count increment.
        // Both pointers refer into the owned `buffers` vecs below.
        table: HashTable<(u64, NonNull<[u8]>, usize)>,
        // First-seen order: (full_line_ptr, total_count). Written out at the end.
        order: Vec<(NonNull<[u8]>, u64)>,
        buffers: Vec<Vec<u8>>,
    },
}

impl DeduplicatorSeen {
    fn new(capacity: usize, fast: bool, safe: bool, count: bool) -> Self {
        if count {
            Self::Count {
                table: HashTable::with_capacity(capacity),
                order: Vec::new(),
                buffers: Vec::new(),
            }
        } else if safe {
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
    has_filter: bool,
    skip_chars: Option<NonZero<usize>>,
    check_chars: Option<NonZero<usize>>,
    skip_fields: Option<NonZero<usize>>,
}

impl Deduplicator {
    fn new(args: &Args) -> Self {
        let skip_chars = args.skip_chars.and_then(NonZero::new);
        let check_chars = args.check_chars.map(|c| NonZero::new(c).unwrap());
        let skip_fields = args.skip_fields.and_then(NonZero::new);
        Self {
            seen: DeduplicatorSeen::new(args.size_hint, args.fast, args.safe, args.count),
            has_filter: skip_chars.is_some() || check_chars.is_some() || skip_fields.is_some(),
            skip_chars,
            check_chars,
            skip_fields,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn is_duplicate(&mut self, line: &[u8]) -> bool {
        let mut key = match line {
            // Do I want to support this?
            // [k @ .., b'\r', b'\n'] => k,
            [k @ .., b'\n'] => k,
            k => k,
        };
        if self.has_filter {
            if let Some(n) = self.skip_fields {
                for _ in 0..n.get() {
                    let is_blank = |&b| b != b' ' && b != b'\t';
                    let i = key.iter().position(is_blank).unwrap_or(key.len());
                    key = &key[i..];
                    let i = memchr2(b' ', b'\t', key).unwrap_or(key.len());
                    // SAFETY: memchr2 returns an index within `key`, but the
                    // compiler cannot prove it.
                    key = unsafe { key.get_unchecked(i..) };
                }
            }
            if let Some(skip) = self.skip_chars {
                key = &key[skip.get().min(key.len())..];
            }
            if let Some(check) = self.check_chars {
                key = &key[..check.get().min(key.len())];
            }
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
            DeduplicatorSeen::Count { table, order, .. } => {
                let hash = XxHash3_64::oneshot(key);
                match table.entry(hash, |(_, k, _)| unsafe { k.as_ref() } == key, |(h, ..)| *h) {
                    Entry::Occupied(entry) => {
                        let idx = entry.get().2;
                        // SAFETY: idx was set to order.len() at insertion time, so it is in bounds.
                        unsafe { order.get_unchecked_mut(idx) }.1 += 1;
                        true
                    }
                    Entry::Vacant(entry) => {
                        let idx = order.len();
                        entry.insert((hash, key.into(), idx));
                        order.push((line.into(), 1));
                        false
                    }
                }
            }
        }
    }

    fn buffers(&mut self) -> Option<&mut Vec<Vec<u8>>> {
        match &mut self.seen {
            DeduplicatorSeen::Safe { buffers, .. } => Some(buffers),
            DeduplicatorSeen::Count { buffers, .. } => Some(buffers),
            _ => None,
        }
    }

    fn write_counts(&self, writer: &mut impl Write) -> io::Result<()> {
        let DeduplicatorSeen::Count { order, .. } = &self.seen else {
            return Ok(());
        };
        for &(ptr, count) in order {
            write!(writer, "{:>7} ", count)?;
            // SAFETY: ptr points into self.buffers which are alive for the duration of self.
            writer.write_all(unsafe { ptr.as_ref() })?;
        }
        Ok(())
    }
}

fn process_chunk(
    data: &[u8],
    dedup: &mut Deduplicator,
    is_final: bool,
    writer: &mut dyn Write,
) -> io::Result<usize> {
    let mut pos = 0;
    // Tracks the start of the current run of unique lines. When a duplicate is
    // found, we flush [write_start..pos] (everything before the duplicate) in
    // one write, then skip the duplicate by advancing write_start past it.
    let mut write_start = 0;
    while pos < data.len() {
        let next = match memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i + 1,
            None if is_final => data.len(),
            None => break,
        };
        // Help the compiler to remove bound checks by making invariants explicit.
        // SAFETY: next is either pos + newline_offset + 1 (bounded by data.len())
        // or data.len(); pos < next by construction; write_start only moves forward.
        unsafe { assert_unchecked(next <= data.len() && pos < next && write_start <= pos) };
        if dedup.is_duplicate(&data[pos..next]) {
            writer.write_all(&data[write_start..pos])?;
            write_start = next;
        }
        pos = next;
    }
    // Help the compiler to remove bound checks by making invariants explicit.
    // SAFETY: the loop only advances pos forward and breaks before data.len();
    // write_start <= pos because it only moves to next, which was pos's prior value.
    unsafe { assert_unchecked(pos <= data.len() && write_start <= pos) };
    // Flush the trailing run of unique lines not yet written.
    writer.write_all(&data[write_start..pos])?;
    Ok(pos)
}

fn process_mmap(data: &[u8], dedup: &mut Deduplicator, writer: &mut dyn Write) -> io::Result<()> {
    process_chunk(data, dedup, true, writer)?;
    Ok(())
}

fn process_stream(dedup: &mut Deduplicator, writer: &mut dyn Write) -> io::Result<()> {
    // We manage the read buffer manually rather than using BufReader so we can
    // control its capacity (64 KB was benchmarked as optimal) and, in safe mode,
    // hand ownership of the buffer to the deduplicator once it's full.
    let mut buf = Vec::with_capacity(READ_BUF_SIZE);
    while let Some(n) = read(buf.spare_capacity_mut())? {
        unsafe { buf.set_len(buf.len() + n.get()) };
        let processed = process_chunk(&buf, dedup, false, writer)?;
        // `processed < buf.len()` means the last line had no newline yet; keep
        // the unprocessed tail and continue reading. Three sub-cases:
        if processed < buf.len() {
            if processed == 0 {
                // The entire buffer is one unterminated line; double capacity to
                // make room without losing data.
                buf.reserve(buf.len());
            } else if let Some(buffers) = dedup.buffers() {
                // Safe mode: the table holds raw pointers into `buf`, so we
                // cannot drain it. Move it into `buffers` and start a fresh one.
                let mut new_buf = Vec::with_capacity(READ_BUF_SIZE);
                new_buf.extend_from_slice(&buf[processed..]);
                buffers.push(buf);
                buf = new_buf;
            } else {
                // Normal mode: drain the processed prefix in place.
                buf.drain(..processed);
            }
        } else if let Some(buffers) = dedup.buffers() {
            // Safe mode, buffer fully consumed: archive it and start fresh.
            buffers.push(buf);
            buf = Vec::with_capacity(READ_BUF_SIZE);
        } else {
            buf.clear();
        }
    }
    // Process whatever remains after EOF as the final (possibly unterminated) chunk.
    process_chunk(&buf, dedup, true, writer)?;
    if let Some(buffers) = dedup.buffers() {
        // Store the buffer for consistency.
        buffers.push(buf);
    }
    Ok(())
}

fn deduplicate(args: Args) -> io::Result<()> {
    // Dumb case: no chars are checked so every line is a duplicate; just print the first line.
    // Skip this shortcut in --count mode since we need a full pass to accumulate totals.
    if args.check_chars == Some(0) && !args.count {
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        return io::stdout().write_all(line.as_bytes());
    }
    let mut writer = io::BufWriter::new(RawStdout);
    // SAFETY: we do not mutate the mapped file while the mapping is live.
    let mmap = unsafe { Mmap::map(&io::stdin().lock()).ok() };
    // `mmap` must outlive the `dedup` which can contains references to it.
    let mut dedup = Deduplicator::new(&args);
    let write = if args.count {
        &mut io::sink() as &mut dyn Write
    } else {
        &mut writer
    };
    match mmap {
        Some(ref mmap) => process_mmap(mmap, &mut dedup, write)?,
        None => process_stream(&mut dedup, write)?,
    }
    if args.count {
        dedup.write_counts(&mut writer)?;
    }
    // Prevent mmap to be dropped before writing the counts
    drop(mmap);
    writer.flush()
}

fn main() -> io::Result<()> {
    match deduplicate(Args::parse()) {
        Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(()),
        res => res,
    }
}
