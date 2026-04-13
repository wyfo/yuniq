use std::{
    hash::{BuildHasher, Hash, Hasher},
    io::{self, ErrorKind, Write},
    mem::MaybeUninit,
    num::NonZero,
    ops::Deref,
    ptr::NonNull,
};

use bumpalo::Bump;
use clap::Parser;
use hashbrown::{HashMap, HashSet, HashTable, hash_map, hash_set, hash_table};
use memchr::{memchr, memchr2};
use memmap2::Mmap;

const DEFAULT_CAPACITY: usize = 1024 * 1024;
const READ_BUF_SIZE: usize = 64 * 1024;

#[derive(Parser)]
#[command(about = "Hyperfast line deduplicator")]
struct Args {
    /// Use 64-bit hashing only (2-3x faster, negligible collision risk, but still unsafe)
    #[arg(long)]
    fast: bool,
    /// Copy unique lines into a bump allocator instead of retaining read buffers.
    /// Reduces memory usage when input has many duplicates, at the cost of one
    /// allocation per unique line.
    #[arg(long, conflicts_with = "fast")]
    lean: bool,
    /// Prefix each line with its global occurrence count, sorted by count
    #[arg(short = 'c', long, conflicts_with = "fast")]
    count: bool,
    /// Reverse sort order (requires --count, incompatible with --no-sort)
    #[arg(
        short = 'r',
        long,
        alias = "rev",
        requires = "count",
        conflicts_with = "no_sort"
    )]
    reverse: bool,
    /// Preserve insertion order instead of sorting by count (requires --count)
    #[arg(short = 'S', long, requires = "count")]
    no_sort: bool,
    /// Expected number of unique lines (used to pre-size internal structures)
    #[arg(long, default_value_t = DEFAULT_CAPACITY)]
    size_hint: usize,
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
    // SAFETY: `STDIN_FILENO` is a valid open fd; `buf` is a valid writable region
    // of `buf.len()` bytes; uninitialized memory is acceptable as the read target.
    match unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr().cast(), buf.len()) } {
        n if n >= 0 => Ok(NonZero::new(n as usize)),
        _ => Err(io::Error::last_os_error()),
    }
}

struct RawStdout;
impl Write for RawStdout {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // SAFETY: `STDOUT_FILENO` is a valid open fd; `buf` is a valid readable
        // region of `buf.len()` initialized bytes.
        match unsafe { libc::write(libc::STDOUT_FILENO, buf.as_ptr().cast(), buf.len()) } {
            n if n >= 0 => Ok(n as usize),
            _ => Err(io::Error::last_os_error()),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// A byte-slice pointer whose referent is kept alive by the `buffers` vec of the
// enclosing `DeduplicatorSeen`. Every `UnsafeSlice` must not outlive the buffer
// it was created from.
#[derive(Clone, Copy)]
struct UnsafeSlice(NonNull<[u8]>);
impl UnsafeSlice {
    // SAFETY: `slice` must outlive the returned `UnsafeSlice`. Callers ensure
    // this by archiving the owning buffer into `buffers` before it can be dropped.
    unsafe fn new(slice: &[u8]) -> Self {
        Self(slice.into())
    }
}
impl Deref for UnsafeSlice {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        // SAFETY: constructed from a valid `&[u8]`; the owning buffer in
        // `DeduplicatorSeen::buffers` is kept alive for the lifetime of self.
        unsafe { self.0.as_ref() }
    }
}
impl PartialEq for UnsafeSlice {
    fn eq(&self, other: &UnsafeSlice) -> bool {
        **self == **other
    }
}
impl Eq for UnsafeSlice {}
impl Hash for UnsafeSlice {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Call `write` directly rather than delegating to `<[u8] as Hash>`,
        // which would prepend the length. Skipping it is correct here because
        // we hash single values, not sequences, so the length prefix adds no
        // disambiguation benefit and just wastes a few cycles.
        state.write(self);
    }
}

// Backing store that keeps the bytes pointed to by `UnsafeSlice` entries alive.
//
// Both variants satisfy the same contract but with opposite memory trade-offs:
//
//  • `ReadBuffer` archives raw read buffers as-is. It is zero-copy but retains
//    every input byte, including duplicates, so memory scales with input size.
//
//  • `Arena` copies only unique line bytes into a bump allocator; duplicates are
//    discarded immediately, so memory scales with the number of unique lines.
//
// The `ReadBuffer` path cannot be made safe (slices borrow from a buffer that is
// later moved into the vec), so `UnsafeSlice` is used for both variants rather
// than introducing two separate safe/unsafe code paths.
enum Storage {
    ReadBuffer(Vec<Vec<u8>>),
    Arena(Bump),
}

impl Storage {
    fn new(lean: bool) -> Self {
        if lean {
            Self::Arena(Bump::new())
        } else {
            Self::ReadBuffer(Vec::new())
        }
    }

    // # Safety
    //
    // The returned `UnsafeSlice` must not outlive `self`:
    // - `Arena`: the slice points into arena-owned memory; valid for the arena's
    //   lifetime, with no further obligation on the caller.
    // - `ReadBuffer`: the slice borrows directly from `key` without copying. The
    //   caller must ensure the buffer containing `key` is archived into the vec
    //   (via `buffers.push`) before it can be dropped. In `process_stream` this
    //   invariant is upheld on every iteration; in `process_mmap` `key` is a
    //   subslice of the mmap which outlives the deduplicator.
    unsafe fn store_key(&self, key: &[u8]) -> UnsafeSlice {
        match self {
            // SAFETY: `alloc_slice_copy` copies `key` into the arena and returns
            // a reference tied to the arena's lifetime, which equals `self`'s.
            Self::Arena(arena) => unsafe { UnsafeSlice::new(arena.alloc_slice_copy(key)) },
            // SAFETY: caller guarantees the buffer containing `key` outlives the
            // returned slice per the contract above.
            Self::ReadBuffer(_) => unsafe { UnsafeSlice::new(key) },
        }
    }
}

enum DeduplicatorSeen {
    // Only the hash is stored; a collision is treated as a duplicate (false
    // positive). `foldhash::quality` is used over `foldhash::fast` to keep the
    // collision probability low — the throughput difference is not measurable.
    Fast {
        table: HashTable<u64>,
        hash_state: foldhash::quality::RandomState,
    },
    // Each UnsafeSlice points into one of the owned `buffers` vecs below.
    // The set must not outlive those vecs.
    Default {
        set: HashSet<UnsafeSlice>,
        storage: Storage,
    },
    // `map` stores the key (line without trailing newline) → index into `order`.
    // `order` stores the line (without trailing newline) and its running count,
    // in first-seen order, so we can emit insertion-order output without sorting.
    // Both UnsafeSlices point into the owned `buffers` vecs below.
    Count {
        map: HashMap<UnsafeSlice, usize>,
        order: Vec<(UnsafeSlice, u64)>,
        storage: Storage,
    },
}

impl DeduplicatorSeen {
    fn new(capacity: usize, fast: bool, lean: bool, count: bool) -> Self {
        if count {
            Self::Count {
                map: HashMap::with_capacity(capacity),
                order: Vec::new(),
                storage: Storage::new(lean),
            }
        } else if fast {
            Self::Fast {
                table: HashTable::with_capacity(capacity),
                hash_state: Default::default(),
            }
        } else {
            Self::Default {
                set: HashSet::with_capacity(capacity),
                storage: Storage::new(lean),
            }
        }
    }

    /// Returns `true` if `key` was not previously seen (i.e. `line` should be
    /// emitted), or `false` if it is a duplicate.
    ///
    /// # Safety
    ///
    /// For `Default` and `Count` with `ReadBuffer` storage, `key` and `line`
    /// must be subslices of a buffer that will be archived into the `ReadBuffer`
    /// before it is dropped (see `Storage::store_key`). For `Arena` storage and
    /// the `Fast` variant there are no additional obligations.
    unsafe fn insert(&mut self, key: &[u8], line: &[u8]) -> bool {
        match self {
            DeduplicatorSeen::Fast { table, hash_state } => {
                let mut hasher = hash_state.build_hasher();
                // Write raw bytes directly, same reasoning as `UnsafeSlice::hash`.
                hasher.write(key);
                let hash = hasher.finish();
                match table.entry(hash, |h| *h == hash, |h| *h) {
                    hash_table::Entry::Occupied(_) => false,
                    hash_table::Entry::Vacant(entry) => {
                        entry.insert(hash);
                        true
                    }
                }
            }
            DeduplicatorSeen::Default { set, storage } => {
                // SAFETY: upheld by caller per `insert`'s contract.
                match set.entry(unsafe { storage.store_key(key) }) {
                    hash_set::Entry::Occupied(_) => false,
                    hash_set::Entry::Vacant(entry) => {
                        entry.insert();
                        true
                    }
                }
            }
            DeduplicatorSeen::Count {
                map,
                order,
                storage,
            } => {
                // SAFETY: upheld by caller per `insert`'s contract.
                match map.entry(unsafe { storage.store_key(key) }) {
                    hash_map::Entry::Occupied(entry) => {
                        // SAFETY: the index was set to `order.len()` at insertion
                        // time and `order` only grows, so it is always in bounds.
                        unsafe { order.get_unchecked_mut(*entry.get()) }.1 += 1;
                        false
                    }
                    hash_map::Entry::Vacant(entry) => {
                        let idx = order.len();
                        entry.insert(idx);
                        // SAFETY: same contract as `key` above.
                        order.push((unsafe { storage.store_key(line) }, 1));
                        true
                    }
                }
            }
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
            seen: DeduplicatorSeen::new(args.size_hint, args.fast, args.lean, args.count),
            has_filter: skip_chars.is_some() || check_chars.is_some() || skip_fields.is_some(),
            skip_chars,
            check_chars,
            skip_fields,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn is_duplicate(&mut self, line: &[u8]) -> bool {
        let mut key = line;
        if self.has_filter {
            key = self.filter_key(key);
        }
        // SAFETY: `key` and `line` are subslices of the current read chunk. For
        // `ReadBuffer` storage, `process_stream` archives the owning buffer
        // before releasing it. For `Arena` storage and `Fast`, bytes are either
        // copied into the arena or hashed and discarded immediately.
        !unsafe { self.seen.insert(key, line) }
    }

    #[cold]
    fn filter_key<'a>(&self, mut key: &'a [u8]) -> &'a [u8] {
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
        key
    }

    fn buffers(&mut self) -> Option<&mut Vec<Vec<u8>>> {
        if let DeduplicatorSeen::Default { storage, .. } | DeduplicatorSeen::Count { storage, .. } =
            &mut self.seen
            && let Storage::ReadBuffer(buffers) = storage
        {
            return Some(buffers);
        }
        None
    }

    fn write_counts(&mut self, sort: bool, reverse: bool) -> io::Result<()> {
        let DeduplicatorSeen::Count { order, .. } = &mut self.seen else {
            unreachable!()
        };
        if sort {
            if reverse {
                order.sort_by_key(|&(_, count)| std::cmp::Reverse(count));
            } else {
                order.sort_by_key(|&(_, count)| count);
            }
        }
        let mut writer = io::BufWriter::new(RawStdout);
        let mut buf = itoa::Buffer::new();
        for &(line, count) in order.iter() {
            writer.write_all(buf.format(count).as_bytes())?;
            writer.write_all(b"\t")?;
            writer.write_all(&line)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
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
    loop {
        // SAFETY: both `nl == data.len()` branches at the end either returned or broke,
        // so `nl < data.len()` here, therefore `pos = nl + 1 <= data.len()`.
        let nl = match memchr(b'\n', unsafe { data.get_unchecked(pos..) }) {
            Some(off) => pos + off,
            // No newline found: if this is the final chunk and bytes remain,
            // use `data.len()` as a virtual newline position. The `nl == data.len()`
            // branches below handle both cases — a duplicate returns early, a
            // unique line breaks out and is written without a trailing newline,
            // preserving the original absence of one.
            None if is_final && pos != data.len() => data.len(),
            None => break,
        };
        // SAFETY: `nl` is either `pos + newline_offset` (bounded by `data.len() - 1`)
        // or `data.len()` (virtual newline);
        if dedup.is_duplicate(unsafe { data.get_unchecked(pos..nl) }) {
            // SAFETY: `write_start` only moves forward to a previous `pos` value, so it never exceeds `pos`
            writer.write_all(unsafe { data.get_unchecked(write_start..pos) })?;
            if nl == data.len() {
                debug_assert!(is_final);
                return Ok(nl);
            }
            write_start = nl + 1;
        } else if nl == data.len() {
            debug_assert!(is_final);
            pos = nl;
            break;
        }
        pos = nl + 1;
    }
    // Flush the trailing run of unique lines not yet written.
    // SAFETY: the loop only advances pos forward and breaks before data.len();
    // write_start <= pos because it only moves to next, which was pos's prior value.
    writer.write_all(unsafe { data.get_unchecked(write_start..pos) })?;
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
        // SAFETY: `read()` writes exactly `n` bytes into the spare capacity;
        // `buf.len() + n` does not exceed `buf.capacity()` by construction.
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
    // SAFETY: we do not mutate the mapped file while the mapping is live.
    let mmap = unsafe { Mmap::map(&io::stdin().lock()).ok() };
    // `mmap` must outlive the `dedup` which can contains references to it.
    let mut dedup = Deduplicator::new(&args);
    let writer = if args.count {
        &mut io::sink() as &mut dyn Write
    } else {
        &mut io::BufWriter::new(RawStdout)
    };
    match mmap {
        Some(ref mmap) => process_mmap(mmap, &mut dedup, writer)?,
        None => process_stream(&mut dedup, writer)?,
    }
    if args.count {
        dedup.write_counts(!args.no_sort, args.reverse)?;
    } else {
        writer.flush()?;
    }
    // Prevent mmap to be dropped before writing the counts
    drop(mmap);
    Ok(())
}

fn main() -> io::Result<()> {
    match deduplicate(Args::parse()) {
        Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(()),
        res => res,
    }
}
