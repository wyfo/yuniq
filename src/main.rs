use std::{
    hash::{Hash, Hasher},
    io::{self, ErrorKind, Write},
    num::NonZero,
};

use bumpalo::{Bump, collections::Vec as BVec};
use clap::Parser;
use getrandom::fill as getrandom_fill;
use hashbrown::{HashMap, HashSet, hash_map, hash_set};
use memchr::{memchr, memchr2};
use memmap2::Mmap;
use twox_hash::xxhash3_128;

const DEFAULT_CAPACITY: usize = 1024 * 1024;
const READ_BUF_SIZE: usize = 64 * 1024;

fn random_seed() -> u64 {
    let mut buf = [0u8; 8];
    getrandom_fill(&mut buf).expect("failed to generate random seed");
    u64::from_ne_bytes(buf)
}

fn random_secret() -> Box<[u8]> {
    let mut secret = vec![0u8; xxhash3_128::DEFAULT_SECRET_LENGTH];
    getrandom_fill(&mut secret).expect("failed to generate random secret");
    secret.into_boxed_slice()
}

#[derive(Parser)]
#[command(about = "Hyperfast line deduplicator")]
struct Args {
    /// Use 128-bit hashing only (lower memory consumption, negligible collision risk)
    #[arg(long)]
    fast: bool,
    /// Reduce memory footprint by only retaining unique lines instead of buffering
    /// all of stdin. Slightly slower and prevents memory-mapping stdin.
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

// Read from stdin into the spare capacity of `buf`, set_len, and return the byte count.
// bumpalo::collections::Vec has no spare_capacity_mut, so we compute the spare slice via
// pointer arithmetic.
fn read(buf: &mut BVec<u8>) -> io::Result<Option<NonZero<usize>>> {
    // SAFETY: `buf.as_mut_ptr().add(buf.len())` points to the first byte of spare capacity;
    // `spare_len` bytes are available. Uninitialized memory is acceptable as the read target.
    let spare_ptr = unsafe { buf.as_mut_ptr().add(buf.len()) }.cast::<libc::c_void>();
    let spare_len = buf.capacity() - buf.len();
    // SAFETY: `STDIN_FILENO` is a valid open fd; `spare_ptr` points to `spare_len` writable bytes.
    match unsafe { libc::read(libc::STDIN_FILENO, spare_ptr, spare_len) } {
        n if n > 0 => {
            let n = n as usize;
            // SAFETY: `libc::read` wrote exactly `n` bytes into the spare capacity.
            unsafe { buf.set_len(buf.len() + n) };
            Ok(NonZero::new(n))
        }
        0 => Ok(None),
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

// A byte-slice key with a custom `Hash` that writes the raw bytes without prefixing
// the length. Skipping the length prefix is correct here because we hash single
// values (not sequences), so the prefix adds no disambiguation benefit.
#[derive(Clone, Copy, PartialEq, Eq)]
struct LineKey<'a>(&'a [u8]);

impl Hash for LineKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0);
    }
}

// Owns a bump allocator and tracks the lean flag so every caller that needs both
// can receive a single `&'arena Arena` parameter.
struct Arena {
    bump: Bump,
    lean: bool,
}

impl Arena {
    fn new_buf(&self) -> BVec<'_, u8> {
        BVec::with_capacity_in(READ_BUF_SIZE, &self.bump)
    }

    /// Extends `data`'s lifetime to `'arena`, either by copying bytes into the
    /// arena (lean mode) or by transmuting the reference (non-lean mode).
    ///
    /// # Safety
    ///
    /// In non-lean mode the caller must guarantee that `data`'s backing memory
    /// lives for `'arena` and is never mutated while the returned reference
    /// exists. Concretely, `data` must be a subslice of either:
    ///   • a bumpalo-Vec buffer allocated from this arena (process_stream), or
    ///   • a mmap that outlives `'arena` (process_mmap).
    unsafe fn alloc_line<'s>(&'s self, data: &[u8]) -> &'s [u8] {
        if self.lean {
            self.bump.alloc_slice_copy(data)
        } else {
            // SAFETY: upheld by caller per contract above.
            unsafe { std::mem::transmute::<&[u8], &'s [u8]>(data) }
        }
    }

    /// Returns a buffer seeded with `buf[processed..]` ready for the next read.
    /// In non-lean mode the old buffer is forgotten rather than dropped (bumpalo's
    /// dealloc can roll back the bump pointer if called on the last allocation,
    /// corrupting live LineKey references); in lean mode the processed prefix is
    /// drained in place. When `processed == buf.len()` the returned buffer is empty.
    fn reset_buf<'b>(&'b self, mut buf: BVec<'b, u8>, processed: usize) -> BVec<'b, u8> {
        if self.lean {
            if processed == buf.len() {
                buf.clear();
            } else {
                buf.drain(..processed);
            }
            buf
        } else {
            let tail = (processed < buf.len()).then(|| buf[processed..].to_vec());
            std::mem::forget(buf);
            let mut new_buf = self.new_buf();
            if let Some(tail) = tail {
                new_buf.extend_from_slice(&tail);
            }
            new_buf
        }
    }
}

enum DeduplicatorSeen<'arena> {
    // Only the 128-bit XXHash3 digest is stored; a collision is treated as a
    // duplicate (false positive). The seed and secret are randomly generated
    // at startup.
    Fast {
        set: HashSet<u128>,
        seed: u64,
        secret: Box<[u8]>,
    },
    // Each LineKey<'arena> borrows from either an archived arena-backed buffer
    // (non-lean) or a fresh arena allocation (lean). Both live for 'arena.
    Default {
        set: HashSet<LineKey<'arena>>,
    },
    // `map` stores key → index into `order`.
    // `order` stores (line without trailing newline, running count) in first-seen
    // order. Lines are `&'arena [u8]` since they only need to be emitted, not
    // looked up by hash.
    Count {
        map: HashMap<LineKey<'arena>, usize>,
        order: Vec<(&'arena [u8], u64)>,
    },
}

impl<'arena> DeduplicatorSeen<'arena> {
    fn new(capacity: usize, fast: bool, count: bool) -> Self {
        if count {
            Self::Count {
                map: HashMap::with_capacity(capacity),
                order: Vec::new(),
            }
        } else if fast {
            Self::Fast {
                set: HashSet::with_capacity(capacity),
                seed: random_seed(),
                secret: random_secret(),
            }
        } else {
            Self::Default {
                set: HashSet::with_capacity(capacity),
            }
        }
    }

    /// Returns `true` if `key` was not previously seen (i.e. `line` should be
    /// emitted), or `false` if it is a duplicate.
    fn insert(&mut self, key: LineKey<'arena>, line: &'arena [u8]) -> bool {
        match self {
            DeduplicatorSeen::Fast { set, seed, secret } => {
                // Write raw bytes directly, same reasoning as `LineKey::hash`.
                // SAFETY: secret is always DEFAULT_SECRET_LENGTH (192) bytes,
                // which exceeds SECRET_MINIMUM_LENGTH (136).
                let hash = xxhash3_128::Hasher::oneshot_with_seed_and_secret(*seed, secret, key.0)
                    .expect("secret length is always valid");
                set.insert(hash)
            }
            DeduplicatorSeen::Default { set } => match set.entry(key) {
                hash_set::Entry::Occupied(_) => false,
                hash_set::Entry::Vacant(entry) => {
                    entry.insert();
                    true
                }
            },
            DeduplicatorSeen::Count { map, order } => {
                match map.entry(key) {
                    hash_map::Entry::Occupied(entry) => {
                        // SAFETY: the index was set to `order.len()` at insertion
                        // time and `order` only grows, so it is always in bounds.
                        unsafe { order.get_unchecked_mut(*entry.get()) }.1 += 1;
                        false
                    }
                    hash_map::Entry::Vacant(entry) => {
                        let idx = order.len();
                        entry.insert(idx);
                        order.push((line, 1));
                        true
                    }
                }
            }
        }
    }
}

struct Deduplicator<'arena> {
    seen: DeduplicatorSeen<'arena>,
    has_filter: bool,
    skip_chars: Option<NonZero<usize>>,
    check_chars: Option<NonZero<usize>>,
    skip_fields: Option<NonZero<usize>>,
}

impl<'arena> Deduplicator<'arena> {
    fn new(args: &Args) -> Self {
        let skip_chars = args.skip_chars.and_then(NonZero::new);
        let check_chars = args.check_chars.map(|c| NonZero::new(c).unwrap());
        let skip_fields = args.skip_fields.and_then(NonZero::new);
        Self {
            seen: DeduplicatorSeen::new(args.size_hint, args.fast, args.count),
            has_filter: skip_chars.is_some() || check_chars.is_some() || skip_fields.is_some(),
            skip_chars,
            check_chars,
            skip_fields,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn is_duplicate(&mut self, line: &'arena [u8]) -> bool {
        let key: &'arena [u8] = if self.has_filter {
            self.filter_key(line)
        } else {
            line
        };
        !self.seen.insert(LineKey(key), line)
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
            writer.write_all(line)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }
}

fn process_chunk<'arena>(
    data: &[u8],
    dedup: &mut Deduplicator<'arena>,
    arena: &'arena Arena,
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
        // or `data.len()` (virtual newline).
        let line_data: &[u8] = unsafe { data.get_unchecked(pos..nl) };

        // SAFETY: `data` is a bumpalo-Vec buffer (process_stream) or a mmap
        // (process_mmap) that outlives 'arena; neither is mutated while refs exist.
        let arena_line: &'arena [u8] = unsafe { arena.alloc_line(line_data) };

        if dedup.is_duplicate(arena_line) {
            // SAFETY: `write_start` only moves forward to a previous `pos` value,
            // so it never exceeds `pos`.
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

// `mmap` shares `'arena` lifetime with `dedup`, so the borrow checker enforces
// that the mmap cannot be dropped while `dedup` holds LineKey references into it.
fn process_mmap<'arena>(
    mmap: &'arena [u8],
    dedup: &mut Deduplicator<'arena>,
    arena: &'arena Arena,
    writer: &mut dyn Write,
) -> io::Result<()> {
    process_chunk(mmap, dedup, arena, true, writer)?;
    Ok(())
}

fn process_stream<'arena>(
    dedup: &mut Deduplicator<'arena>,
    arena: &'arena Arena,
    writer: &mut dyn Write,
) -> io::Result<()> {
    // We manage the read buffer manually rather than using BufReader so we can
    // control its capacity (64 KB was benchmarked as optimal) and, in non-lean
    // mode, hand ownership of the arena-backed buffer to the arena once it's full
    // (the bytes are kept alive for 'arena without any extra bookkeeping).
    let mut buf = BVec::with_capacity_in(READ_BUF_SIZE, &arena.bump);
    while read(&mut buf)?.is_some() {
        let processed = process_chunk(&buf, dedup, arena, false, writer)?;
        if processed == 0 {
            // The entire buffer is one unterminated line; double capacity.
            buf.reserve(buf.len());
        } else {
            buf = arena.reset_buf(buf, processed);
        }
    }
    // Process whatever remains after EOF as the final (possibly unterminated) chunk.
    process_chunk(&buf, dedup, arena, true, writer)?;
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
    // Declaration order: arena → mmap → dedup.
    // Drop order (reverse): dedup → mmap → arena.
    // This guarantees that LineKey references into mmap memory (non-lean, mmap path)
    // remain valid for the entire lifetime of `dedup`.
    let arena = Arena {
        bump: Bump::new(),
        lean: args.lean,
    };
    // SAFETY: we do not mutate the mapped file while the mapping is live.
    let mmap = (!args.lean)
        .then(|| unsafe { Mmap::map(&io::stdin().lock()).ok() })
        .flatten();
    let mut dedup = Deduplicator::new(&args);
    let writer = if args.count {
        &mut io::sink() as &mut dyn Write
    } else {
        &mut io::BufWriter::new(RawStdout)
    };
    match mmap {
        Some(ref mmap) => process_mmap(mmap, &mut dedup, &arena, writer)?,
        None => process_stream(&mut dedup, &arena, writer)?,
    }
    if args.count {
        dedup.write_counts(!args.no_sort, args.reverse)?;
    } else {
        writer.flush()?;
    }
    Ok(())
}

fn main() -> io::Result<()> {
    match deduplicate(Args::parse()) {
        Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(()),
        res => res,
    }
}
