#!/usr/bin/env python3
"""Generate benchmark data files for yuniq testing.

Produces 9 files (3 dup rates × 3 line-length ranges), each with 1M lines,
written to bench_data/.

File naming: dup{D}_len{MIN}-{MAX}.txt
"""

import os
import random
import string
import sys
import time

LINES = 1_000_000
POOL_CAP = 50_000   # max pool size to keep memory reasonable
CHARS = string.ascii_lowercase + string.digits
OUTDIR = "bench_data"

DUP_RATES = [0.10, 0.50, 0.80]
LEN_RANGES = [(1, 10), (10, 50), (50, 200)]


def rand_line(lo: int, hi: int) -> str:
    return "".join(random.choices(CHARS, k=random.randint(lo, hi)))


def generate(path: str, dup_rate: float, lo: int, hi: int) -> None:
    pool: list[str] = []
    buf: list[str] = []
    BUF_SIZE = 50_000

    with open(path, "w", buffering=1 << 20) as f:
        for _ in range(LINES):
            if pool and random.random() < dup_rate:
                line = random.choice(pool)
            else:
                line = rand_line(lo, hi)
                if len(pool) < POOL_CAP:
                    pool.append(line)

            buf.append(line)
            if len(buf) == BUF_SIZE:
                f.write("\n".join(buf))
                f.write("\n")
                buf.clear()

        if buf:
            f.write("\n".join(buf))
            f.write("\n")


def main() -> None:
    os.makedirs(OUTDIR, exist_ok=True)
    total = len(DUP_RATES) * len(LEN_RANGES)
    done = 0

    for dup in DUP_RATES:
        for lo, hi in LEN_RANGES:
            name = f"dup{int(dup*100):02d}_len{lo}-{hi}.txt"
            path = os.path.join(OUTDIR, name)
            label = f"dup={int(dup*100)}%  len={lo}-{hi}"
            print(f"[{done+1}/{total}] {label:30s} -> {path} ...", end=" ", flush=True)
            t0 = time.perf_counter()
            generate(path, dup, lo, hi)
            elapsed = time.perf_counter() - t0
            size_mb = os.path.getsize(path) / 1_048_576
            print(f"{elapsed:.1f}s  {size_mb:.1f} MB")
            done += 1

    print(f"\nDone. {total} files written to {OUTDIR}/")


if __name__ == "__main__":
    random.seed(42)
    main()
