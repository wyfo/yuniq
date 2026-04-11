#!/usr/bin/env python3
"""Tests for yuniq.

Both execution paths (mmap/file and pipe) are compared against a Python oracle
that mirrors the Rust dedup logic exactly.
"""

import os
import subprocess
import sys
import tempfile

BINARY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "target", "debug", "yuniq")
BUF_SIZE = 64 * 1024  # must match const BUF_SIZE in main.rs

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"

_failures = 0


# ---------------------------------------------------------------------------
# Oracle — mirrors the Rust logic exactly
# ---------------------------------------------------------------------------

def _trim_newline(b: bytes) -> bytes:
    """Mirrors trim_newline() in main.rs: strip \\n then optionally \\r."""
    if b and b[-1:] == b'\n':
        b = b[:-1]
    if b and b[-1:] == b'\r':
        b = b[:-1]
    return b


def yuniq_oracle(data: bytes) -> bytes:
    """Python reference implementation of yuniq."""
    seen: set[bytes] = set()
    out: list[bytes] = []
    pos = 0
    while pos < len(data):
        nl = data.find(b'\n', pos)
        end = nl + 1 if nl != -1 else len(data)
        line = data[pos:end]
        key = _trim_newline(line)
        if key not in seen:
            seen.add(key)
            out.append(line)
        pos = end
    return b''.join(out)


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------

def run_pipe(data: bytes) -> bytes:
    """Feed data through a pipe → triggers the streaming/BufReader path."""
    return subprocess.run([BINARY], input=data, capture_output=True).stdout


def run_file(data: bytes) -> bytes:
    """Feed data from a regular file → triggers the mmap path."""
    with tempfile.TemporaryFile() as f:
        f.write(data)
        f.flush()
        return subprocess.run([BINARY], stdin=f, capture_output=True).stdout


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------

def check(label: str, got: bytes, want: bytes) -> None:
    global _failures
    if got == want:
        print(f"  {PASS}  {label}")
    else:
        print(f"  {FAIL}  {label}")

        # Truncate long diffs so the output stays readable
        def _repr(b: bytes, limit: int = 120) -> str:
            r = repr(b)
            return r if len(r) <= limit else r[:limit] + "…"

        print(f"         got:  {_repr(got)}")
        print(f"         want: {_repr(want)}")
        _failures += 1


def run_both(label: str, data: bytes) -> None:
    """Run both paths and compare each to the oracle."""
    want = yuniq_oracle(data)
    check(f"[pipe] {label}", run_pipe(data), want)
    check(f"[mmap] {label}", run_file(data), want)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_basic_dedup():
    print("basic deduplication")
    run_both("keeps first, drops later dupes", b"foo\nbar\nfoo\nbaz\nbar\n")


def test_all_unique():
    print("all unique lines")
    run_both("unchanged", b"alpha\nbeta\ngamma\n")


def test_all_dupes():
    print("all duplicate lines")
    run_both("single copy kept", b"x\nx\nx\n")


def test_empty_input():
    print("empty input")
    run_both("empty → empty", b"")


def test_no_trailing_newline():
    print("no trailing newline on last line")
    run_both("unterminated last line deduped", b"foo\nbar\nfoo\nbar")


def test_crlf():
    print("CRLF line endings")
    run_both("CRLF dedup", b"foo\r\nbar\r\nfoo\r\n")
    run_both("CRLF vs LF treated as same content", b"foo\r\nfoo\n")


def test_blank_lines():
    print("blank lines")
    run_both("blank line deduped", b"foo\n\nbar\n\n")


def test_single_line():
    print("single line input")
    run_both("no newline", b"hello")
    run_both("with newline", b"hello\n")


def test_order_preserved():
    print("first-occurrence order preserved")
    run_both("order", b"c\nb\na\nb\nc\n")


def test_line_larger_than_buf_size():
    """Lines longer than BUF_SIZE force the pipe path to double its buffer."""
    print(f"lines larger than BUF_SIZE ({BUF_SIZE} bytes)")

    big = b"A" * (BUF_SIZE + 1000)
    huge = b"B" * (BUF_SIZE * 3 + 7)

    run_both("single big line, unique", big + b"\n")
    run_both("single big line, duplicated", big + b"\n" + big + b"\n")
    run_both("big line between normal lines",
             b"before\n" + big + b"\nbefore\n" + big + b"\nafter\n")
    run_both("very large line (3× BUF_SIZE)", huge + b"\n" + huge + b"\n")
    run_both("two distinct big lines", big + b"\n" + huge + b"\n")


def test_line_at_buf_size_boundary():
    """Boundary conditions around BUF_SIZE."""
    print("lines at BUF_SIZE boundary")

    for n, label in [
        (BUF_SIZE - 1, "BUF_SIZE - 1"),
        (BUF_SIZE, "BUF_SIZE"),
        (BUF_SIZE + 1, "BUF_SIZE + 1"),
    ]:
        line = b"X" * n
        run_both(f"{label} bytes: deduped", line + b"\n" + line + b"\n")


def test_many_lines_stress():
    print("many lines (stress)")
    lines = [f"line{i}".encode() for i in range(5000)]
    data = b"\n".join(lines * 3) + b"\n"
    run_both("5 000 unique lines × 3 copies", data)


# ---------------------------------------------------------------------------

def build():
    print("Building yuniq…")
    result = subprocess.run(
        ["cargo", "build"],
        capture_output=True, text=True,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(1)
    print(f"Binary: {BINARY}\n")


def main():
    build()

    for test in [
        test_basic_dedup,
        test_all_unique,
        test_all_dupes,
        test_empty_input,
        test_no_trailing_newline,
        test_crlf,
        test_blank_lines,
        test_single_line,
        test_order_preserved,
        test_line_larger_than_buf_size,
        test_line_at_buf_size_boundary,
        test_many_lines_stress,
    ]:
        test()

    print()
    if _failures == 0:
        print(f"{PASS} All tests passed.")
    else:
        print(f"{FAIL} {_failures} test(s) failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
