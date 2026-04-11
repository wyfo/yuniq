#!/usr/bin/env python3
"""Tests for yuniq.

Both execution paths (mmap/file and pipe) are compared against a Python oracle
that mirrors the Rust dedup logic exactly.
"""

import os
import subprocess
import sys
import tempfile
import unittest

BINARY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "target", "debug", "yuniq")
BUF_SIZE = 64 * 1024  # must match const BUF_SIZE in main.rs


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
# Tests
# ---------------------------------------------------------------------------

class TestYuniq(unittest.TestCase):

    def assertBoth(self, data: bytes) -> None:
        """Assert pipe and mmap outputs both match the oracle."""
        expected = yuniq_oracle(data)
        self.assertEqual(run_pipe(data), expected, "pipe path")
        self.assertEqual(run_file(data), expected, "mmap path")

    def test_basic_dedup(self):
        self.assertBoth(b"foo\nbar\nfoo\nbaz\nbar\n")

    def test_all_unique(self):
        self.assertBoth(b"alpha\nbeta\ngamma\n")

    def test_all_dupes(self):
        self.assertBoth(b"x\nx\nx\n")

    def test_empty_input(self):
        self.assertBoth(b"")

    def test_no_trailing_newline(self):
        self.assertBoth(b"foo\nbar\nfoo\nbar")

    def test_crlf(self):
        self.assertBoth(b"foo\r\nbar\r\nfoo\r\n")

    def test_crlf_vs_lf_same_content(self):
        self.assertBoth(b"foo\r\nfoo\n")

    def test_blank_lines(self):
        self.assertBoth(b"foo\n\nbar\n\n")

    def test_single_line_no_newline(self):
        self.assertBoth(b"hello")

    def test_single_line_with_newline(self):
        self.assertBoth(b"hello\n")

    def test_order_preserved(self):
        self.assertBoth(b"c\nb\na\nb\nc\n")

    def test_line_larger_than_buf_size(self):
        big = b"A" * (BUF_SIZE + 1000)
        self.assertBoth(big + b"\n")
        self.assertBoth(big + b"\n" + big + b"\n")
        self.assertBoth(b"before\n" + big + b"\nbefore\n" + big + b"\nafter\n")

    def test_line_much_larger_than_buf_size(self):
        huge = b"B" * (BUF_SIZE * 3 + 7)
        self.assertBoth(huge + b"\n" + huge + b"\n")

    def test_two_distinct_big_lines(self):
        big = b"A" * (BUF_SIZE + 1000)
        huge = b"B" * (BUF_SIZE * 3 + 7)
        self.assertBoth(big + b"\n" + huge + b"\n")

    def test_buf_size_boundary_minus_one(self):
        line = b"X" * (BUF_SIZE - 1)
        self.assertBoth(line + b"\n" + line + b"\n")

    def test_buf_size_boundary_exact(self):
        line = b"X" * BUF_SIZE
        self.assertBoth(line + b"\n" + line + b"\n")

    def test_buf_size_boundary_plus_one(self):
        line = b"X" * (BUF_SIZE + 1)
        self.assertBoth(line + b"\n" + line + b"\n")

    def test_many_lines_stress(self):
        lines = [f"line{i}".encode() for i in range(5000)]
        self.assertBoth(b"\n".join(lines * 3) + b"\n")


# ---------------------------------------------------------------------------

def build():
    result = subprocess.run(
        ["cargo", "build"],
        capture_output=True, text=True,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    build()
    unittest.main()
