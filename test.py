#!/usr/bin/env python3
"""Tests for yuniq.

Both execution paths (mmap/file and pipe) are compared against a Python oracle
that mirrors the Rust dedup logic exactly.
"""

import os
import re
import subprocess
import sys
import tempfile
import unittest

BINARY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "target", "debug", "yuniq"
)
BUF_SIZE = 64 * 1024  # must match DEFAULT_BUF_SIZE in main.rs


# ---------------------------------------------------------------------------
# Oracle
# ---------------------------------------------------------------------------


def oracle(data: bytes) -> bytearray:
    """Python reference implementation of yuniq."""
    seen: set[bytes] = set()
    out = bytearray()
    for m in re.finditer(rb"(?P<line>(?P<key>.*?)(\r\n|\n|$))", data):
        if m["key"] not in seen:
            seen.add(m["key"])
            out.extend(m["line"])
    return out


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------


def run_pipe(data: bytes, extra_args: list[str] = None) -> bytes:
    """Feed data through a pipe → triggers the streaming/BufReader path."""
    args = [BINARY] + (extra_args or [])
    return subprocess.run(args, input=data, capture_output=True).stdout


def run_file(data: bytes, extra_args: list[str] = None) -> bytes:
    """Feed data from a regular file → triggers the mmap path."""
    args = [BINARY] + (extra_args or [])
    with tempfile.TemporaryFile() as f:
        f.write(data)
        f.flush()
        return subprocess.run(args, stdin=f, capture_output=True).stdout


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestYuniq(unittest.TestCase):
    def check(self, data: bytes) -> None:
        """Assert pipe and mmap outputs both match the oracle, for default and --fast."""
        expected = oracle(data)
        for run in [run_pipe, run_file]:
            for args in [[], ["--fast"]]:
                self.assertEqual(run_pipe(data, args), expected, f"{run.__name__}({args=})")

    def test_basic_dedup(self):
        self.check(b"foo\nbar\nfoo\nbaz\nbar\n")

    def test_all_unique(self):
        self.check(b"alpha\nbeta\ngamma\n")

    def test_all_dupes(self):
        self.check(b"x\nx\nx\n")

    def test_empty_input(self):
        self.check(b"")

    def test_no_trailing_newline(self):
        self.check(b"foo\nbar\nfoo\nbar")

    def test_crlf(self):
        self.check(b"foo\r\nbar\r\nfoo\r\n")

    def test_crlf_vs_lf_same_content(self):
        self.check(b"foo\r\nfoo\n")

    def test_blank_lines(self):
        self.check(b"foo\n\nbar\n\n")

    def test_single_line_no_newline(self):
        self.check(b"hello")

    def test_single_line_with_newline(self):
        self.check(b"hello\n")

    def test_order_preserved(self):
        self.check(b"c\nb\na\nb\nc\n")

    def test_line_larger_than_buf_size(self):
        big = b"A" * (BUF_SIZE + 1000)
        self.check(big + b"\n")
        self.check(big + b"\n" + big + b"\n")
        self.check(b"before\n" + big + b"\nbefore\n" + big + b"\nafter\n")

    def test_line_much_larger_than_buf_size(self):
        huge = b"B" * (BUF_SIZE * 3 + 7)
        self.check(huge + b"\n" + huge + b"\n")

    def test_two_distinct_big_lines(self):
        big = b"A" * (BUF_SIZE + 1000)
        huge = b"B" * (BUF_SIZE * 3 + 7)
        self.check(big + b"\n" + huge + b"\n")

    def test_buf_size_boundary_minus_one(self):
        line = b"X" * (BUF_SIZE - 1)
        self.check(line + b"\n" + line + b"\n")

    def test_buf_size_boundary_exact(self):
        line = b"X" * BUF_SIZE
        self.check(line + b"\n" + line + b"\n")

    def test_buf_size_boundary_plus_one(self):
        line = b"X" * (BUF_SIZE + 1)
        self.check(line + b"\n" + line + b"\n")

    def test_many_lines_stress(self):
        lines = [f"line{i}".encode() for i in range(5000)]
        self.check(b"\n".join(lines * 3) + b"\n")

    def test_check_chars(self):
        data = b"foobar\nfoobaz\nfoobar\nqux\n"
        expected = b"foobar\nqux\n"
        for run in [run_pipe, run_file]:
            self.assertEqual(run(data, ["-w", "3"]), expected, run.__name__)

    def test_check_chars_shorter_than_limit(self):
        # lines shorter than -w limit should not panic and compare by full content
        data = b"ab\nab\ncd\n"
        expected = b"ab\ncd\n"
        for run in [run_pipe, run_file]:
            self.assertEqual(run(data, ["-w", "5"]), expected, run.__name__)


# ---------------------------------------------------------------------------


def build():
    result = subprocess.run(
        ["cargo", "build"],
        capture_output=True,
        text=True,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    build()
    unittest.main()
