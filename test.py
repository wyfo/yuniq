#!/usr/bin/env python3
"""Tests for yuniq — both mmap (file) and pipe execution paths."""

import os
import subprocess
import sys
import tempfile
import unittest

BINARY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "target", "debug", "yuniq"
)
BUF_SIZE = 64 * 1024  # must match DEFAULT_BUF_SIZE in main.rs


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------


def run_pipe(data: str, extra_args: list[str] = None) -> str:
    """Feed data through a pipe → triggers the streaming/BufReader path."""
    args = [BINARY] + (extra_args or [])
    result = subprocess.run(args, input=data.encode(), capture_output=True)
    return result.stdout.decode()


def run_file(data: str, extra_args: list[str] = None) -> str:
    """Feed data from a regular file → triggers the mmap path."""
    args = [BINARY] + (extra_args or [])
    with tempfile.TemporaryFile() as f:
        f.write(data.encode())
        f.flush()
        return subprocess.run(args, stdin=f, capture_output=True).stdout.decode()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestYuniq(unittest.TestCase):
    def check(self, data: str, expected: str, extra_args: list[str] = None) -> None:
        """Assert both pipe and mmap outputs equal expected."""
        self.assertEqual(run_pipe(data, extra_args), expected, "pipe path")
        self.assertEqual(run_file(data, extra_args), expected, "mmap path")

    def test_basic_dedup(self):
        self.check("foo\nbar\nfoo\nbaz\nbar\n", "foo\nbar\nbaz\n")

    def test_all_unique(self):
        self.check("alpha\nbeta\ngamma\n", "alpha\nbeta\ngamma\n")

    def test_all_dupes(self):
        self.check("x\nx\nx\n", "x\n")

    def test_empty_input(self):
        self.check("", "")

    def test_no_trailing_newline(self):
        # last "bar" is a dup of "bar\n", so output ends with \n from first occurrence
        self.check("foo\nbar\nfoo\nbar", "foo\nbar\n")

    def test_no_trailing_newline_unique_last(self):
        # last line is unique and unterminated — output must not gain a \n
        self.check("foo\nbar", "foo\nbar")

    def test_crlf(self):
        self.check("foo\r\nbar\r\nfoo\r\n", "foo\r\nbar\r\n")

    def test_crlf_vs_lf_same_content(self):
        # \r\n and \n strip to the same key; first occurrence wins
        self.check("foo\r\nfoo\n", "foo\r\n")

    def test_blank_lines(self):
        self.check("foo\n\nbar\n\n", "foo\n\nbar\n")

    def test_single_line_no_newline(self):
        self.check("hello", "hello")

    def test_single_line_with_newline(self):
        self.check("hello\n", "hello\n")

    def test_order_preserved(self):
        self.check("c\nb\na\nb\nc\n", "c\nb\na\n")

    def test_line_larger_than_buf_size(self):
        big = "A" * (BUF_SIZE + 1000)
        self.check(f"{big}\n", f"{big}\n")
        self.check(f"{big}\n{big}\n", f"{big}\n")
        self.check(f"before\n{big}\nbefore\n{big}\nafter\n", f"before\n{big}\nafter\n")

    def test_line_much_larger_than_buf_size(self):
        huge = "B" * (BUF_SIZE * 3 + 7)
        self.check(f"{huge}\n{huge}\n", f"{huge}\n")

    def test_two_distinct_big_lines(self):
        big = "A" * (BUF_SIZE + 1000)
        huge = "B" * (BUF_SIZE * 3 + 7)
        self.check(f"{big}\n{huge}\n", f"{big}\n{huge}\n")

    def test_buf_size_boundary_minus_one(self):
        line = "X" * (BUF_SIZE - 1)
        self.check(f"{line}\n{line}\n", f"{line}\n")

    def test_buf_size_boundary_exact(self):
        line = "X" * BUF_SIZE
        self.check(f"{line}\n{line}\n", f"{line}\n")

    def test_buf_size_boundary_plus_one(self):
        line = "X" * (BUF_SIZE + 1)
        self.check(f"{line}\n{line}\n", f"{line}\n")

    def test_many_lines_stress(self):
        lines = "".join(f"line{i}\n" for i in range(5000))
        self.check(lines * 3, lines)

    def test_check_chars(self):
        self.check("foobar\nfoobaz\nfoobar\nqux\n", "foobar\nqux\n", ["-w", "3"])

    def test_check_chars_shorter_than_limit(self):
        # lines shorter than -w limit should not panic and compare by full content
        self.check("ab\nab\ncd\n", "ab\ncd\n", ["-w", "5"])


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
