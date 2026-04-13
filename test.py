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
        f.seek(0)
        return subprocess.run(args, stdin=f, capture_output=True).stdout.decode()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestYuniq(unittest.TestCase):
    def check(self, data: str, expected: str, extra_args: list[str] = None) -> None:
        """Assert both pipe and mmap outputs equal expected, with --fast and --lean."""
        for run in (run_pipe, run_file):
            for args in ([], ["--fast"], ["--lean"]):
                self.assertEqual(run(data, args + (extra_args or [])), expected, f"{run.__name__}{args}")

    def check_count(self, data: str, expected: str, extra_args: list[str] = None) -> None:
        """Assert -c output for both pipe and mmap paths, with --lean"""
        for run in (run_pipe, run_file):
            for args in ([], ["--lean"]):
                self.assertEqual(run(data, ["-c"] + args + (extra_args or [])), expected, run.__name__)

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

    # def test_crlf_vs_lf_same_content(self):
    #     # \r\n and \n strip to the same key; first occurrence wins
    #     self.check("foo\r\nfoo\n", "foo\r\n")

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

    def test_check_chars_zero(self):
        # -w 0: only the first line is emitted, regardless of duplicates
        self.check("foo\nbar\nfoo\n", "foo\n", ["-w", "0"])
        self.check("only\n", "only\n", ["-w", "0"])
        self.check("", "", ["-w", "0"])

    def test_skip_fields_basic(self):
        self.check("a foo\nb foo\na bar\n", "a foo\na bar\n", ["-f", "1"])

    def test_skip_fields_zero(self):
        # -f 0 is a no-op: compare full lines
        self.check("a foo\nb foo\n", "a foo\nb foo\n", ["-f", "0"])

    def test_skip_fields_beyond_end(self):
        # skipping more fields than exist → empty key → all lines collapse to first
        self.check("a\nb\nc\n", "a\n", ["-f", "5"])

    def test_skip_fields_multi_space(self):
        # multiple spaces between fields are treated as one separator
        self.check("a  foo\nb  foo\na  bar\n", "a  foo\na  bar\n", ["-f", "1"])

    def test_skip_fields_with_skip_chars(self):
        # -f 1 skips first field, then -s 3 skips 3 more chars of the remainder
        self.check("ts1 xx rest\nts2 yy rest\nts1 xx other\n", "ts1 xx rest\nts1 xx other\n", ["-f", "1", "-s", "3"])

    def test_count_basic(self):
        # -c sorts ascending by default
        self.check_count("a\nb\na\nc\na\n", "1\tb\n1\tc\n3\ta\n")

    def test_count_order_preserved(self):
        # -S disables sorting, output is insertion order
        self.check_count("b\na\nb\n", "2\tb\n1\ta\n", ["-S"])

    def test_count_all_unique(self):
        self.check_count("x\ny\nz\n", "1\tx\n1\ty\n1\tz\n")

    def test_count_all_same(self):
        self.check_count("x\nx\nx\n", "3\tx\n")

    def test_count_empty(self):
        self.check_count("", "")

    def test_count_no_trailing_newline_dup(self):
        # last line is a dup of an earlier newline-terminated occurrence;
        # output uses the stored pointer (with \n)
        self.check_count("a\nb\na\nb", "2\ta\n2\tb\n")

    def test_count_no_trailing_newline_unique(self):
        # sorting moves the unterminated line away from the end; a \n is added
        self.check_count("a\na\nb", "1\tb\n2\ta\n")

    def test_count_check_chars(self):
        # -w 3: "foobar" and "foobaz" share the same 3-char key "foo"
        self.check_count("foobar\nfoobaz\nqux\n", "1\tqux\n2\tfoobar\n", ["-w", "3"])

    def test_count_skip_fields(self):
        # -f 1: skip first field, "foo" is the common key for ts1/ts2 lines
        self.check_count("ts1 foo\nts2 foo\nts1 bar\n", "1\tts1 bar\n2\tts1 foo\n", ["-f", "1"])

    def test_sort_ascending(self):
        # default sort is ascending
        self.check_count("a\nb\na\nc\na\nb\n", "1\tc\n2\tb\n3\ta\n")

    def test_sort_descending(self):
        self.check_count("a\nb\na\nc\na\nb\n", "3\ta\n2\tb\n1\tc\n", ["-r"])

    def test_sort_stable_ties(self):
        # equal counts preserve first-seen order
        self.check_count("b\na\nb\na\n", "2\tb\n2\ta\n")

    def test_sort_already_ordered(self):
        self.check_count("c\nb\nb\na\na\na\n", "1\tc\n2\tb\n3\ta\n")

    def test_no_sort(self):
        # -S preserves insertion order
        self.check_count("a\nb\na\nc\na\nb\n", "3\ta\n2\tb\n1\tc\n", ["-S"])


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
