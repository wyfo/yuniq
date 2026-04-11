use crate::{Deduplicator, process_chunk};

fn run(data: &[u8], is_final: bool) -> (Vec<u8>, usize) {
    let mut out = Vec::new();
    let mut dedup = Deduplicator::new(16);
    let rem = process_chunk(data, &mut dedup, is_final, |s| {
        out.extend_from_slice(s);
        Ok(())
    })
    .unwrap();
    (out, rem)
}

#[test]
fn all_unique() {
    let (out, rem) = run(b"a\nb\n", false);
    assert_eq!(out, b"a\nb\n");
    assert_eq!(rem, 0);
}

#[test]
fn duplicate() {
    let (out, rem) = run(b"a\nb\na\n", false);
    assert_eq!(out, b"a\nb\n");
    assert_eq!(rem, 0);
}

#[test]
fn all_dupes() {
    let (out, rem) = run(b"x\nx\nx\n", false);
    assert_eq!(out, b"x\n");
    assert_eq!(rem, 0);
}

#[test]
fn empty_not_final() {
    let (out, rem) = run(b"", false);
    assert_eq!(out, b"");
    assert_eq!(rem, 0);
}

#[test]
fn empty_final() {
    let (out, rem) = run(b"", true);
    assert_eq!(out, b"");
    assert_eq!(rem, 0);
}

#[test]
fn final_unique_tail() {
    let (out, rem) = run(b"a\nb", true);
    assert_eq!(out, b"a\nb");
    assert_eq!(rem, 0);
}

#[test]
fn final_dup_tail() {
    let (out, rem) = run(b"a\nb\na", true);
    assert_eq!(out, b"a\nb\n");
    assert_eq!(rem, 1); // duplicate tail is not consumed
}

#[test]
fn non_final_tail() {
    let (out, rem) = run(b"a\nb", false);
    assert_eq!(out, b"a\n");
    assert_eq!(rem, 1);
}

#[test]
fn crlf() {
    let (out, rem) = run(b"a\r\nb\r\na\r\n", false);
    assert_eq!(out, b"a\r\nb\r\n");
    assert_eq!(rem, 0);
}

#[test]
fn crlf_vs_lf_same_key() {
    let (out, rem) = run(b"a\r\na\n", false);
    assert_eq!(out, b"a\r\n");
    assert_eq!(rem, 0);
}

#[test]
fn blank_lines() {
    let (out, rem) = run(b"\na\n\n", false);
    assert_eq!(out, b"\na\n");
    assert_eq!(rem, 0);
}

#[test]
fn write_batching() {
    let mut calls = 0usize;
    let mut dedup = Deduplicator::new(16);
    process_chunk(b"a\nb\nc\n", &mut dedup, false, |s| {
        if !s.is_empty() {
            calls += 1;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(
        calls, 1,
        "expected a single write call for all-unique lines"
    );
}
