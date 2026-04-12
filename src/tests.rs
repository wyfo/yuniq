use crate::{Deduplicator, DeduplicatorSeen, process_chunk};

fn run(data: &[u8], is_final: bool) -> (Vec<u8>, usize) {
    let mut out = Vec::new();
    let mut dedup = Deduplicator {
        seen: DeduplicatorSeen::new(16, false),
        skip_chars: None,
        check_chars: None,
    };
    let rem = process_chunk(data, &mut dedup, is_final, |s| {
        out.extend_from_slice(s);
        Ok(())
    })
    .unwrap();
    if is_final {
        assert_eq!(rem, 0);
    }
    (out, rem)
}

#[test]
fn all_unique() {
    let (out, _) = run(b"a\nb\n", false);
    assert_eq!(out, b"a\nb\n");
}

#[test]
fn duplicate() {
    let (out, _) = run(b"a\nb\na\n", false);
    assert_eq!(out, b"a\nb\n");
}

#[test]
fn all_dupes() {
    let (out, _) = run(b"x\nx\nx\n", false);
    assert_eq!(out, b"x\n");
}

#[test]
fn empty_not_final() {
    let (out, rem) = run(b"", false);
    assert_eq!(out, b"");
    assert_eq!(rem, 0);
}

#[test]
fn empty_final() {
    let (out, _) = run(b"", true);
    assert_eq!(out, b"");
}

#[test]
fn final_unique_tail() {
    let (out, _) = run(b"a\nb", true);
    assert_eq!(out, b"a\nb");
}

#[test]
fn final_dup_tail() {
    let (out, _) = run(b"a\nb\na", true);
    assert_eq!(out, b"a\nb\n");
}

#[test]
fn non_final_tail() {
    let (out, rem) = run(b"a\nb", false);
    assert_eq!(out, b"a\n");
    assert_eq!(rem, 1);
}

#[test]
fn crlf() {
    let (out, _) = run(b"a\r\nb\r\na\r\n", false);
    assert_eq!(out, b"a\r\nb\r\n");
}

#[test]
fn crlf_vs_lf_same_key() {
    let (out, _) = run(b"a\r\na\n", false);
    assert_eq!(out, b"a\r\n");
}

#[test]
fn blank_lines() {
    let (out, _) = run(b"\na\n\n", false);
    assert_eq!(out, b"\na\n");
}

#[test]
fn write_batching() {
    let mut calls = 0usize;
    let mut dedup = Deduplicator {
        seen: DeduplicatorSeen::new(16, false),
        skip_chars: None,
        check_chars: None,
    };
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
