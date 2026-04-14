use bumpalo::Bump;
use unicode_normalization::{IsNormalized, UnicodeNormalization, is_nfc_quick};

const CONT_MASK: u8 = 0b0011_1111;

#[inline]
const fn utf8_first_byte(byte: u8, width: u32) -> u32 {
    (byte & (0x7F >> width)) as u32
}

/// Returns the value of `ch` updated with continuation byte `byte`.
#[inline]
const fn utf8_acc_cont_byte(ch: u32, byte: u8) -> u32 {
    (ch << 6) | (byte & CONT_MASK) as u32
}

struct Chars<'a> {
    iter: std::slice::Iter<'a, u8>,
    error: bool,
}

impl<'a> Chars<'a> {
    #[inline]
    fn next_or_err(&mut self) -> Option<u8> {
        if let Some(ch) = self.iter.next() {
            return Some(*ch);
        }
        self.error = true;
        None
    }
}

// `unicode_encode` package was way too slow when tested, so I've adapted
// std::str::Chars code
impl Iterator for Chars<'_> {
    type Item = char;
    fn next(&mut self) -> Option<char> {
        // Decode UTF-8
        let x = *self.iter.next()?;
        if x < 128 {
            // SAFETY: x is a valid utf8 char
            return Some(unsafe { char::from_u32_unchecked(x as u32) });
        }

        // Multibyte case follows
        // Decode from a byte combination out of: [[[x y] z] w]
        // NOTE: Performance is sensitive to the exact formulation here
        let init = utf8_first_byte(x, 2);
        let y = self.next_or_err()?;
        let mut ch = utf8_acc_cont_byte(init, y);
        if x >= 0xE0 {
            // [[x y z] w] case
            // 5th bit in 0xE0 .. 0xEF is always clear, so `init` is still valid
            let z = self.next_or_err()?;
            let y_z = utf8_acc_cont_byte((y & CONT_MASK) as u32, z);
            ch = init << 12 | y_z;
            if x >= 0xF0 {
                // [x y z w] case
                // use only the lower 3 bits of `init`
                let w = self.next_or_err()?;
                ch = (init & 7) << 18 | utf8_acc_cont_byte(y_z, w);
            }
        }

        // SAFETY: ch is a valid utf8 char
        Some(unsafe { char::from_u32_unchecked(ch) })
    }
}

#[cold]
pub(crate) fn normalize_unicode<'arena>(data: &[u8], arena: &'arena Bump) -> Option<&'arena [u8]> {
    let mut chars = Chars {
        iter: data.iter(),
        error: false,
    };
    if is_nfc_quick(chars.by_ref()) != IsNormalized::Yes && !chars.error {
        // `is_nfc_quick` short-circuits on the first non-NFC char, so
        // it may not have consumed the full iterator; drain the rest to
        // ensure `error` reflects any invalid UTF-8 bytes that follow.
        if chars.iter.len() > 0 {
            for _ in chars.by_ref() {}
        }
        if !chars.error {
            // SAFETY: no error means every byte in `data` decoded
            // successfully through `to_utf8chars`, so `data` is valid UTF-8.
            let utf8 = unsafe { str::from_utf8_unchecked(data) };
            let string = bumpalo::collections::String::from_iter_in(utf8.nfc(), arena);
            return Some(string.into_bump_str().as_bytes());
        }
    }
    None
}
