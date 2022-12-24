
use std::fmt;

/// Write a hexdump of the provided byte slice.
pub fn hexdump(
    f: &mut fmt::Formatter,
    prefix: &str,
    buffer: &[u8],
) -> std::result::Result<(), std::fmt::Error> {
    const COLUMNS: usize = 16;
    let mut offset: usize = 0;
    if buffer.is_empty() {
        // For a zero-length buffer, at least print an offset instead of
        // nothing.
        write!(f, "{}{:04x}: ", prefix, 0)?;
    }
    while offset < buffer.len() {
        write!(f, "{}{:04x}: ", prefix, offset)?;

        // Determine row byte range
        let next_offset = offset + COLUMNS;
        let (row_size, padding) = if next_offset <= buffer.len() {
            (COLUMNS, 0)
        } else {
            (buffer.len() - offset, next_offset - buffer.len())
        };
        let row = &buffer[offset..offset + row_size];

        // Print hex representation
        for b in row {
            write!(f, "{:02x} ", b)?;
        }
        for _ in 0..padding {
            write!(f, "   ")?;
        }

        // Print ASCII representation
        for b in row {
            write!(
                f,
                "{}",
                match *b {
                    c @ 0x20..=0x7E => c as char,
                    _ => '.',
                }
            )?;
        }

        offset += COLUMNS;
        if offset < buffer.len() {
            writeln!(f)?;
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub struct Hex<'a>(pub &'a [u8]);
impl<'a> fmt::Display for Hex<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        hexdump(f, "", self.0)
    }
}

pub fn hex(bytes: &[u8]) -> Hex {
    Hex(bytes)
}

/// Provide a convenience function for getting a byte slice based on its length
/// instead of its ending position.
pub trait Slice {
    fn slice(&self, offset: usize, size: usize) -> &[u8];
}

impl<'a> Slice for &'a [u8] {
    #[inline]
    fn slice(&self, offset: usize, size: usize) -> &[u8] {
        &self[offset..offset + size]
    }
}

/// Provide a convenience function for getting a mutable byte slice based on
/// its length instead of its ending position.
pub trait SliceMut {
    fn slice_mut(&mut self, offset: usize, size: usize) -> &mut [u8];
}

impl<'a> SliceMut for &'a mut [u8] {
    #[inline]
    fn slice_mut(&mut self, offset: usize, size: usize) -> &mut [u8] {
        &mut self[offset..offset + size]
    }
}
