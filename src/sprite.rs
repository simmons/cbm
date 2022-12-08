pub const SPRITE_SIZE: usize = 63;
pub const SPRITE_WIDTH: usize = 24;
pub const SPRITE_HEIGHT: usize = 21;

/// Store the data associated with a sprite image.  Multicolor sprites are not
/// supported at this time.
pub struct Sprite {
    data: Vec<u8>,
}

/// These codepoints are block element glyphs that can represent any combination of a 2x2 pixel
/// bitmap.  The most significant two bits are the upper pixels, and the least significant bits are
/// the lower pixels.
#[rustfmt::skip]
static UNICODE_BLOCK_ELEMENTS: [char; 16] = [
    ' ', '\u{2597}', '\u{2596}', '\u{2584}',
    '\u{259D}', '\u{2590}', '\u{259E}', '\u{259F}',
    '\u{2598}', '\u{259A}', '\u{258C}', '\u{2599}',
    '\u{2580}', '\u{259C}', '\u{259B}', '\u{2588}',
];

impl Sprite {
    pub fn from_bytes(bytes: &[u8]) -> Sprite {
        assert_eq!(bytes.len(), SPRITE_SIZE);
        Sprite {
            data: bytes.to_vec(),
        }
    }

    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert_eq!(bytes.len(), SPRITE_SIZE);
        bytes.copy_from_slice(&self.data);
    }

    pub fn to_unicode(&self) -> String {
        const ROWS: usize = (SPRITE_HEIGHT + 1) / 2;
        const COLUMNS: usize = (SPRITE_WIDTH + 1) / 2;
        // Unicode block elements (but not the empty block which is space) occupy three
        // bytes each when encoded as UTF-8.
        const MAX_BYTES: usize = 3 * ROWS * COLUMNS;
        let mut string = String::with_capacity(MAX_BYTES);
        for row in 0..ROWS {
            for offset in 0..3 {
                let top_byte = self.data[row * 2 * 3 + offset];
                let bottom_byte = if row * 2 + 1 < SPRITE_HEIGHT {
                    self.data[(row * 2 + 1) * 3 + offset]
                } else {
                    0 // The last row has no bottom half.
                };
                for block in 0..4 {
                    let top_bits = top_byte >> ((3 - block) * 2) & 0x03;
                    let bottom_bits = bottom_byte >> ((3 - block) * 2) & 0x03;
                    let glyph = (top_bits << 2) | bottom_bits;
                    string.push(UNICODE_BLOCK_ELEMENTS[glyph as usize]);
                }
            }
            string.push('\n');
        }
        string
    }
}
