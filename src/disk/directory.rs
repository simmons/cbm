//! CBM DOS directories

use std::fmt;
use std::fmt::Write;
use std::io;
use std::iter;

use crate::disk::block::{Location, Position, PositionedData, BLOCK_SIZE};
use crate::disk::chain::{ChainIterator, ChainSector};
use crate::disk::file::Scheme;
use crate::disk::geos::{GEOSFileStructure, GEOSFileType};
use crate::disk::{Disk, DiskError, PADDING_BYTE};
use crate::petscii::Petscii;

const FILE_TYPE_DEL: u8 = 0x00;
const FILE_TYPE_SEQ: u8 = 0x01;
const FILE_TYPE_PRG: u8 = 0x02;
const FILE_TYPE_USR: u8 = 0x03;
const FILE_TYPE_REL: u8 = 0x04;
const FILE_ATTRIB_FILE_TYPE_MASK: u8 = 0x0F;
const FILE_ATTRIB_UNUSED_MASK: u8 = 0x10;
const FILE_ATTRIB_SAVE_WITH_REPLACE_MASK: u8 = 0x20;
const FILE_ATTRIB_LOCKED_MASK: u8 = 0x40;
const FILE_ATTRIB_CLOSED_MASK: u8 = 0x80;

/// A directory entry categorizes files as SEQ, PRG, USR, or REL, along with a
/// pseudo-file-type of DEL to indicate deleted files.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum FileType {
    DEL,
    SEQ,
    PRG,
    USR,
    REL,
    Unknown(u8),
}

impl FileType {
    /// Return a string representation of this file type.
    pub fn from_string(string: &str) -> Option<FileType> {
        match string.to_uppercase().as_str() {
            "DEL" => Some(FileType::DEL),
            "SEQ" => Some(FileType::SEQ),
            "PRG" => Some(FileType::PRG),
            "USR" => Some(FileType::USR),
            "REL" => Some(FileType::REL),
            _ => None,
        }
    }
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            &FileType::DEL => "del",
            &FileType::SEQ => "seq",
            &FileType::PRG => "prg",
            &FileType::USR => "usr",
            &FileType::REL => "rel",
            &FileType::Unknown(_) => "unk",
        })
    }
}

/// We introduce the term "file attributes" to refer to the full 8-bit
/// directory entry field which contains the file type along with several flags.
#[derive(Clone)]
pub struct FileAttributes {
    /// Bits 0-3 indicate the file type.
    pub file_type: FileType,
    /// Bit 4 is unused, but we store it anyway so we can reproduce this field
    /// verbatim.
    pub unused_bit: bool,
    /// Bit 5 is the "save with replace" flag.
    pub save_with_replace_flag: bool,
    /// Bit 6 is the "locked" flag, indicated by a ">" in directory listings.
    pub locked_flag: bool,
    /// Bit 7 is the "closed" flag.  Files are normally closed, so this bit is
    /// normally set. Unclosed files are indicated in directory listings
    /// with a "*", leading to such files being known as "splat files".
    pub closed_flag: bool,
}

impl FileAttributes {
    /// Parse a byte into a `FileAttributes` struct.
    pub fn from_byte(byte: u8) -> FileAttributes {
        let file_type = match byte & FILE_ATTRIB_FILE_TYPE_MASK {
            FILE_TYPE_DEL => FileType::DEL,
            FILE_TYPE_SEQ => FileType::SEQ,
            FILE_TYPE_PRG => FileType::PRG,
            FILE_TYPE_USR => FileType::USR,
            FILE_TYPE_REL => FileType::REL,
            b => FileType::Unknown(b),
        };
        FileAttributes {
            file_type,
            unused_bit: byte & FILE_ATTRIB_UNUSED_MASK != 0,
            save_with_replace_flag: byte & FILE_ATTRIB_SAVE_WITH_REPLACE_MASK != 0,
            locked_flag: byte & FILE_ATTRIB_LOCKED_MASK != 0,
            closed_flag: byte & FILE_ATTRIB_CLOSED_MASK != 0,
        }
    }

    /// Generate the byte which encodes this `FileAttributes` struct.
    pub fn to_byte(&self) -> u8 {
        let mut byte = match &self.file_type {
            &FileType::DEL => FILE_TYPE_DEL,
            &FileType::SEQ => FILE_TYPE_SEQ,
            &FileType::PRG => FILE_TYPE_PRG,
            &FileType::USR => FILE_TYPE_USR,
            &FileType::REL => FILE_TYPE_REL,
            &FileType::Unknown(b) => b,
        };
        if self.unused_bit {
            byte = byte | FILE_ATTRIB_UNUSED_MASK
        };
        if self.save_with_replace_flag {
            byte = byte | FILE_ATTRIB_SAVE_WITH_REPLACE_MASK
        };
        if self.locked_flag {
            byte = byte | FILE_ATTRIB_LOCKED_MASK
        };
        if self.closed_flag {
            byte = byte | FILE_ATTRIB_CLOSED_MASK
        };
        byte
    }

    /// Return true if this entry represents a properly deleted ("scratched")
    /// file.  That is, if its file type is DEL and the closed flag is not
    /// set.
    pub fn is_scratched(&self) -> bool {
        self.file_type == FileType::DEL && !self.closed_flag
    }
}

impl fmt::Display for FileAttributes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            if self.closed_flag { ' ' } else { '*' },
            self.file_type,
            match (self.locked_flag, self.save_with_replace_flag) {
                (true, false) => "<",
                (false, true) => "@",
                (true, true) => "<@",
                (false, false) => " ",
            },
        )
    }
}

impl fmt::Debug for FileAttributes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // This is different from the Display impl in that there is no padding.
        if !self.closed_flag {
            f.write_char('*')?;
        }
        <FileType as fmt::Debug>::fmt(&self.file_type, f)?;
        f.write_str(match (self.locked_flag, self.save_with_replace_flag) {
            (true, false) => "<",
            (false, true) => "@",
            (true, true) => "<@",
            (false, false) => "",
        })
    }
}

pub(super) const ENTRY_SIZE: usize = 32;
const ENTRY_FILE_ATTRIBUTE_OFFSET: usize = 0x02;
const ENTRY_FIRST_SECTOR_OFFSET: usize = 0x03;
const ENTRY_FILENAME_OFFSET: usize = 0x05;
const ENTRY_FILENAME_LENGTH: usize = 16;
const ENTRY_EXTRA_OFFSET: usize = 0x15;
const EXTRA_SIZE: usize = 9;
const ENTRY_FILE_SIZE_OFFSET: usize = 0x1E;

/// Different file storage schemes use the nine directory entry bytes
/// 0x15..0x1E differently, hence the need for this enum to encapsulate the
/// different interpretations.
///
/// NOTE: Technically, the two bytes @ 0x1C..0x1E are used in "save and
/// replace" operations.  Maybe they should be parsed in some cases?  In
/// regular files, they should normally be 0x00 unless a save-and-replace
/// operation is currently in progress.  They are used to store the temporary
/// next track/sector as the replacement data is written.
#[derive(Clone, PartialEq)]
pub enum Extra {
    Linear(LinearExtra),
    Relative(RelativeExtra),
    GEOS(GEOSExtra),
}

impl Extra {
    pub fn default() -> Extra {
        Extra::Linear(LinearExtra::from_bytes(&[0u8; EXTRA_SIZE]))
    }

    pub fn from_bytes(scheme: Scheme, bytes: &[u8]) -> Extra {
        assert_eq!(bytes.len(), EXTRA_SIZE);
        match scheme {
            Scheme::Linear => Extra::Linear(LinearExtra::from_bytes(bytes)),
            Scheme::Relative => Extra::Relative(RelativeExtra::from_bytes(bytes)),
            Scheme::GEOSSequential | Scheme::GEOSVLIR => Extra::GEOS(GEOSExtra::from_bytes(bytes)),
        }
    }

    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert_eq!(bytes.len(), EXTRA_SIZE);
        match self {
            Extra::Linear(e) => e.to_bytes(bytes),
            Extra::Relative(e) => e.to_bytes(bytes),
            Extra::GEOS(e) => e.to_bytes(bytes),
        }
    }
}

impl fmt::Debug for Extra {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Extra::Linear(e) => e.fmt(f),
            Extra::Relative(e) => e.fmt(f),
            Extra::GEOS(e) => e.fmt(f),
        }
    }
}

/// The extra directory entry bytes used in regular files.  These should all be
/// unused, so we simply preserve whatever bytes are present.
#[derive(Clone, PartialEq)]
pub struct LinearExtra {
    pub unused: Vec<u8>, // 9 bytes
}

impl LinearExtra {
    pub fn from_bytes(bytes: &[u8]) -> LinearExtra {
        assert_eq!(bytes.len(), EXTRA_SIZE);
        LinearExtra {
            unused: bytes.to_vec(),
        }
    }

    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert_eq!(bytes.len(), EXTRA_SIZE);
        (&mut bytes[..]).copy_from_slice(&self.unused);
    }
}

impl fmt::Debug for LinearExtra {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "REGULAR")
    }
}

/// The extra directory entry bytes used in relative files, such as the record
/// length and the location of the first side sector.
#[derive(Clone, PartialEq)]
pub struct RelativeExtra {
    pub first_side_sector: Location,
    pub record_length: u8,
    pub unused: Vec<u8>, // 6 bytes
}

impl RelativeExtra {
    const FIRST_SIDE_SECTOR_OFFSET: usize = 0x00;
    const RECORD_LENGTH_OFFSET: usize = 0x02;
    const UNUSED_OFFSET: usize = 0x03;

    pub fn from_bytes(bytes: &[u8]) -> RelativeExtra {
        assert_eq!(bytes.len(), EXTRA_SIZE);
        RelativeExtra {
            first_side_sector: Location::from_bytes(
                &bytes[Self::FIRST_SIDE_SECTOR_OFFSET..Self::FIRST_SIDE_SECTOR_OFFSET + 2],
            ),
            record_length: bytes[Self::RECORD_LENGTH_OFFSET],
            unused: bytes[Self::UNUSED_OFFSET..EXTRA_SIZE].to_vec(),
        }
    }

    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert_eq!(bytes.len(), EXTRA_SIZE);
        bytes[Self::FIRST_SIDE_SECTOR_OFFSET] = self.first_side_sector.0;
        bytes[Self::FIRST_SIDE_SECTOR_OFFSET + 1] = self.first_side_sector.1;
        (&mut bytes[Self::UNUSED_OFFSET..EXTRA_SIZE]).copy_from_slice(&self.unused);
    }
}

impl fmt::Debug for RelativeExtra {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "REL(side={} rec_len={})",
            self.first_side_sector, self.record_length
        )
    }
}

/// The extra directory entry bytes used in GEOS files.
#[derive(Clone, PartialEq)]
pub struct GEOSExtra {
    pub info_block: Location,
    pub structure: GEOSFileStructure,
    pub geos_file_type: GEOSFileType,
    pub year: u8,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
}

impl GEOSExtra {
    const INFO_BLOCK_OFFSET: usize = 0x00;
    const STRUCTURE_OFFSET: usize = 0x02;
    const GEOS_FILE_TYPE_OFFSET: usize = 0x03;
    const YEAR_OFFSET: usize = 0x04;
    const MONTH_OFFSET: usize = 0x05;
    const DAY_OFFSET: usize = 0x06;
    const HOUR_OFFSET: usize = 0x07;
    const MINUTE_OFFSET: usize = 0x08;

    pub fn from_bytes(bytes: &[u8]) -> GEOSExtra {
        assert_eq!(bytes.len(), EXTRA_SIZE);
        GEOSExtra {
            info_block: Location::from_bytes(
                &bytes[Self::INFO_BLOCK_OFFSET..Self::INFO_BLOCK_OFFSET + 2],
            ),
            structure: GEOSFileStructure::from_byte(bytes[Self::STRUCTURE_OFFSET]),
            geos_file_type: GEOSFileType::from_byte(bytes[Self::GEOS_FILE_TYPE_OFFSET]),
            year: bytes[Self::YEAR_OFFSET],
            month: bytes[Self::MONTH_OFFSET],
            day: bytes[Self::DAY_OFFSET],
            hour: bytes[Self::HOUR_OFFSET],
            minute: bytes[Self::MINUTE_OFFSET],
        }
    }

    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert_eq!(bytes.len(), EXTRA_SIZE);
        self.info_block
            .to_bytes(&mut bytes[Self::INFO_BLOCK_OFFSET..]);
        bytes[Self::STRUCTURE_OFFSET] = self.structure.to_byte();
        bytes[Self::GEOS_FILE_TYPE_OFFSET] = self.geos_file_type.to_byte();
        bytes[Self::YEAR_OFFSET] = self.year;
        bytes[Self::MONTH_OFFSET] = self.month;
        bytes[Self::DAY_OFFSET] = self.day;
        bytes[Self::HOUR_OFFSET] = self.hour;
        bytes[Self::MINUTE_OFFSET] = self.minute;
    }

    #[inline]
    fn is_entry_geos(bytes: &[u8]) -> bool {
        // GEOS file detection works according to the instructions in GEOS.TXT, except
        // for the following errata:
        //     Check the bottom 3 bits of the D64 file type (byte position $02  of  the
        //     directory entry). If it is not 0, 1 or 2  (but  3  or  higher,  REL  and
        //     above), the file cannot be GEOS.
        // It should read "If it is not 0, 1, 2 or 3  (but  4  or  higher,  REL and
        // above), the file cannot be GEOS."
        (bytes[ENTRY_FILE_ATTRIBUTE_OFFSET] & 0x07) < 4
            && (bytes[ENTRY_EXTRA_OFFSET + Self::STRUCTURE_OFFSET] != 0
                || bytes[ENTRY_EXTRA_OFFSET + Self::GEOS_FILE_TYPE_OFFSET] != 0)
            && bytes[ENTRY_EXTRA_OFFSET + Self::STRUCTURE_OFFSET] <= 1
    }

    #[inline]
    fn is_entry_vlir(bytes: &[u8]) -> bool {
        bytes[ENTRY_EXTRA_OFFSET + Self::STRUCTURE_OFFSET] == 1
    }
}

impl fmt::Debug for GEOSExtra {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GEOS({}, {}, {:04}-{:02}-{:02}-{:02}:{:02})",
            self.structure,
            self.geos_file_type,
            1900 + (self.year as usize),
            self.month,
            self.day,
            self.hour,
            self.minute
        )
    }
}

/// A CBM DOS directory entry.
#[derive(Clone)]
pub struct DirectoryEntry {
    pub file_attributes: FileAttributes,
    pub first_sector: Location,
    pub filename: Petscii,
    pub extra: Extra,
    pub file_size: u16,
    // The disk image position where this entry is stored, if available.
    pub position: Option<Position>,
    pub scheme: Scheme,
    // We'll keep non-CBM metadata private for now, so we can more easily refactor later.
    geos_border: bool,
}

impl DirectoryEntry {
    #[allow(unused)]
    fn from_bytes(bytes: &[u8]) -> DirectoryEntry {
        Self::parse(bytes, None)
    }

    fn from_positioned_bytes(bytes: &[u8], position: Position) -> DirectoryEntry {
        Self::parse(bytes, Some(position))
    }

    fn parse(bytes: &[u8], position: Option<Position>) -> DirectoryEntry {
        assert_eq!(bytes.len(), ENTRY_SIZE);

        let file_attributes = FileAttributes::from_byte(bytes[ENTRY_FILE_ATTRIBUTE_OFFSET]);

        // Determine the file storage scheme
        let scheme = if file_attributes.file_type == FileType::REL {
            Scheme::Relative
        } else if GEOSExtra::is_entry_geos(&bytes) {
            if GEOSExtra::is_entry_vlir(&bytes) {
                Scheme::GEOSVLIR
            } else {
                Scheme::GEOSSequential
            }
        } else {
            Scheme::Linear
        };

        // Parse extra metadata
        let extra = Extra::from_bytes(
            scheme,
            &bytes[ENTRY_EXTRA_OFFSET..ENTRY_EXTRA_OFFSET + EXTRA_SIZE],
        );

        DirectoryEntry {
            file_attributes,
            first_sector: Location::from_bytes(&bytes[ENTRY_FIRST_SECTOR_OFFSET..]),
            filename: Petscii::from_padded_bytes(
                &bytes[ENTRY_FILENAME_OFFSET..ENTRY_FILENAME_OFFSET + ENTRY_FILENAME_LENGTH],
                PADDING_BYTE,
            ),
            extra,
            file_size: ((bytes[ENTRY_FILE_SIZE_OFFSET + 1] as u16) << 8)
                | (bytes[ENTRY_FILE_SIZE_OFFSET] as u16),
            position,
            scheme,
            geos_border: false,
        }
    }

    /// Reset all fields to default values, in preparation for a fresh entry.
    pub(super) fn reset(&mut self) {
        self.file_attributes = FileAttributes::from_byte(0);
        self.first_sector = Location::new(0, 0);
        self.filename = Petscii::from_bytes(&[0u8; ENTRY_FILENAME_LENGTH]);
        self.extra = Extra::default();
        self.file_size = 0;
        // The position field must be left untouched.
    }

    /// Read the serialized directory entry from the provided byte slice, and
    /// update our fields. This is useful when reading an updated version
    /// of the same entry.
    fn reread_from_bytes(&mut self, bytes: &[u8]) {
        let mut entry = DirectoryEntry::parse(bytes, self.position);
        ::std::mem::swap(self, &mut entry);
    }

    /// Write the serialized directory entry to the provided mutable byte
    /// slice.  This operation preserves any existing "next directory sector"
    /// field.
    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert_eq!(bytes.len(), ENTRY_SIZE);
        bytes[ENTRY_FILE_ATTRIBUTE_OFFSET] = self.file_attributes.to_byte();
        bytes[ENTRY_FIRST_SECTOR_OFFSET] = self.first_sector.0;
        bytes[ENTRY_FIRST_SECTOR_OFFSET + 1] = self.first_sector.1;
        self.filename
            .write_bytes_with_padding(
                &mut bytes[ENTRY_FILENAME_OFFSET..ENTRY_FILENAME_OFFSET + ENTRY_FILENAME_LENGTH],
                PADDING_BYTE,
            )
            .unwrap();
        self.extra
            .to_bytes(&mut bytes[ENTRY_EXTRA_OFFSET..ENTRY_EXTRA_OFFSET + EXTRA_SIZE]);
        bytes[ENTRY_FILE_SIZE_OFFSET] = (self.file_size & 0xFF) as u8;
        bytes[ENTRY_FILE_SIZE_OFFSET + 1] = (self.file_size >> 8) as u8;
    }

    /// Return true if this entry was found on the GEOS border block
    pub fn is_geos_border(&self) -> bool {
        self.geos_border
    }
}

impl PositionedData for DirectoryEntry {
    fn position(&self) -> io::Result<Position> {
        match self.position {
            Some(p) => Ok(p),
            None => Err(DiskError::Unpositioned.into()),
        }
    }

    fn positioned_read(&mut self, buffer: &[u8]) -> io::Result<()> {
        let position = self.position()?;
        if buffer.len() < position.size as usize {
            return Err(DiskError::ReadUnderrun.into());
        }
        self.reread_from_bytes(buffer);
        Ok(())
    }

    fn positioned_write(&self, buffer: &mut [u8]) -> io::Result<()> {
        let position = self.position()?;
        if buffer.len() < position.size as usize {
            return Err(DiskError::WriteUnderrun.into());
        }
        self.to_bytes(buffer);
        Ok(())
    }
}

impl fmt::Display for DirectoryEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:<4} {:18}{}",
            self.file_size,
            format!("\"{}\"", self.filename),
            self.file_attributes
        )?;
        if f.alternate() {
            // verbose
            write!(f, " {:?}", self.extra)?;
        }
        if self.geos_border {
            write!(f, " (GEOS border)")?;
        }
        Ok(())
    }
}

impl fmt::Debug for DirectoryEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{},{},{:?} @ {:?}",
            format!("\"{}\"", self.filename),
            self.file_size,
            self.file_attributes,
            self.position
        )
    }
}

/// We use a boxed type for this instead of just a ChainIterator, so we can add
/// the GEOS border block to the iteration if needed.
type DirectoryBlockIterator = Box<dyn Iterator<Item = io::Result<ChainSector>>>;

/// This iterator will process the entire directory of a disk image and return
/// a sequence of entries.
pub struct DirectoryIterator {
    block_iter: DirectoryBlockIterator,
    chunks: ::std::vec::IntoIter<Vec<u8>>,
    position: Position,
    error: Option<io::Error>,
    geos_border: Option<Location>,
    processing_geos_border: bool,
}

impl DirectoryIterator {
    /// Create a new directory iterator for the provided disk.
    pub fn new<T: Disk>(disk: &T) -> DirectoryIterator
    where
        T: ?Sized,
    {
        let format = match disk.disk_format() {
            Ok(f) => f,
            Err(e) => return Self::new_error(disk, e),
        };
        let header = match disk.header() {
            Ok(h) => h,
            Err(e) => return Self::new_error(disk, e),
        };
        let location = format.first_directory_location();

        // Prepare to iterate over the chain of directory blocks.
        let chain = ChainIterator::new(disk.blocks(), location);

        // If a GEOS border block is available, add it to the iteration at the end.
        let (block_iter, geos_border): (DirectoryBlockIterator, Option<Location>) =
            match header.geos {
                Some(ref geos_header) => {
                    let border_block_result = disk.blocks_ref().sector_owned(geos_header.border);
                    let tail = iter::once(border_block_result.map(|block| ChainSector {
                        data: block,
                        location: geos_header.border,
                    }));
                    (Box::new(chain.chain(tail)), Some(geos_header.border))
                }
                None => (Box::new(chain), None),
            };

        DirectoryIterator {
            block_iter,
            chunks: vec![].into_iter(), // Arrange to return None the first time.
            position: Position {
                location: location,
                offset: 0,
                size: ENTRY_SIZE as u8,
            },
            error: None,
            geos_border,
            processing_geos_border: false,
        }
    }

    /// We avoid returning an error in new(), so we can preserve the iter()
    /// convention of returning an iterator directly.  Instead, we use this
    /// method to produce an iterator that yields an error on the first
    /// iteration.
    fn new_error<T: Disk>(_disk: &T, error: io::Error) -> DirectoryIterator
    where
        T: ?Sized,
    {
        DirectoryIterator {
            block_iter: Box::new(iter::empty()),
            chunks: vec![].into_iter(),
            position: Position {
                location: Location(0, 0),
                offset: 0,
                size: ENTRY_SIZE as u8,
            },
            error: Some(error),
            geos_border: None,
            processing_geos_border: false,
        }
    }
}

impl Iterator for DirectoryIterator {
    type Item = io::Result<DirectoryEntry>;

    fn next(&mut self) -> Option<io::Result<DirectoryEntry>> {
        // Return any pending error, if present.
        let error = self.error.take();
        if let Some(e) = error {
            return Some(Err(e));
        }

        loop {
            match self.chunks.next() {
                Some(chunk) => {
                    // Use exact_chunks() when it becomes stable, to avoid this check.
                    if chunk.len() != ENTRY_SIZE {
                        continue;
                    }

                    // Track the position of this entry.
                    let entry_position = self.position;
                    // The offset will normally wrap back to 0x00 when processing the last entry in
                    // a sector.
                    self.position.offset = self.position.offset.wrapping_add(ENTRY_SIZE as u8);

                    // Parse
                    let mut entry = DirectoryEntry::from_positioned_bytes(&chunk, entry_position);
                    entry.geos_border = self.processing_geos_border;
                    // Don't include scratched files
                    if entry.file_attributes.is_scratched() {
                        continue;
                    }
                    return Some(Ok(entry));
                }
                None => {
                    match self.block_iter.next() {
                        Some(Ok(block)) => {
                            if let Some(border) = self.geos_border {
                                self.processing_geos_border = border == block.location;
                            }
                            // Convert this block into a vector of entry-size chunk vectors.
                            let mut chunks: Vec<Vec<u8>> = vec![];
                            for chunk in block.data.chunks(ENTRY_SIZE) {
                                chunks.push(chunk.to_vec());
                            }
                            self.chunks = chunks.into_iter();

                            self.position.location = block.location;
                            self.position.offset = 0;
                            // Loop back to the Some(_) case to process the first chunk.
                        }
                        Some(Err(e)) => {
                            return Some(Err(e));
                        }
                        None => {
                            return None;
                        }
                    }
                }
            }
        }
    }
}

/// Return a `DirectoryEntry` representing the next free slot on the directory
/// track.  This entry may be used to create a new directory entry by
/// populating its fields and passing it to `Disk::write_directory_entry()`.
pub(super) fn next_free_directory_entry<T: Disk>(disk: &mut T) -> io::Result<DirectoryEntry>
where
    T: ?Sized,
{
    let first_sector = disk.disk_format()?.first_directory_location();
    let mut last_sector: Location = first_sector;

    // Search the existing directory chain for a free slot.
    {
        let chain = ChainIterator::new(disk.blocks(), first_sector);
        for chain_block in chain {
            let ChainSector { data, location } = chain_block?;
            last_sector = location;
            let mut offset: u8 = 0;
            for chunk in data.chunks(ENTRY_SIZE) {
                let entry = DirectoryEntry::from_positioned_bytes(
                    chunk,
                    Position {
                        location,
                        offset,
                        size: ENTRY_SIZE as u8,
                    },
                );
                if entry.file_attributes.is_scratched() {
                    return Ok(entry);
                }
                // The offset will normally wrap back to 0x00 when processing the last entry in
                // a sector.
                offset = offset.wrapping_add(ENTRY_SIZE as u8);
            }
        }
    }

    // No free slots are available in the currently allocated directory sectors, so
    // we need to create a new one and link to it from the last found sector.
    let bam = disk.bam()?;
    let new_sector;
    {
        let bam = bam.borrow();
        new_sector = bam.next_free_block(Some(last_sector))?;
    }

    // Write the new directory sector
    let new_entry;
    {
        let mut blocks = disk.blocks_ref_mut();
        let block = blocks.sector_mut(new_sector)?;
        // The next location link will be (0x00,0xFF) to indicate that this is the last
        // in the chain, and it is used in its entirety.
        block[0] = 0x00;
        block[1] = 0xFF;
        // All other bytes should be zeroed.
        for offset in 2..BLOCK_SIZE {
            block[offset] = 0;
        }
        new_entry = DirectoryEntry::from_positioned_bytes(
            &block[0..ENTRY_SIZE],
            Position {
                location: new_sector,
                offset: 0,
                size: ENTRY_SIZE as u8,
            },
        );
    }

    // Allocate the new sector in BAM
    {
        let mut bam = bam.borrow_mut();
        bam.allocate(new_sector)?;
    }

    // Link to the new sector from the old sector
    {
        let mut blocks = disk.blocks_ref_mut();
        let block = blocks.sector_mut(last_sector)?;
        block[0] = new_sector.0;
        block[1] = new_sector.1;
    }

    // Return the new entry
    Ok(new_entry)
}

/// Confirm that the specified filename is valid.  A filename is considered
/// valid if it is 16 characters or fewer.
pub(super) fn check_filename_validity(filename: &Petscii) -> io::Result<()> {
    if filename.len() > ENTRY_FILENAME_LENGTH {
        return Err(DiskError::FilenameTooLong.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::d64::D64;
    use crate::disk::DiskFormat;

    fn get_fresh_d64() -> (DiskFormat, D64) {
        let mut d64 = D64::open_memory(D64::geometry(false)).unwrap();
        d64.write_format(&"test".into(), &"t1".into()).unwrap();
        let format = d64.disk_format().unwrap().clone();
        (format, d64)
    }

    #[test]
    fn test_next_free_directory_entry() {
        const MAX_NEW_ENTRIES: usize = 1000;
        const MAX_DIRECTORY_ENTRIES: usize = 144;
        let (_format, mut disk) = get_fresh_d64();
        let mut disk_full: bool = false;
        let mut entries_written: usize = 0;
        for _ in 0..MAX_NEW_ENTRIES {
            let mut entry = match next_free_directory_entry(&mut disk) {
                Ok(entry) => entry,
                Err(ref e) => match DiskError::from_io_error(&e) {
                    Some(ref e) if *e == DiskError::DiskFull => {
                        disk_full = true;
                        break;
                    }
                    Some(ref e) => panic!("error: {}", e),
                    None => break,
                },
            };

            entry.file_attributes.file_type = FileType::PRG;
            entry.file_attributes.closed_flag = true;
            entry.filename = "filename".into();
            disk.write_directory_entry(&entry).unwrap();
            entries_written += 1;
        }
        assert!(disk_full);
        assert_eq!(entries_written, MAX_DIRECTORY_ENTRIES);
    }

    #[test]
    fn test_directory_entry() {
        // All bits cleared
        static BUFFER1: [u8; ENTRY_SIZE] = [0u8; ENTRY_SIZE];
        let entry = DirectoryEntry::from_bytes(&BUFFER1);
        let mut output = [0u8; ENTRY_SIZE];
        entry.to_bytes(&mut output);
        assert_eq!(output, BUFFER1);
        assert_eq!(entry.file_attributes.file_type, FileType::DEL);
        assert_eq!(entry.file_attributes.unused_bit, false);
        assert_eq!(entry.file_attributes.save_with_replace_flag, false);
        assert_eq!(entry.file_attributes.locked_flag, false);
        assert_eq!(entry.file_attributes.closed_flag, false);
        assert_eq!(entry.first_sector, Location(0, 0));
        assert_eq!(
            entry.filename,
            Petscii::from_bytes(&[0; ENTRY_FILENAME_LENGTH])
        );
        assert_eq!(entry.extra, Extra::default());
        assert_eq!(entry.file_size, 0);

        // All padding bytes
        static BUFFER2: [u8; ENTRY_SIZE] = [PADDING_BYTE; ENTRY_SIZE];
        let entry = DirectoryEntry::from_bytes(&BUFFER2);
        let mut output = [0u8; ENTRY_SIZE];
        output[0] = 0xa0; // to_bytes() doesn't touch the first two bytes
        output[1] = 0xa0;
        entry.to_bytes(&mut output);
        assert_eq!(output, BUFFER2);
        assert_eq!(entry.file_attributes.file_type, FileType::DEL);
        assert_eq!(entry.file_attributes.unused_bit, false);
        assert_eq!(entry.file_attributes.save_with_replace_flag, true);
        assert_eq!(entry.file_attributes.locked_flag, false);
        assert_eq!(entry.file_attributes.closed_flag, true);
        assert_eq!(entry.first_sector, Location(PADDING_BYTE, PADDING_BYTE));
        assert_eq!(entry.filename, Petscii::from_bytes(&[0; 0]));
        assert_eq!(
            entry.extra,
            Extra::Linear(LinearExtra::from_bytes(&[PADDING_BYTE; EXTRA_SIZE]))
        );
        assert_eq!(
            entry.file_size,
            ((PADDING_BYTE as u16) << 8) | (PADDING_BYTE as u16)
        );

        // All bits set
        static BUFFER3: [u8; ENTRY_SIZE] = [0xFFu8; ENTRY_SIZE];
        let entry = DirectoryEntry::from_bytes(&BUFFER3);
        let mut output = [0u8; ENTRY_SIZE];
        output[0] = 0xff; // to_bytes() doesn't touch the first two bytes
        output[1] = 0xff;
        entry.to_bytes(&mut output);
        assert_eq!(output, BUFFER3);
        assert_eq!(entry.file_attributes.file_type, FileType::Unknown(0x0F));
        assert_eq!(entry.file_attributes.unused_bit, true);
        assert_eq!(entry.file_attributes.save_with_replace_flag, true);
        assert_eq!(entry.file_attributes.locked_flag, true);
        assert_eq!(entry.file_attributes.closed_flag, true);
        assert_eq!(entry.first_sector, Location(0xFF, 0xFF));
        assert_eq!(
            entry.filename,
            Petscii::from_bytes(&[0xFF; ENTRY_FILENAME_LENGTH])
        );
        assert_eq!(
            entry.extra,
            Extra::Linear(LinearExtra::from_bytes(&[0xFFu8; EXTRA_SIZE]))
        );
        assert_eq!(entry.file_size, 0xFFFF);

        // A real world example.
        // 00016620: 5347 8211 0541 5343 4949 2043 4f44 4553  SG...ASCII CODES
        // 00016630: a0a0 a0a0 a000 0000 0000 0000 0000 0600  ................
        // This is the second directory entry on the first directory sector.
        // Why are the first two (presumably unused) bytes 0x53 0x47?  Who knows.
        static BUFFER4: [u8; ENTRY_SIZE] = [
            0x53, 0x47, 0x82, 0x11, 0x05, 0x41, 0x53, 0x43, 0x49, 0x49, 0x20, 0x43, 0x4f, 0x44,
            0x45, 0x53, 0xa0, 0xa0, 0xa0, 0xa0, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x06, 0x00,
        ];
        let entry = DirectoryEntry::from_bytes(&BUFFER4);
        let mut output = [0u8; ENTRY_SIZE];
        output[0] = BUFFER4[0]; // to_bytes() doesn't touch the first two bytes
        output[1] = BUFFER4[1];
        entry.to_bytes(&mut output);
        assert_eq!(output, BUFFER4);
        assert_eq!(entry.file_attributes.file_type, FileType::PRG);
        assert_eq!(entry.file_attributes.unused_bit, false);
        assert_eq!(entry.file_attributes.save_with_replace_flag, false);
        assert_eq!(entry.file_attributes.locked_flag, false);
        assert_eq!(entry.file_attributes.closed_flag, true);
        assert_eq!(entry.first_sector, Location(0x11, 0x05));
        assert_eq!(entry.filename, Petscii::from_str("ascii codes"));
        assert_eq!(entry.extra, Extra::default());
        assert_eq!(entry.file_size, 0x0006);
    }
}
