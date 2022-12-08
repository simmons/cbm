//! GEOS, the Graphic Environment Operating System
mod file;
mod reader;

use std::fmt::Write;
use std::fmt::{self, Display};
use std::io;

use crate::disk::block::Location;
use crate::disk::directory::{DirectoryEntry, Extra, FileAttributes};
use crate::disk::DiskError;
use crate::sprite::Sprite;
use crate::util::{Slice, SliceMut};

pub use crate::disk::geos::file::GEOSFile;
pub use crate::disk::geos::reader::GEOSReader;

/// GEOS itself looks for "GEOS format" to determine if a disk is
/// GEOS-formatted, although the full GEOS id string will be something like
/// "GEOS format V1.0".
static GEOS_SIGNATURE: &[u8] = b"GEOS format";
/// The GEOS signature will always be at offset 0xAD of the header block.
const GEOS_SIGNATURE_OFFSET: usize = 0xAD;
/// The offset of the full GEOS id string
const GEOS_ID_OFFSET: usize = 0xAD;
/// The size of the GEOS id string
const GEOS_ID_SIZE: usize = 16;
/// The offset of the border location.  The GEOS "border" is the extra
/// directory sector which exists outside of the normal directory chain to
/// represent files which have been dragged to the border.
const GEOS_BORDER_OFFSET: usize = 0xAB;

/// A GEOS disk header.  If present, it indicates that the disk is considered
/// to be GEOS-formatted.
pub struct GEOSDiskHeader {
    /// The location of the GEOS border sector.
    pub border: Location,
    /// A string starting with "GEOS format " which is the marker indicating
    /// that this is a GEOS disk header.  Typically the full string is
    /// something like "GEOS format V1.0".
    pub id: GEOSString,
}

impl GEOSDiskHeader {
    pub fn new(block: &[u8]) -> Option<GEOSDiskHeader> {
        if &block[GEOS_SIGNATURE_OFFSET..GEOS_SIGNATURE_OFFSET + GEOS_SIGNATURE.len()]
            == GEOS_SIGNATURE
        {
            Some(GEOSDiskHeader {
                border: Location::from_bytes(&block[GEOS_BORDER_OFFSET..GEOS_BORDER_OFFSET + 2]),
                id: GEOSString::from_bytes(&block[GEOS_ID_OFFSET..GEOS_ID_OFFSET + GEOS_ID_SIZE]),
            })
        } else {
            None
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum GEOSFileStructure {
    Sequential,
    VLIR,
    Undefined(u8),
}

impl GEOSFileStructure {
    pub fn from_byte(byte: u8) -> GEOSFileStructure {
        match byte {
            0x00 => GEOSFileStructure::Sequential,
            0x01 => GEOSFileStructure::VLIR,
            b => GEOSFileStructure::Undefined(b),
        }
    }

    pub fn to_byte(&self) -> u8 {
        match self {
            GEOSFileStructure::Sequential => 0x00,
            GEOSFileStructure::VLIR => 0x01,
            GEOSFileStructure::Undefined(b) => *b,
        }
    }
}

impl Display for GEOSFileStructure {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: String;
        f.write_str(match self {
            GEOSFileStructure::Sequential => "Sequential",
            GEOSFileStructure::VLIR => "VLIR",
            GEOSFileStructure::Undefined(b) => {
                s = format!("Unknown(0x{:02x})", b);
                &s
            }
        })
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum GEOSFileType {
    NonGEOS,
    BASIC,
    Assembler,
    DataFile,
    SystemFile,
    DeskAccessory,
    Application,
    ApplicationData,
    FontFile,
    PrinterDriver,
    InputDriver,
    DiskDriver,
    SystemBootFile,
    Temporary,
    AutoExecuteFile,
    Undefined(u8),
}

impl GEOSFileType {
    pub fn from_byte(byte: u8) -> GEOSFileType {
        match byte {
            0x00 => GEOSFileType::NonGEOS,
            0x01 => GEOSFileType::BASIC,
            0x02 => GEOSFileType::Assembler,
            0x03 => GEOSFileType::DataFile,
            0x04 => GEOSFileType::SystemFile,
            0x05 => GEOSFileType::DeskAccessory,
            0x06 => GEOSFileType::Application,
            0x07 => GEOSFileType::ApplicationData,
            0x08 => GEOSFileType::FontFile,
            0x09 => GEOSFileType::PrinterDriver,
            0x0a => GEOSFileType::InputDriver,
            0x0b => GEOSFileType::DiskDriver,
            0x0c => GEOSFileType::SystemBootFile,
            0x0d => GEOSFileType::Temporary,
            0x0e => GEOSFileType::AutoExecuteFile,
            b => GEOSFileType::Undefined(b),
        }
    }

    pub fn to_byte(&self) -> u8 {
        match self {
            GEOSFileType::NonGEOS => 0x00,
            GEOSFileType::BASIC => 0x01,
            GEOSFileType::Assembler => 0x02,
            GEOSFileType::DataFile => 0x03,
            GEOSFileType::SystemFile => 0x04,
            GEOSFileType::DeskAccessory => 0x05,
            GEOSFileType::Application => 0x06,
            GEOSFileType::ApplicationData => 0x07,
            GEOSFileType::FontFile => 0x08,
            GEOSFileType::PrinterDriver => 0x09,
            GEOSFileType::InputDriver => 0x0a,
            GEOSFileType::DiskDriver => 0x0b,
            GEOSFileType::SystemBootFile => 0x0c,
            GEOSFileType::Temporary => 0x0d,
            GEOSFileType::AutoExecuteFile => 0x0e,
            GEOSFileType::Undefined(b) => *b,
        }
    }
}

impl Display for GEOSFileType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: String;
        f.write_str(match self {
            GEOSFileType::NonGEOS => "Non-GEOS",
            GEOSFileType::BASIC => "BASIC",
            GEOSFileType::Assembler => "Assembler",
            GEOSFileType::DataFile => "Data file",
            GEOSFileType::SystemFile => "System File",
            GEOSFileType::DeskAccessory => "Desk Accessory",
            GEOSFileType::Application => "Application",
            GEOSFileType::ApplicationData => "Application Data",
            GEOSFileType::FontFile => "Font File",
            GEOSFileType::PrinterDriver => "Printer Driver",
            GEOSFileType::InputDriver => "Input Driver",
            GEOSFileType::DiskDriver => "Disk Driver",
            GEOSFileType::SystemBootFile => "System Boot File",
            GEOSFileType::Temporary => "Temporary",
            GEOSFileType::AutoExecuteFile => "Auto-Execute File",
            GEOSFileType::Undefined(b) => {
                s = format!("Unknown(0x{:02x})", b);
                &s
            }
        })
    }
}

use std::ascii;

/// GEOS uses regular ASCII to store text, as opposed to PETSCII or Unicode.

pub struct GEOSString {
    data: Vec<u8>,
}

impl GEOSString {
    pub fn from_bytes(data: &[u8]) -> GEOSString {
        GEOSString {
            data: data.to_vec(),
        }
    }

    pub fn to_bytes(&self, bytes: &mut [u8]) {
        bytes.copy_from_slice(&self.data);
    }

    /// GEOS strings are generally null-terminated, but the GEOSString buffer
    /// may be used to store the entire binary contents of a field,
    /// including any garbage bytes beyond the terminator. This method will
    /// return only the bytes up to the null terminator.
    fn get_trimmed_buffer(&self) -> &[u8] {
        match self.data.iter().position(|b| *b == 0u8) {
            Some(n) => &self.data[..n],
            None => &self.data[..],
        }
    }

    pub fn to_escaped_string(&self) -> String {
        let mut s: String = String::new();
        for byte in self.get_trimmed_buffer().iter() {
            for escaped_byte in ascii::escape_default(*byte) {
                s.push(escaped_byte as char);
            }
        }
        s
    }
}

impl Display for GEOSString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.to_escaped_string())
    }
}

#[inline]
fn u16_from_le(bytes: &[u8]) -> u16 {
    ((bytes[1] as u16) << 8) | (bytes[0] as u16)
}

#[inline]
fn u16_to_le(bytes: &mut [u8], n: u16) {
    bytes[0] = (n & 0xFF) as u8;
    bytes[1] = ((n >> 8) & 0xFF) as u8;
}

/// A representation of a GEOS info block.
pub struct GEOSInfo {
    pub icon: Sprite,
    pub file_attributes: FileAttributes, // redundant with dir entry
    pub geos_file_type: GEOSFileType,    // redundant with dir entry
    pub structure: GEOSFileStructure,    // redundant with dir entry
    pub program_load_address: u16,
    pub program_end_address: u16,
    pub program_start_address: u16,
    pub class: GEOSString,
    pub author: GEOSString,
    pub application: GEOSString,
    pub reserved: Vec<u8>,
    pub description: GEOSString,
}

impl GEOSInfo {
    const ICON_OFFSET: usize = 0x05;
    const ICON_SIZE: usize = 63;
    const FILE_ATTRIBUTES_OFFSET: usize = 0x44;
    const GEOS_FILE_TYPE_OFFSET: usize = 0x45;
    const GEOS_STRUCTURE_OFFSET: usize = 0x46;
    const PROGRAM_LOAD_ADDRESS_OFFSET: usize = 0x47;
    const PROGRAM_END_ADDRESS_OFFSET: usize = 0x49;
    const PROGRAM_START_ADDRESS_OFFSET: usize = 0x4B;
    const CLASS_OFFSET: usize = 0x4D;
    const CLASS_SIZE: usize = 20;
    const AUTHOR_OFFSET: usize = 0x61;
    const AUTHOR_SIZE: usize = 20;
    const APPLICATION_OFFSET: usize = 0x75;
    const APPLICATION_SIZE: usize = 20;
    const RESERVED_OFFSET: usize = 0x89;
    const RESERVED_SIZE: usize = 23;
    const DESCRIPTION_OFFSET: usize = 0xA0;
    const DESCRIPTION_SIZE: usize = 96;

    pub fn from_bytes(bytes: &[u8]) -> GEOSInfo {
        GEOSInfo {
            icon: Sprite::from_bytes(bytes.slice(Self::ICON_OFFSET, Self::ICON_SIZE)),
            file_attributes: FileAttributes::from_byte(bytes[Self::FILE_ATTRIBUTES_OFFSET]),
            geos_file_type: GEOSFileType::from_byte(bytes[Self::GEOS_FILE_TYPE_OFFSET]),
            structure: GEOSFileStructure::from_byte(bytes[Self::GEOS_STRUCTURE_OFFSET]),
            program_load_address: u16_from_le(&bytes[Self::PROGRAM_LOAD_ADDRESS_OFFSET..]),
            program_end_address: u16_from_le(&bytes[Self::PROGRAM_END_ADDRESS_OFFSET..]),
            program_start_address: u16_from_le(&bytes[Self::PROGRAM_START_ADDRESS_OFFSET..]),
            class: GEOSString::from_bytes(bytes.slice(Self::CLASS_OFFSET, Self::CLASS_SIZE)),
            author: GEOSString::from_bytes(bytes.slice(Self::AUTHOR_OFFSET, Self::AUTHOR_SIZE)),
            application: GEOSString::from_bytes(
                bytes.slice(Self::APPLICATION_OFFSET, Self::APPLICATION_SIZE),
            ),
            reserved: bytes
                .slice(Self::RESERVED_OFFSET, Self::RESERVED_SIZE)
                .to_vec(),
            description: GEOSString::from_bytes(
                bytes.slice(Self::DESCRIPTION_OFFSET, Self::DESCRIPTION_SIZE),
            ),
        }
    }

    pub fn to_bytes(&self, mut bytes: &mut [u8]) {
        self.icon
            .to_bytes(bytes.slice_mut(Self::ICON_OFFSET, Self::ICON_SIZE));
        bytes[Self::FILE_ATTRIBUTES_OFFSET] = self.file_attributes.to_byte();
        bytes[Self::GEOS_FILE_TYPE_OFFSET] = self.geos_file_type.to_byte();
        bytes[Self::GEOS_STRUCTURE_OFFSET] = self.structure.to_byte();
        u16_to_le(
            &mut bytes[Self::PROGRAM_LOAD_ADDRESS_OFFSET..],
            self.program_load_address,
        );
        u16_to_le(
            &mut bytes[Self::PROGRAM_END_ADDRESS_OFFSET..],
            self.program_end_address,
        );
        u16_to_le(
            &mut bytes[Self::PROGRAM_START_ADDRESS_OFFSET..],
            self.program_start_address,
        );
        self.class
            .to_bytes(bytes.slice_mut(Self::CLASS_OFFSET, Self::CLASS_SIZE));
        self.author
            .to_bytes(bytes.slice_mut(Self::AUTHOR_OFFSET, Self::AUTHOR_SIZE));
        self.application
            .to_bytes(bytes.slice_mut(Self::APPLICATION_OFFSET, Self::APPLICATION_SIZE));
        bytes
            .slice_mut(Self::RESERVED_OFFSET, Self::RESERVED_SIZE)
            .copy_from_slice(&self.reserved);
        self.description
            .to_bytes(bytes.slice_mut(Self::APPLICATION_OFFSET, Self::APPLICATION_SIZE));
    }
}

impl Display for GEOSInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut text = String::new();
        writeln!(text, "Type: {} ({})", self.geos_file_type, self.structure)?;
        writeln!(
            text,
            "Program addresses: load=0x{:04x} end=0x{:04x} start=0x{:04x}",
            self.program_load_address, self.program_end_address, self.program_start_address
        )?;
        writeln!(text, "Class: {}", self.class)?;
        writeln!(text, "Author: {}", self.author)?;
        writeln!(text, "Application: {}", self.application)?;
        writeln!(text, "Description: {}", self.description)?;

        // Combine icon block graphics with the above text.
        let icon = self.icon.to_unicode();
        let icon_iter = icon.lines().chain(::std::iter::repeat(""));
        let text_iter = text.lines().chain(::std::iter::repeat(""));
        for (icon_line, text_line) in icon_iter
            .zip(text_iter)
            .take_while(|(a, b)| !(a.is_empty() && b.is_empty()))
        {
            writeln!(f, "{:12} {}", icon_line, text_line)?;
        }

        Ok(())
    }
}

trait GEOSDirectoryEntry {
    fn is_vlir(&self) -> io::Result<bool>;
    fn info_location(&self) -> io::Result<Option<Location>>;
}
impl GEOSDirectoryEntry for DirectoryEntry {
    fn is_vlir(&self) -> io::Result<bool> {
        let extra = match self.extra {
            Extra::GEOS(ref e) => e,
            _ => unreachable!(),
        };
        match extra.structure {
            GEOSFileStructure::Sequential => Ok(false),
            GEOSFileStructure::VLIR => Ok(true),
            GEOSFileStructure::Undefined(_) => Err(DiskError::UnknownFormat.into()),
        }
    }
    fn info_location(&self) -> io::Result<Option<Location>> {
        let extra = match self.extra {
            Extra::GEOS(ref e) => e,
            _ => unreachable!(),
        };
        if extra.info_block.0 == 0 && extra.info_block.1 == 0 {
            Ok(None)
        } else {
            Ok(Some(extra.info_block))
        }
    }
}
