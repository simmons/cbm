use std::fmt;
use std::io;

use crate::disk::block::{BlockDeviceRef, Location};
pub use crate::disk::error::DiskError;
use crate::disk::format::DiskFormat;
use crate::disk::geos::GEOSDiskHeader;
use crate::disk::{self, Id, PADDING_BYTE};
use crate::petscii::Petscii;

/// A HeaderFormat describes how header information is stored for a particular
/// disk image format.
pub struct HeaderFormat {
    pub location: Location,
    // offsets
    pub first_directory_offset: usize,
    pub disk_dos_version_offset: usize,
    pub disk_name_offset: usize,
    pub disk_id_offset: usize,
    pub dos_type_offset: usize,
    pub padding_offsets: &'static [u8],
    // expectations
    pub expected_dos_version: u8,
    pub expected_dos_type: Id,
    pub double_sided_flag_expectation: Option<(usize, u8)>,
}

pub struct Header {
    // http://unusedino.de/ec64/technical/formats/d64.html
    // says to not trust this field.
    pub first_directory_sector: Location,
    pub disk_dos_version_type: u8,
    pub disk_name: Petscii,
    pub disk_id: Id,
    pub dos_type: Id,
    pub geos: Option<GEOSDiskHeader>,
}

impl Header {
    pub fn new(
        header_format: &HeaderFormat,
        disk_format: &DiskFormat,
        name: &Petscii,
        id: &Id,
    ) -> Header {
        Header {
            first_directory_sector: disk_format.first_directory_location(),
            disk_dos_version_type: header_format.expected_dos_version,
            disk_name: name.clone(),
            disk_id: id.clone(),
            dos_type: header_format.expected_dos_type,
            geos: None,
        }
    }

    /// Read a header from disk using the provided header format.
    pub fn read(blocks: BlockDeviceRef, format: &HeaderFormat) -> io::Result<Header> {
        let blocks = blocks.borrow();
        let block = blocks.sector(format.location)?;

        // This dos_type field is a composite of the "directory DOS version" and the
        // "directory format type" fields. (TODO: These should probably be made distinct
        // fields.) We don't enforce any particular values when reading or writing
        // disks. See the notes in the cbm::disk module documentation for full details.
        let dos_type = Id::from_bytes(&block[format.dos_type_offset..format.dos_type_offset + 2]);

        // We only handle disks with the standard CBM DOS version for its
        // particular format (e.g. 0x41 "A" for 1541, 0x44 "D" for 1581, etc.).
        // (For whatever it's worth, the sample "edit disk name" program on page 78
        // of "Inside Commodore DOS" also performs this check.)
        let disk_dos_version_type = block[format.disk_dos_version_offset];
        if disk_dos_version_type != format.expected_dos_version {
            return Err(DiskError::InvalidHeader.into());
        }

        // All 1571 disk images should have the double-side flag set.
        if let Some((offset, value)) = format.double_sided_flag_expectation {
            if block[offset] != value {
                return Err(DiskError::InvalidHeader.into());
            }
        }

        Ok(Header {
            first_directory_sector: Location::from_bytes(&block[format.first_directory_offset..]),
            disk_dos_version_type,
            disk_name: Petscii::from_bytes(
                &block[format.disk_name_offset..format.disk_name_offset + disk::DISK_NAME_SIZE],
            ),
            disk_id: Id::from_bytes(&block[format.disk_id_offset..format.disk_id_offset + 2]),
            dos_type,
            geos: GEOSDiskHeader::new(block),
        })
    }

    /// Write the header to the provided block buffer.  This function only
    /// writes into the regions corresponding to the fields we know about,
    /// thus allowing the preservation of any non-standard fields if the
    /// caller provides the current header block.
    pub fn write(&mut self, blocks: BlockDeviceRef, format: &HeaderFormat) -> io::Result<()> {
        // Read the header block
        let mut block = blocks.borrow().sector(format.location)?.to_vec();

        // Render our header struct into the block
        {
            self.first_directory_sector
                .to_bytes(&mut block[format.first_directory_offset..]);
            block[format.disk_dos_version_offset] = self.disk_dos_version_type;
            self.disk_name
                .write_bytes_with_padding(
                    &mut block
                        [format.disk_name_offset..format.disk_name_offset + disk::DISK_NAME_SIZE],
                    PADDING_BYTE,
                )
                .map_err(|_| {
                    let e: io::Error = DiskError::FilenameTooLong.into();
                    e
                })?;
            block[format.disk_id_offset] = self.disk_id[0];
            block[format.disk_id_offset + 1] = self.disk_id[1];
            block[format.dos_type_offset] = self.dos_type[0];
            block[format.dos_type_offset + 1] = self.dos_type[1];
        }

        // All 1571 disk images should have the double-side flag set.
        if let Some((offset, value)) = format.double_sided_flag_expectation {
            block[offset] = value;
        }

        // Headers should have certain bytes set to the padding byte (0xA0).  If this
        // is not done, directory listings generated by CBM DOS will be garbled
        // on the "blocks free" line.
        for padding_offset in format.padding_offsets {
            block[*padding_offset as usize] = disk::PADDING_BYTE;
        }

        // Write the header block
        blocks
            .borrow_mut()
            .sector_mut(format.location)?
            .copy_from_slice(&block);
        Ok(())
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "disk name: {:?}", self.disk_name)?;
        writeln!(f, "disk id: {:?}", self.disk_id)?;
        writeln!(f, "dos type: {:?}", self.dos_type)?;
        writeln!(
            f,
            "format: {}",
            match self.geos {
                Some(ref geos) => geos.id.to_escaped_string(),
                None => "CBM".to_string(),
            }
        )
    }
}
