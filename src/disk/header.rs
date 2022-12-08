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
    pub disk_format_type_offset: usize,
    pub disk_name_offset: usize,
    pub disk_id_offset: usize,
    pub directory_dos_version_offset: usize,
    pub directory_format_type_offset: usize,
    pub padding_offsets: &'static [u8],
    // DOS version and format type defaults
    // See the notes in the cbm::disk module documentation for full details.
    pub default_disk_format_type: u8,
    pub default_directory_dos_version: u8,
    pub default_directory_format_type: u8,
    // expectations
    pub double_sided_flag_expectation: Option<(usize, u8)>,
}

pub struct Header {
    // http://unusedino.de/ec64/technical/formats/d64.html
    // says to not trust this field.
    pub first_directory_sector: Location,
    pub disk_format_type: u8,
    pub disk_name: Petscii,
    pub disk_id: Id,
    pub directory_dos_version: u8,
    pub directory_format_type: u8,
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
            disk_format_type: header_format.default_disk_format_type,
            disk_name: name.clone(),
            disk_id: id.clone(),
            directory_format_type: header_format.default_directory_format_type,
            directory_dos_version: header_format.default_directory_dos_version,
            geos: None,
        }
    }

    /// Read a header from disk using the provided header format.
    pub fn read(blocks: BlockDeviceRef, format: &HeaderFormat) -> io::Result<Header> {
        let blocks = blocks.borrow();
        let block = blocks.sector(format.location)?;

        // We don't enforce any particular values for these fields when reading or
        // writing disks. See the notes in the cbm::disk module documentation for full
        // details.
        let directory_dos_version = block[format.directory_dos_version_offset];
        let directory_format_type = block[format.directory_format_type_offset];

        // This field is the diskette format type. We don't enforce any particular value
        // for this field. A real CBM DOS would allow the disk to be read normally
        // regardless of the value of this field, but would error if write attempts are
        // made to a disk with a DOS version other than 0x00 or the expected DOS
        // version. See the notes in the cbm::disk module documentation for full
        // details.
        let disk_format_type = block[format.disk_format_type_offset];

        // All 1571 disk images should have the double-side flag set.
        if let Some((offset, value)) = format.double_sided_flag_expectation {
            if block[offset] != value {
                return Err(DiskError::InvalidHeader.into());
            }
        }

        Ok(Header {
            first_directory_sector: Location::from_bytes(&block[format.first_directory_offset..]),
            disk_format_type,
            disk_name: Petscii::from_bytes(
                &block[format.disk_name_offset..format.disk_name_offset + disk::DISK_NAME_SIZE],
            ),
            disk_id: Id::from_bytes(&block[format.disk_id_offset..format.disk_id_offset + 2]),
            directory_dos_version,
            directory_format_type,
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
            block[format.disk_format_type_offset] = self.disk_format_type;
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
            block[format.directory_dos_version_offset] = self.directory_dos_version;
            block[format.directory_format_type_offset] = self.directory_format_type;
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
        writeln!(
            f,
            "directory dos version and format type: {}",
            Petscii::from_bytes(&[self.directory_dos_version, self.directory_format_type]),
        )?;
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
