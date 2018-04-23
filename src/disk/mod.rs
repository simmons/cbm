//! Traits, structs, and functions relating to disk images.

mod bam;
mod block;
mod chain;
mod d64;
mod d71;
mod d81;
mod error;
mod format;
mod header;
mod image;
mod validation;

pub mod directory;
pub mod file;
pub mod geos;

use std::cell::RefCell;
use std::fmt;
use std::io::{self, Write};
use std::ops::{Index, IndexMut};
use std::path::Path;
use std::rc::Rc;

use disk::bam::{BAMEntry, BAMRef, BAM};
use disk::block::{BlockDevice, BlockDeviceRef, Location, BLOCK_SIZE};
use disk::directory::{DirectoryEntry, DirectoryIterator, FileType};
use disk::file::{File, LinearFile, Scheme};
use disk::format::{DiskFormat, Track};
use disk::header::Header;
use disk::image::Image;
use petscii::Petscii;

pub use self::d64::D64;
pub use self::d71::D71;
pub use self::d81::D81;
pub use self::error::DiskError;
pub use self::validation::ValidationError;

const PADDING_BYTE: u8 = 0xa0; // For padding filenames, disk name, etc.
const DISK_NAME_SIZE: usize = 16;

/// Disk image types.
#[derive(Clone, Copy, Debug)]
pub enum DiskType {
    /// A 1541 disk image in "D64" format.
    D64,
    /// A 1571 disk image in "D71" format.
    D71,
    /// A 1581 disk image in "D81" format.
    D81,
}

impl DiskType {
    pub fn from_extension<P: AsRef<Path>>(path: P) -> Option<DiskType> {
        const D64_EXTENSION: &str = "d64";
        const D71_EXTENSION: &str = "d71";
        const D81_EXTENSION: &str = "d81";

        let extension = path
            .as_ref()
            .extension()
            .and_then(|s| s.to_str())
            .and_then(|s| Some(s.to_lowercase()));

        if let Some(extension) = extension {
            match &extension.to_lowercase()[..] {
                D64_EXTENSION => Some(DiskType::D64),
                D71_EXTENSION => Some(DiskType::D71),
                D81_EXTENSION => Some(DiskType::D81),
                _ => None,
            }
        } else {
            None
        }
    }
}

/// Open a disk image, regardless of whether it is a D64 (1541), D71 (1571), or
/// D81 (1581) image.
pub fn open<P: AsRef<Path>>(path: P, writable: bool) -> io::Result<Box<Disk>> {
    // Try to determine the disk type based on its filename extension.
    match DiskType::from_extension(&path) {
        Some(DiskType::D64) => return Ok(Box::new(d64::D64::open(path, writable)?)),
        Some(DiskType::D71) => return Ok(Box::new(d71::D71::open(path, writable)?)),
        Some(DiskType::D81) => return Ok(Box::new(d81::D81::open(path, writable)?)),
        None => {}
    }

    // If no extension matches, then try each disk implementation until one works.
    // This will end up selecting the implementation based on the file size.
    #[inline]
    fn is_layout_error(error: &io::Error) -> bool {
        DiskError::from_io_error(error) == Some(DiskError::InvalidLayout)
    }
    let path = path.as_ref();
    match d64::D64::open(path, writable) {
        Ok(d64) => return Ok(Box::new(d64)),
        Err(ref e) if is_layout_error(e) => match d71::D71::open(path, writable) {
            Ok(d71) => return Ok(Box::new(d71)),
            Err(ref e) if is_layout_error(e) => {
                return Ok(Box::new(d81::D81::open(path, writable)?));
            }
            Err(e) => return Err(e),
        },
        Err(e) => return Err(e),
    }
}

/// This is a helper method that Disk implementations can use to set BAM.
fn set_bam(bam_ref: &mut Option<BAMRef>, new_bam: Option<BAM>) {
    // We must be careful to replace the contents of the existing RefCell
    // (if any) so that any holders of the Rc<RefCell<BAM>> will use the
    // new BAM and not try to use the old BAM, which could cause
    // corruption.
    match new_bam {
        Some(new_bam) => match bam_ref {
            Some(ref cell) => {
                cell.replace(new_bam);
            }
            None => *bam_ref = Some(Rc::new(RefCell::new(new_bam))),
        },
        None => {
            // Setting the BAM to None essentially regards the disk as unformatted.
            // Invalidate any pre-existing BAM object so it can't be used again.
            if let Some(cell) = bam_ref.take() {
                cell.borrow_mut().invalidate();
            }
        }
    };
}

/// All disk image types implement the `Disk` trait, and most disk operations
/// can be performed polymorphically using `Disk` as a trait object.
pub trait Disk {
    fn native_disk_format(&self) -> &'static DiskFormat;
    fn disk_format(&self) -> io::Result<&DiskFormat>;
    fn disk_format_mut(&mut self) -> io::Result<&mut DiskFormat>;
    fn set_disk_format(&mut self, disk_format: Option<DiskFormat>);
    fn blocks(&self) -> BlockDeviceRef;
    fn blocks_ref(&self) -> ::std::cell::Ref<BlockDevice>;
    fn blocks_ref_mut(&self) -> ::std::cell::RefMut<'_, BlockDevice>;
    fn header(&self) -> io::Result<&Header>;
    fn header_mut(&mut self) -> io::Result<&mut Header>;
    fn set_header(&mut self, header: Option<Header>);
    fn flush_header(&mut self) -> io::Result<()>;
    fn bam(&self) -> io::Result<BAMRef>;
    fn set_bam(&mut self, bam: Option<BAM>);

    /// Initialize the disk by reading the format metadata, if any.  This may
    /// be called again, for example, when the BAM or header have been
    /// re-written.
    fn initialize(&mut self) {
        let disk_format = self.native_disk_format();
        let mut header = Header::read(self.blocks().clone(), disk_format.header).ok();
        let mut bam = match header {
            Some(_) => BAM::read(self.blocks(), disk_format).ok(),
            None => {
                // It doesn't make sense to have an unreadable BAM with a valid header --
                // consider the disk image to be unformatted.
                header = None;
                None
            }
        };

        // Each disk image type supplies a static Format describing the format used
        // natively by the drive's CBM DOS.  However, upon initialization, we
        // copy this static format to support any per-image peculiarities.
        // Currently, the only non-native format supported is GEOS.
        let format = match header {
            Some(ref header) => {
                let mut format = self.native_disk_format().clone();
                // Is this a GEOS-formatted disk?
                if header.geos.is_some() {
                    format.geos = true;
                }
                Some(format)
            }
            None => None,
        };

        // Install the (potentially) updated format into the BAM.
        if let Some(ref format) = format {
            if let Some(ref mut bam) = bam {
                bam.set_format(format);
            }
        }

        self.set_header(header);
        self.set_bam(bam);
        self.set_disk_format(format);
    }

    /// Format the disk image.  Currently, this does not accurately reflect the
    /// exact formatting method used by the 1541/1571/1581 CBM DOS.
    fn write_format(&mut self, name: &Petscii, id: &Id) -> io::Result<()> {
        let disk_format = self.native_disk_format();

        // Zero all sectors
        let track_count = self.blocks_ref().geometry().tracks;
        for track in disk_format.first_track..=track_count {
            for sector in 0..disk_format.tracks[track as usize].sectors {
                let location = Location(track, sector);
                let mut blocks = self.blocks_ref_mut();
                let block = blocks.sector_mut(location)?;
                for offset in 0..BLOCK_SIZE {
                    block[offset] = 0;
                }
            }
        }

        // Write the initial directory sector
        {
            let mut blocks = self.blocks_ref_mut();
            let block = blocks.sector_mut(disk_format.first_directory_location())?;
            // The next location link will be (0x00,0xFF) to indicate that this is the last
            // in the chain, and it is used in its entirety.
            block[0] = 0x00;
            block[1] = 0xFF;
        }

        // Write a fresh header
        {
            let mut header = Header::new(disk_format.header, disk_format, &name, &id);
            header.write(self.blocks(), disk_format.header)?;
            self.set_header(Some(header));
        }

        // Write a fresh BAM
        {
            let mut bam = BAM::new(self.blocks(), disk_format);

            // Perform the initial allocations for this format.
            for location in disk_format.system_locations() {
                bam.allocate(location)?;
            }

            // Write the new BAM.
            bam.flush()?;
            self.set_bam(Some(bam));
        }

        self.blocks_ref_mut().flush()?;
        self.initialize();
        Ok(())
    }

    /// Return an iterator of directory entries found on this disk image.
    fn iter(&self) -> DirectoryIterator {
        DirectoryIterator::new(self)
    }

    /// Return a list of all directory entries
    fn directory(&self) -> io::Result<Vec<DirectoryEntry>> {
        self.iter().collect::<io::Result<Vec<_>>>()
    }

    /// Locate a directory entry based on its filename.
    fn find_directory_entry(&self, filename: &Petscii) -> io::Result<DirectoryEntry> {
        self.iter()
            .find(|x| match x {
                &Err(_) => true,
                &Ok(ref entry) => entry.filename == *filename,
            })
            .unwrap_or(Err(DiskError::NotFound.into()))
    }

    /// Return a `DirectoryEntry` representing the next free slot on the
    /// directory track.  This entry may be used to create a new directory
    /// entry by populating its fields and passing it to
    /// `write_directory_entry()`.
    fn next_free_directory_entry(&mut self) -> io::Result<DirectoryEntry> {
        directory::next_free_directory_entry(self)
    }

    /// Write the the provide directory entry to disk, using the same slot as
    /// it was originally read from.
    fn write_directory_entry(&mut self, entry: &DirectoryEntry) -> io::Result<()> {
        let mut blocks = self.blocks_ref_mut();
        blocks.positioned_write(entry)?;
        Ok(())
    }

    /// Confirm that no directory entry currently exists with the provided
    /// filename.
    fn check_filename_availability(&self, filename: &Petscii) -> io::Result<()> {
        // Check that the new filename doesn't already exist.
        match self.find_directory_entry(&filename) {
            Ok(_) => Err(DiskError::FileExists.into()),
            Err(e) => match DiskError::from_io_error(&e) {
                Some(DiskError::NotFound) => Ok(()),
                Some(disk_error) => Err(disk_error.into()),
                _ => Err(e.into()),
            },
        }
    }

    /// Rename a file.
    fn rename(&mut self, original_filename: &Petscii, new_filename: &Petscii) -> io::Result<()> {
        // Lookup the file to rename
        let mut entry = self.find_directory_entry(original_filename)?;

        // Validate new filename
        directory::check_filename_validity(new_filename)?;

        // Check that the new filename doesn't already exist.
        self.check_filename_availability(new_filename)?;

        // Rename
        entry.filename = new_filename.clone();
        self.write_directory_entry(&entry)?;
        Ok(())
    }

    /// Open a file based on its filename.
    fn open_file(&self, filename: &Petscii) -> io::Result<File> {
        File::open(self, filename)
    }

    /// Open a file based on its directory entry.
    fn open_file_from_entry(&self, entry: &DirectoryEntry) -> io::Result<File> {
        File::open_from_entry(self, entry)
    }

    /// Create a new file on the disk.  It will be initialized as a closed
    /// zero-length file, and a file handle will be returned which may be
    /// used to provision a writer.
    fn create_file(
        &mut self,
        filename: &Petscii,
        file_type: FileType,
        scheme: Scheme,
    ) -> io::Result<File> {
        // Only Scheme::Linear files allow writing at present.
        assert_eq!(scheme, Scheme::Linear);

        // Assure the filename is valid and available.
        directory::check_filename_validity(filename)?;
        self.check_filename_availability(filename)?;

        // Find the next available directory slot
        let mut entry = self.next_free_directory_entry()?;

        // Find and allocate the first sector for this new file
        let bam = self.bam()?;
        let mut bam = bam.borrow_mut();
        let first_sector = bam.next_free_block(None)?;
        bam.allocate(first_sector)?;

        // Initialize the first sector with link bytes [0x00,0x01] to indicate that it
        // is a tail block of zero length.
        let mut blocks = self.blocks_ref_mut();
        {
            let mut block = blocks.sector_mut(first_sector)?;
            chain::CHAIN_LINK_ZERO.to_bytes(&mut block);
        }

        // Populate and write the directory entry for this file.
        entry.reset();
        entry.file_attributes.file_type = file_type;
        // Allow the created file to be a closed zero-length file until such a time as
        // the caller opens a Write.  Then, we'll mark the file as open (e.g. a
        // "splat file") for the duration of the writing.
        entry.file_attributes.closed_flag = true;
        entry.first_sector = first_sector;
        entry.filename = filename.clone();
        entry.file_size = 1; // One block is allocated for the first sector.

        // Write the directory entry.
        match blocks.positioned_write(&entry) {
            Ok(_) => {}
            Err(e) => {
                // If the directory entry could not be written, roll back the BAM allocation
                // before returning an error.
                bam.free(first_sector)?;
                return Err(e);
            }
        }

        // Return the file handle
        Ok(File::Linear(LinearFile::new(
            self.blocks(),
            self.bam()?,
            entry,
        )))
    }

    /// Read a specific block from the disk, given its track and sector
    /// location.
    fn read_sector<'a>(&self, location: Location) -> io::Result<Vec<u8>> {
        Ok(self.blocks_ref().sector_owned(location)?)
    }

    /// Write a block of data to a specific location on the disk.
    fn write_sector(&mut self, location: Location, data: &[u8]) -> io::Result<()> {
        if data.len() != BLOCK_SIZE {
            return Err(DiskError::WriteUnderrun.into());
        }
        let mut blocks = self.blocks_ref_mut();
        let sector = blocks.sector_mut(location)?;
        sector.copy_from_slice(data);
        Ok(())
    }

    /// Write a hex dump of the disk image to the provided writer.
    fn dump(&mut self, writer: &mut Write) -> io::Result<()> {
        self.blocks_ref().dump(writer)
    }

    /// Return the name of this disk as found in the disk header.
    fn name<'a>(&'a self) -> Option<&'a Petscii> {
        match &self.header() {
            &Ok(ref header) => Some(&header.disk_name),
            &Err(_) => None,
        }
    }

    /// Check the consistency of the disk image.  Unlike the "validate" ("v0:")
    /// command in CBM DOS, this is a read-only operation and does not
    /// attempt any repairs.  A list of validation errors is returned.
    #[inline]
    fn validate(&self) -> io::Result<Vec<ValidationError>> {
        self::validation::validate(self)
    }

    /// Return the blocks free based on the BAM free sector counts.
    /// (Not based on the BAM bitmaps, nor on the file sizes in the directory
    /// entries.)
    fn blocks_free(&self) -> io::Result<usize> {
        let bam_ref = self.bam()?;
        let bam = bam_ref.borrow();
        Ok(bam.blocks_free())
    }
}

impl fmt::Display for Disk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.header() {
            &Ok(ref header) => {
                write!(
                    f,
                    "{} \"{:16}\" {} {}",
                    0, header.disk_name, header.disk_id, header.dos_type
                )?;
                if let Some(ref geos_header) = header.geos {
                    write!(f, " ({})", geos_header.id.to_escaped_string())?;
                }
                Ok(())
            }
            &Err(ref e) => write!(f, "Cannot read header: {}", e),
        }
    }
}

impl fmt::Debug for Disk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Read a fresh header so we can return a useful error condition should
        // it not be readable.
        let blocks = self.blocks();
        let header_result = Header::read(blocks.clone(), self.native_disk_format().header);
        writeln!(f, "{:?}", header_result)?;
        writeln!(f, "error table: {:?}", blocks.borrow().error_table())?;
        Ok(())
    }
}

impl<'a> IntoIterator for &'a Disk {
    type Item = io::Result<DirectoryEntry>;
    type IntoIter = DirectoryIterator;

    fn into_iter(self) -> Self::IntoIter {
        DirectoryIterator::new(self)
    }
}

/// A `Geometry` specifies the track and sector layout of a disk image, and
/// also whether it has an error table appended or not.
#[derive(Copy, Clone)]
pub struct Geometry {
    track_layouts: &'static [Track],
    tracks: u8,
    with_error_table: bool,
}

impl Geometry {
    /// Given a disk image file size, return the first matching geometry.
    pub fn find_by_size<'a>(
        size: usize,
        geometries: &'a [&'static Geometry],
    ) -> Option<&'a Geometry> {
        for geometry in geometries.iter() {
            if geometry.size() == size {
                return Some(geometry);
            }
        }
        None
    }

    /// Return the size of this geometry, if it didn't have an error table
    /// attached.
    #[inline]
    fn size_without_error_table(&self) -> usize {
        let tracks = self.tracks as usize;
        self.track_layouts[tracks].byte_offset as usize
            + self.track_layouts[tracks].sectors as usize * BLOCK_SIZE
    }

    /// Return the size of the error table for this geometry, if one existed.
    #[inline]
    fn error_table_size(&self) -> usize {
        // The error table is one byte for every sector in this image.
        self.track_layouts
            .iter()
            .take((self.tracks as usize) + 1)
            .map(|t| t.sectors as usize)
            .sum::<usize>()
    }

    /// Return the total number of bytes used to represent a disk image in this
    /// geometry.
    pub fn size(&self) -> usize {
        if self.with_error_table {
            self.size_without_error_table() + self.error_table_size()
        } else {
            self.size_without_error_table()
        }
    }

    /// Return the offset of the error table, if one is present in this
    /// geometry.
    pub fn error_table_offset(&self) -> Option<usize> {
        if self.with_error_table {
            Some(self.size_without_error_table())
        } else {
            None
        }
    }
}

/// Various fields in CBM DOS are two-byte identifiers which are frequently
/// shown as Petscii strings.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Id([u8; 2]);

impl Id {
    pub fn from_bytes(bytes: &[u8]) -> Id {
        bytes.into()
    }
}

impl Into<Petscii> for Id {
    fn into(self) -> Petscii {
        Petscii::from_bytes(&self.0)
    }
}

impl Into<String> for Id {
    fn into(self) -> String {
        let p: Petscii = self.into();
        p.into()
    }
}

impl AsRef<[u8]> for Id {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<Id> for Id {
    fn as_ref(&self) -> &Id {
        &self
    }
}

impl<'a> From<&'a [u8]> for Id {
    fn from(bytes: &[u8]) -> Id {
        // Best-effort only.  Use the first two bytes for the Id, using zeros
        // for any byte not present.
        Id([
            if bytes.len() > 0 { bytes[0] } else { 0 },
            if bytes.len() > 1 { bytes[1] } else { 0 },
        ])
    }
}

impl From<Petscii> for Id {
    fn from(petscii: Petscii) -> Id {
        petscii.as_bytes().into()
    }
}

impl From<String> for Id {
    fn from(string: String) -> Id {
        let petscii: Petscii = string.into();
        petscii.into()
    }
}

impl<'a> From<&'a String> for Id {
    fn from(string: &String) -> Id {
        let petscii: Petscii = string.into();
        petscii.into()
    }
}

impl<'a> From<&'a str> for Id {
    fn from(string: &str) -> Id {
        let petscii: Petscii = string.into();
        petscii.into()
    }
}

impl Index<usize> for Id {
    type Output = u8;
    fn index(&self, i: usize) -> &u8 {
        &self.0[i]
    }
}

impl IndexMut<usize> for Id {
    fn index_mut(&mut self, i: usize) -> &mut u8 {
        &mut self.0[i]
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Petscii::from_bytes(&self.0))
    }
}
