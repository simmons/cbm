//! CBM DOS files

use std::io::{self, Read, Write};

use disk::bam::BAMRef;
use disk::block::BlockDeviceRef;
use disk::block::Location;
use disk::block::Position;
use disk::block::BLOCK_SIZE;
use disk::chain::{self, ChainIterator, ChainLink, ChainReader, ChainWriter};
use disk::directory::{DirectoryEntry, Extra, FileType};
use disk::geos::{GEOSFile, GEOSInfo};
use disk::{Disk, DiskError};
use petscii::Petscii;
use util;

/// A scheme represents a particular file layout scheme.  This is different
/// from `FileType` which represents the CBM file type (PRG, SEQ, REL, or USR).
/// `Scheme` is used to differentiate between files that require different
/// access methods due to their structure.  Most conventional files can be
/// accessed in a linear fashion (e.g. read or written from beginning to end
/// via a `Read` or `Write` trait), while other files (e.g. REL and GEOS files)
/// are non-linear and require special approaches to consume.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Scheme {
    /// A regular, linear file that can be read from the beginning to the end.
    /// Most CBM PRG, SEQ, and USR files fall into this category.
    Linear,
    /// A CBM relative (REL) file.  In addition to the linear byte stream, this
    /// file type stores a record size value in the directory entry, and
    /// index information in a number of "side sectors" to allow random
    /// access of specific records.
    Relative,
    /// A GEOS sequential file.  In addition to the linear byte stream, this
    /// scheme stores additional GEOS-specific metadata in an "info block".
    /// Even though GEOS sequential files are mostly linear, we still
    /// categorize them as non-linear due to the info block.
    GEOSSequential,
    /// A GEOS Variable Length Index Record (VLIR) file.  In addition to a GEOS
    /// info block, this scheme stores multiple variable-length records.
    /// The starting sectors of each record are stored in a VLIR record
    /// block.
    GEOSVLIR,
}

/// A File represents a file that has been opened from a CBM disk image.
///
/// In the 1980's, non-linear file structures were more common, and such files
/// can't directly map to our sdfsdfsdf modern notion of files as a single
/// sequence of bytes.  This makes it impractical to implement simple Read and
/// Write traits.  Our approach is to allow the caller to open a `File`, which
/// itself is an enum containing more specialized types which map to various
/// file layout schemes.
pub enum File {
    /// The file is a regular, linear file that can be fully read from the
    /// beginning to the end. Most CBM PRG, SEQ, and USR files fall into
    /// this category.  The contents may be accessed using the contained
    /// `LinearFile`.
    Linear(LinearFile),
    /// The file is a CBM relative (REL) file.  In addition to the linear byte
    /// stream, this file type stores a record size value in the directory
    /// entry, and index information in a number of "side sectors" to allow
    /// random access of specific records.  The contents may be accessed
    /// using the contained `RelativeFile`.
    /// `RelativeFile` object.
    Relative(RelativeFile),
    /// The file is a GEOS sequential file.  In addition to the linear byte
    /// stream, this scheme stores additional GEOS-specific metadata in an
    /// "info block".  Even though GEOS sequential files are mostly linear,
    /// we still categorize them as non-linear due to the info block.  The
    /// contents may be accessed using the contained `GEOSFile` object.
    GEOSSequential(GEOSFile),
    /// A GEOS Variable Length Index Record (VLIR) file.  In addition to a GEOS
    /// info block, this scheme stores multiple variable-length records.
    /// The starting sectors of each record are stored in a VLIR record
    /// block.  The contents may be accessed using the contained `GEOSFile`
    /// object.
    GEOSVLIR(GEOSFile),
    Unknown,
    __Nonexhaustive,
}

/// This trait defines methods that are common across all file schemes.
pub trait FileOps {
    /// Return a reference to the directory entry from which this file was
    /// opened.
    fn entry(&self) -> &DirectoryEntry;
    /// Delete the file represented by this object.
    fn delete(&mut self) -> io::Result<()>;
    /// Return the number of records that this file offers.  This will always
    /// be 1 for `Linear` or `GEOSSequential`.)
    fn record_count(&self) -> io::Result<usize>;
    /// Read an entire record into memory and return it.
    fn record(&self, index: usize) -> io::Result<Vec<u8>>;
    /// Write debug-level information about the file to the provided writer.
    fn details(&self, writer: &mut Write, verbosity: usize) -> io::Result<()>;
    /// Return a list of sectors occupied by this file.
    fn occupied_sectors(&self) -> io::Result<Vec<Location>>;
    /// Return a reader for the contents of this file.  If this file is
    /// non-linear, return a NonLinearFile error.
    fn reader(&self) -> io::Result<Box<Read>>;
    /// Return a writer for this file.  If this file is non-linear, return a
    /// NonLinearFile error.
    fn writer(&self) -> io::Result<Box<Write>>;

    /// Return the filename.
    fn name(&self) -> Petscii {
        self.entry().filename.clone()
    }

    /// Hex-dump the file contents to the provided writer.
    fn dump(&self, writer: &mut Write) -> io::Result<()> {
        writeln!(writer, "Filename: \"{}\"", self.name())?;
        for i in 0..self.record_count()? {
            writeln!(writer, "Record: {}", i)?;
            writeln!(writer, "{}", util::hex(&self.record(i)?))?;
        }
        Ok(())
    }
}

impl File {
    pub(super) fn open<T: Disk>(disk: &T, filename: &Petscii) -> io::Result<File>
    where
        T: ?Sized,
    {
        let entry = disk.find_directory_entry(filename)?;
        Self::open_from_entry(disk, &entry)
    }

    pub(super) fn open_from_entry<T: Disk>(disk: &T, entry: &DirectoryEntry) -> io::Result<File>
    where
        T: ?Sized,
    {
        let entry = entry.clone();
        let file = match entry.scheme {
            Scheme::Linear => File::Linear(LinearFile::new(disk.blocks(), disk.bam()?, entry)),
            Scheme::Relative => {
                File::Relative(RelativeFile::new(disk.blocks(), disk.bam()?, entry)?)
            }
            Scheme::GEOSSequential => {
                File::GEOSSequential(GEOSFile::new(disk.blocks(), disk.bam()?, entry)?)
            }
            Scheme::GEOSVLIR => File::GEOSVLIR(GEOSFile::new(disk.blocks(), disk.bam()?, entry)?),
        };
        Ok(file)
    }

    /// Return the GEOS info block, or None if no info block is present or this
    /// is not a GEOS file.
    pub fn geos_info(&self) -> io::Result<Option<GEOSInfo>> {
        let geos_file = match self {
            File::Linear(_) => return Ok(None),
            File::Relative(_) => return Ok(None),
            File::GEOSSequential(ref f) => f,
            File::GEOSVLIR(ref f) => f,
            _ => unimplemented!(),
        };
        geos_file.info()
    }

    /// Return a reference to the underlying specialized file.
    fn get_specialized_file(&self) -> &FileOps {
        match self {
            File::Linear(ref f) => f,
            File::Relative(ref f) => f,
            File::GEOSSequential(ref f) => f,
            File::GEOSVLIR(ref f) => f,
            _ => unimplemented!(),
        }
    }

    /// Return a mutable reference to the underlying specialized file.
    fn get_specialized_file_mut(&mut self) -> &mut FileOps {
        match self {
            File::Linear(ref mut f) => f,
            File::Relative(ref mut f) => f,
            File::GEOSSequential(ref mut f) => f,
            File::GEOSVLIR(ref mut f) => f,
            _ => unimplemented!(),
        }
    }
}

impl FileOps for File {
    fn entry(&self) -> &DirectoryEntry {
        self.get_specialized_file().entry()
    }

    fn delete(&mut self) -> io::Result<()> {
        self.get_specialized_file_mut().delete()
    }

    fn record_count(&self) -> io::Result<usize> {
        self.get_specialized_file().record_count()
    }

    fn record(&self, index: usize) -> io::Result<Vec<u8>> {
        self.get_specialized_file().record(index)
    }

    fn details(&self, writer: &mut Write, verbosity: usize) -> io::Result<()> {
        self.get_specialized_file().details(writer, verbosity)
    }

    fn occupied_sectors(&self) -> io::Result<Vec<Location>> {
        self.get_specialized_file().occupied_sectors()
    }

    fn reader(&self) -> io::Result<Box<Read>> {
        self.get_specialized_file().reader()
    }

    fn writer(&self) -> io::Result<Box<Write>> {
        self.get_specialized_file().writer()
    }
}

/// This is a specialized file type for conventional linear files that can be
/// fully read from the beginning to the end. Most CBM PRG, SEQ, and USR files
/// fall into this category.
pub struct LinearFile {
    blocks: BlockDeviceRef,
    bam: BAMRef,
    entry: DirectoryEntry,
}

impl LinearFile {
    pub fn new(blocks: BlockDeviceRef, bam: BAMRef, entry: DirectoryEntry) -> LinearFile {
        LinearFile { blocks, bam, entry }
    }
}

impl FileOps for LinearFile {
    fn entry(&self) -> &DirectoryEntry {
        &self.entry
    }

    fn delete(&mut self) -> io::Result<()> {
        // Deallocate the chain.
        chain::remove_chain(
            self.blocks.clone(),
            self.bam.clone(),
            self.entry.first_sector,
        )?;
        // Mark the file as "DEL" and "open" (*DEL) in the directory.
        self.entry.file_attributes.file_type = FileType::DEL;
        self.entry.file_attributes.closed_flag = false;
        // Write the new directory entry
        let mut blocks = self.blocks.borrow_mut();
        blocks.positioned_write(&self.entry)?;
        Ok(())
    }

    fn record_count(&self) -> io::Result<usize> {
        // A linear file always has one record.
        Ok(1)
    }

    fn record(&self, index: usize) -> io::Result<Vec<u8>> {
        if index == 0 {
            let mut record = vec![];
            self.reader()?.read_to_end(&mut record)?;
            Ok(record)
        } else {
            Err(DiskError::InvalidRecord.into())
        }
    }

    fn details(&self, writer: &mut Write, verbosity: usize) -> io::Result<()> {
        if verbosity > 0 {
            if let Some(position) = self.entry().position {
                writeln!(writer, "Directory position: {}", position)?;
            }
            let locations =
                ChainIterator::new(self.blocks.clone(), self.entry.first_sector).locations()?;
            writeln!(
                writer,
                "Occupied sectors: {}",
                Location::format_locations(&locations)
            )?;
        }
        Ok(())
    }

    fn occupied_sectors(&self) -> io::Result<Vec<Location>> {
        let mut locations = ChainIterator::new(self.blocks.clone(), self.entry.first_sector)
            .map(|r| r.map(|cs| cs.location))
            .collect::<io::Result<Vec<_>>>()?;
        locations.sort();
        locations.dedup();
        Ok(locations)
    }

    fn reader(&self) -> io::Result<Box<Read>> {
        Ok(Box::new(ChainReader::new(
            self.blocks.clone(),
            self.entry.first_sector,
        )))
    }

    fn writer(&self) -> io::Result<Box<Write>> {
        match ChainWriter::new(
            self.blocks.clone(),
            self.bam.clone(),
            self.entry.clone(),
            self.entry.first_sector,
        ) {
            Ok(w) => Ok(Box::new(w)),
            Err(e) => Err(e),
        }
    }
}

/// Relative ("REL") files are a special feature of the CBM filesystem where an
/// otherwise sequential data stream is split into fixed-size records, and
/// these records can be randomly accessed by way of an index stored in
/// so-called "side sectors". Relative file support is read-only for now.
pub struct RelativeFile {
    blocks: BlockDeviceRef,
    bam: BAMRef,
    entry: DirectoryEntry,
    record_size: usize,
    data_sectors: Vec<Location>,
    side_sectors: Vec<Location>,
    records: usize,
}

const MAX_SIDE_SECTORS: usize = 6;
const SIDE_SECTOR_SSLIST_OFFSET: usize = 0x04;
const SIDE_SECTOR_DATA_OFFSET: usize = 0x10;

impl RelativeFile {
    pub fn new(
        blocks: BlockDeviceRef,
        bam: BAMRef,
        entry: DirectoryEntry,
    ) -> io::Result<RelativeFile> {
        // Extract directory entry metadata
        let (record_size, first_side_sector) = match entry.extra {
            Extra::Relative(ref e) => (e.record_length as usize, e.first_side_sector),
            _ => return Err(DiskError::InvalidRelativeFile.into()),
        };

        // Find side sectors.  Side sector locations are stored redundantly -- once as
        // a linked chain of sectors, and also as a list within each side
        // sector.  I'm not sure how the CBM drives approach this redundancy,
        // so we'll check all instances and consider it a formatting error if
        // they are not consistent.
        let mut side_sectors: Vec<Location> = vec![];
        let mut side_sector_lists: Vec<Vec<Location>> = vec![];
        let mut data_sectors: Vec<Location> = vec![];
        let chain = ChainIterator::new(blocks.clone(), first_side_sector);
        for sector in chain {
            let sector = sector?;
            side_sectors.push(sector.location);

            // Process the list of side sectors which is contained within each side sector.
            let list_bytes = &sector.data
                [SIDE_SECTOR_SSLIST_OFFSET..SIDE_SECTOR_SSLIST_OFFSET + MAX_SIDE_SECTORS * 2];
            let list: Vec<Location> = list_bytes
                .chunks(2)
                .map(|c| Location::new(c[0], c[1]))
                .filter(|l| l.0 != 0)
                .collect();
            side_sector_lists.push(list);

            // How many bytes are used in this side sector?
            let size = match ChainLink::new(&sector.data)? {
                ChainLink::Next(_) => BLOCK_SIZE,
                ChainLink::Tail(size) => size,
            };
            if size < SIDE_SECTOR_DATA_OFFSET || size % 2 == 1 {
                // We must have an even number of bytes beyond SIDE_SECTOR_DATA_OFFSET.
                return Err(DiskError::InvalidRelativeFile.into());
            }

            // Extract the data locations contained in this side sector.
            let mut sectors: Vec<Location> = sector.data[SIDE_SECTOR_DATA_OFFSET..size]
                .chunks(2)
                .map(|c| Location::new(c[0], c[1]))
                .collect();
            data_sectors.append(&mut sectors);
        }

        // Side sector location consistency check
        if side_sectors.len() > MAX_SIDE_SECTORS {
            return Err(DiskError::InvalidRelativeFile.into());
        }
        for list in side_sector_lists {
            if list != side_sectors {
                return Err(DiskError::InvalidRelativeFile.into());
            }
        }

        // Determine the total number of records
        let data_bytes_in_last_block = match data_sectors.last() {
            Some(location) => {
                let blocks = blocks.borrow();
                let last_block = blocks.sector(*location)?;
                match ChainLink::new(last_block)? {
                    ChainLink::Next(_) => return Err(DiskError::InvalidRelativeFile.into()),
                    ChainLink::Tail(size) => size - 2, // Subtract link bytes
                }
            }
            None => 0,
        };
        let data_in_full_blocks = if data_sectors.is_empty() {
            0
        } else {
            (data_sectors.len() - 1) * (BLOCK_SIZE - 2)
        };
        let data_size = data_in_full_blocks + data_bytes_in_last_block;
        if data_size % record_size != 0 {
            return Err(DiskError::InvalidRelativeFile.into());
        }
        let records = data_size / record_size;

        Ok(RelativeFile {
            blocks,
            bam,
            entry,
            record_size,
            data_sectors,
            side_sectors,
            records,
        })
    }

    /// Return position information for the given record.  Since a record may
    /// straddle sector boundaries, a second position may be required to
    /// fully represent the record's storage.
    fn record_position(&self, index: usize) -> io::Result<(Position, Option<Position>)> {
        if index >= self.records {
            return Err(DiskError::InvalidRecordIndex.into());
        }
        let data_offset = self.record_size * index;
        let sector_index = data_offset / (BLOCK_SIZE - 2);
        let sector_offset = data_offset % (BLOCK_SIZE - 2) + 2;
        if sector_offset + self.record_size <= BLOCK_SIZE {
            Ok((
                Position {
                    location: self.data_sectors[sector_index],
                    offset: sector_offset as u8,
                    size: self.record_size as u8,
                },
                None,
            ))
        } else {
            let bytes_in_next_sector = (sector_offset + self.record_size) - BLOCK_SIZE;
            Ok((
                Position {
                    location: self.data_sectors[sector_index],
                    offset: sector_offset as u8,
                    size: (self.record_size - bytes_in_next_sector) as u8,
                },
                Some(Position {
                    location: self.data_sectors[sector_index + 1],
                    offset: 2,
                    size: bytes_in_next_sector as u8,
                }),
            ))
        }
    }
}

impl FileOps for RelativeFile {
    fn entry(&self) -> &DirectoryEntry {
        &self.entry
    }

    fn delete(&mut self) -> io::Result<()> {
        // Deallocate the chain.
        chain::remove_chain(
            self.blocks.clone(),
            self.bam.clone(),
            self.entry.first_sector,
        )?;

        // Deallocate side sectors
        for side_sector in self.side_sectors.iter() {
            self.bam.borrow_mut().free(*side_sector)?
        }

        // Mark the file as "DEL" and "open" (*DEL) in the directory.
        self.entry.file_attributes.file_type = FileType::DEL;
        self.entry.file_attributes.closed_flag = false;
        // Write the new directory entry
        let mut blocks = self.blocks.borrow_mut();
        blocks.positioned_write(&self.entry)?;
        Ok(())
    }

    fn record_count(&self) -> io::Result<usize> {
        Ok(self.records)
    }

    fn record(&self, index: usize) -> io::Result<Vec<u8>> {
        let blocks = self.blocks.borrow();
        let positions = self.record_position(index)?;

        let mut record = blocks.read_position(&positions.0)?.to_vec();
        if let Some(position) = positions.1 {
            record.extend_from_slice(blocks.read_position(&position)?);
        }
        if record.len() != self.record_size {
            return Err(DiskError::InvalidRelativeFile.into());
        }
        Ok(record)
    }

    fn details(&self, writer: &mut Write, verbosity: usize) -> io::Result<()> {
        if verbosity > 0 {
            if let Some(position) = self.entry().position {
                writeln!(writer, "Directory position: {}", position)?;
            }
            writeln!(
                writer,
                "Data sectors: {}",
                Location::format_locations(&self.data_sectors)
            )?;
            writeln!(
                writer,
                "Side sectors: {}",
                Location::format_locations(&self.side_sectors)
            )?;
        }
        Ok(())
    }

    fn occupied_sectors(&self) -> io::Result<Vec<Location>> {
        let mut locations = vec![];
        locations.extend(self.data_sectors.iter());
        locations.extend(self.side_sectors.iter());
        locations.sort();
        locations.dedup();
        Ok(locations)
    }

    fn reader(&self) -> io::Result<Box<Read>> {
        Err(DiskError::NonLinearFile.into())
    }

    fn writer(&self) -> io::Result<Box<Write>> {
        Err(DiskError::NonLinearFile.into())
    }
}
