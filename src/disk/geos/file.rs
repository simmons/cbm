use std::io::{self, Read};

use disk::bam::BAMRef;
use disk::block::BlockDeviceRef;
use disk::block::Location;
use disk::chain::{self, ChainIterator, ChainReader};
use disk::directory::{DirectoryEntry, FileType};
use disk::file::FileOps;
use disk::DiskError;

use disk::geos::reader::GEOSReader;
use disk::geos::GEOSDirectoryEntry;
use disk::geos::GEOSInfo;

/// A specialized file type for GEOS files, which include an out-of-band info
/// block and can be structured either sequentially or as a Variable Length
/// Index Record (VLIR) file.
pub struct GEOSFile {
    blocks: BlockDeviceRef,
    bam: BAMRef,
    entry: DirectoryEntry,
    vlir: Option<Vec<Option<Location>>>,
}

impl GEOSFile {
    pub fn new(blocks: BlockDeviceRef, bam: BAMRef, entry: DirectoryEntry) -> io::Result<GEOSFile> {
        // If VLIR, parse the index block.
        let vlir = if entry.is_vlir()? {
            let blocks = blocks.borrow();
            let index_block = blocks.sector(entry.first_sector)?;
            let records: Vec<Option<Location>> = index_block
                .chunks(2)
                .take_while(|chunk| chunk[0] != 0x00 || chunk[1] != 0x00)
                .map(|chunk| {
                    if chunk[0] == 0x00 && chunk[1] == 0xFF {
                        // An "unavailable" record
                        None
                    } else {
                        Some(Location::from_bytes(chunk))
                    }
                })
                .collect();
            Some(records)
        } else {
            None
        };

        Ok(GEOSFile {
            blocks,
            bam,
            entry,
            vlir,
        })
    }

    fn record_start(&self, index: usize) -> io::Result<Option<Location>> {
        Ok(match self.vlir {
            Some(ref records) => match records.get(index) {
                Some(location) => *location,
                None => return Err(DiskError::InvalidRecordIndex.into()),
            },
            None => Some(self.entry.first_sector),
        })
    }

    /// Return the GEOS info block, if one is present for this file.
    pub fn info(&self) -> io::Result<Option<GEOSInfo>> {
        match self.entry.info_location()? {
            Some(location) => {
                let blocks = self.blocks.borrow_mut();
                let info_bytes = blocks.sector(location)?;
                let info = GEOSInfo::from_bytes(info_bytes);
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    /// Read the GEOS file as a stream of bytes in Convert format.
    pub fn reader(&self) -> io::Result<GEOSReader> {
        GEOSReader::new(self.blocks.clone(), &self.entry)
    }
}

impl FileOps for GEOSFile {
    fn entry<'a>(&'a self) -> &'a DirectoryEntry {
        &self.entry
    }

    fn delete(&mut self) -> io::Result<()> {
        // Deallocate the main chain (Sequential) or index block (VLIR).
        chain::remove_chain(
            self.blocks.clone(),
            self.bam.clone(),
            self.entry.first_sector,
        )?;

        // Deallocate the info block, if present.
        if let Some(info_location) = self.entry.info_location()? {
            self.bam.borrow_mut().free(info_location)?
        }

        // If this is a VLIR file, deallocate the VLIR records.
        if let Some(ref records) = self.vlir {
            for record in records.iter() {
                if let Some(record) = record {
                    chain::remove_chain(self.blocks.clone(), self.bam.clone(), *record)?;
                }
            }
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
        Ok(match self.vlir {
            Some(ref records) => records.len(),
            None => 1,
        })
    }

    fn record(&self, index: usize) -> io::Result<Vec<u8>> {
        match self.record_start(index)? {
            Some(location) => {
                let mut reader = ChainReader::new(self.blocks.clone(), location);
                let mut record = vec![];
                reader.read_to_end(&mut record)?;
                Ok(record)
            }
            // Is it correct to interpret an "unavailable" record as zero-length?
            None => Ok(vec![]),
        }
    }

    fn details(&self, writer: &mut io::Write, verbosity: usize) -> io::Result<()> {
        if verbosity > 0 {
            if let Some(position) = self.entry().position {
                writeln!(writer, "Directory position: {}", position)?;
            }
        }
        if let Some(ref records) = self.vlir {
            // Some VLIR files seem to have their record sectors padded to the end with
            // "present but unavailable" markers (0x00 0xFF), so we truncate any such
            // trailing records instead of listing them.
            let available_record_count = records
                .iter()
                .enumerate()
                .rev()
                .find(|(_, r)| r.is_some())
                .map(|(i, _)| i + 1)
                .unwrap_or(records.len());

            if verbosity > 0 {
                writeln!(writer, "VLIR record sector: {}", self.entry.first_sector)?;

                for (i, record) in records.iter().enumerate() {
                    if i >= available_record_count {
                        writeln!(
                            writer,
                            "{} unavailable records follow...",
                            records.len() - available_record_count
                        )?;
                        break;
                    }
                    let locations = match record {
                        Some(record) => {
                            ChainIterator::new(self.blocks.clone(), *record).locations()?
                        }
                        None => vec![],
                    };
                    writeln!(
                        writer,
                        "Record {} sectors: {}",
                        i,
                        Location::format_locations(&locations)
                    )?;
                }
            } else {
                writeln!(writer, "VLIR records: {}", available_record_count)?;
            }
        } else {
            if verbosity > 0 {
                let locations =
                    ChainIterator::new(self.blocks.clone(), self.entry.first_sector).locations()?;
                writeln!(
                    writer,
                    "Occupied sectors: {}",
                    Location::format_locations(&locations)
                )?;
            }
        }
        if let Some(info) = self.info()? {
            writeln!(writer, "GEOS info block:")?;
            write!(writer, "{}", info)?;
        }
        Ok(())
    }

    fn occupied_sectors(&self) -> io::Result<Vec<Location>> {
        // A GEOS file occupies the following sectors:
        // 1. The normal chain (VLIR index, or sequential data).
        // 2. The info block, if present.
        // 3. If VLIR, the chains of each record
        let mut locations = ChainIterator::new(self.blocks.clone(), self.entry.first_sector)
            .map(|r| r.map(|cs| cs.location))
            .collect::<io::Result<Vec<_>>>()?;
        if let Some(info_location) = self.entry.info_location()? {
            locations.push(info_location);
        }
        if let Some(ref vlir_record_starts) = self.vlir {
            for vlir_record_start in vlir_record_starts {
                if let Some(vlir_record_start) = vlir_record_start {
                    let mut record_locations =
                        ChainIterator::new(self.blocks.clone(), *vlir_record_start)
                            .map(|r| r.map(|cs| cs.location))
                            .collect::<io::Result<Vec<_>>>()?;
                    locations.extend(record_locations);
                }
            }
        }
        locations.sort();
        locations.dedup();
        Ok(locations)
    }

    fn reader(&self) -> io::Result<Box<Read>> {
        match self.vlir {
            Some(_) => Err(DiskError::NonLinearFile.into()),
            None => {
                // We allow reading a GEOS sequential file as if it were a linear file.  (The
                // caller will not receive the info block, obviously.)
                Ok(Box::new(ChainReader::new(
                    self.blocks.clone(),
                    self.entry.first_sector,
                )))
            }
        }
    }

    fn writer(&self) -> io::Result<Box<io::Write>> {
        match self.vlir {
            Some(_) => Err(DiskError::NonLinearFile.into()),
            None => Err(DiskError::ReadOnly.into()),
        }
    }
}
