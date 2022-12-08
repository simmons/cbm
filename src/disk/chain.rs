use std::collections::HashSet;
use std::io::{self, Write};

use crate::disk::bam::BamRef;
use crate::disk::block::{BlockDeviceRef, Location, BLOCK_SIZE};
use crate::disk::directory::DirectoryEntry;
use crate::disk::error::DiskError;

/// A "zero" chain link is a link that indicates that this is a tail block, and
/// it has zero data bytes used.  (Which means it has a total of two bytes
/// used, counting the link itself.)
pub static CHAIN_LINK_ZERO: ChainLink = ChainLink::Tail(2);

#[derive(Debug)]
pub enum ChainLink {
    Next(Location),
    Tail(usize), // used bytes
}

impl ChainLink {
    #[inline]
    pub fn new(block: &[u8]) -> io::Result<ChainLink> {
        if block[0] == 0x00 {
            // This is the last sector of the chain, so the next byte indicates how much of
            // this sector is actually used.
            if block[1] < 1 {
                // It's not valid for a chain sector to not include the first two bytes
                // as allocated.
                return Err(DiskError::InvalidChainLink.into());
            }
            Ok(ChainLink::Tail(block[1] as usize + 1)) // 2..=256
        } else {
            Ok(ChainLink::Next(Location::new(block[0], block[1])))
        }
    }

    #[inline]
    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert!(bytes.len() >= 2);
        match &self {
            ChainLink::Next(location) => location.write_bytes(bytes),
            ChainLink::Tail(size) => {
                assert!(*size >= 2 && *size <= 256);
                bytes[0] = 0x00;
                bytes[1] = (*size - 1) as u8;
            }
        }
    }
}

/// A ChainSector is the result of a chain iteration, and provides the block contents and the
/// location from which it was read.
pub struct ChainSector {
    /// The 256-byte block contents, which includes the two-byte NTS (next track and sector) link.
    pub data: Vec<u8>,
    pub location: Location,
}

/// Returns a ChainSector which includes the NTS (next track and sector) link.
pub struct ChainIterator {
    blocks: BlockDeviceRef,
    next_sector: Option<Location>,
    visited_sectors: HashSet<Location>,
    block: [u8; BLOCK_SIZE],
}

impl ChainIterator {
    /// Create a new chain iterator starting at the specified location.
    pub fn new(blocks: BlockDeviceRef, starting_sector: Location) -> ChainIterator {
        ChainIterator {
            blocks,
            next_sector: Some(starting_sector),
            visited_sectors: HashSet::new(),
            block: [0u8; BLOCK_SIZE],
        }
    }

    /// Read the entire chain and return a list of locations.
    pub fn locations(self) -> io::Result<Vec<Location>> {
        self.map(|r| r.map(|cs| cs.location)).collect()
    }
}

impl Iterator for ChainIterator {
    type Item = io::Result<ChainSector>;

    fn next(&mut self) -> Option<io::Result<ChainSector>> {
        let location = match self.next_sector.take() {
            Some(next) => next,
            None => return None,
        };

        // Loop detection.
        if !self.visited_sectors.insert(location) {
            return Some(Err(DiskError::ChainLoop.into()));
        }

        // Read the next sector.
        {
            let blocks = self.blocks.borrow();
            let block = match blocks.sector(location) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            self.block.copy_from_slice(block);
        }

        // Trim the block if needed.
        let size = match ChainLink::new(&self.block[..]) {
            Ok(ChainLink::Next(location)) => {
                self.next_sector = Some(location);
                BLOCK_SIZE // The entire sector is used.
            }
            Ok(ChainLink::Tail(size)) => size,
            Err(e) => return Some(Err(e)),
        };
        let block = &self.block[..size];

        Some(Ok(ChainSector {
            data: block.to_vec(),
            location,
        }))
    }
}

/// ChainReader objects implement the Read trait are used to read a byte stream
/// represented as a series of chained sectors on the disk image.  Simple files
/// (e.g. CBM PRG and SEQ files) store data in a single chain where the
/// beginning track and sector is provided in the directory entry. More exotic
/// file types (GEOS, REL, etc.) use more complex structures, possibly with
/// multiple ChainReader objects (e.g. a GEOS VLIR file may provide a
/// ChainReader for each record).
pub struct ChainReader {
    chain: ChainIterator,
    block: Option<Vec<u8>>,
    eof: bool,
}

impl ChainReader {
    pub fn new(blocks: BlockDeviceRef, start: Location) -> ChainReader {
        let chain = ChainIterator::new(blocks, start);
        ChainReader {
            chain,
            block: None,
            eof: false,
        }
    }
}

impl io::Read for ChainReader {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let mut total_nbytes = 0;
        while !buf.is_empty() && !self.eof {
            match self.block.take() {
                Some(mut block) => {
                    // Copy as much of this block as possible into the caller-provided buffer.
                    let nbytes = block.len().min(buf.len());
                    let _ = &buf[0..nbytes].copy_from_slice(&block[0..nbytes]);
                    total_nbytes += nbytes;

                    // Reduce the block slice to the unread portion (which may be zero bytes).
                    if block.len() == nbytes {
                    } else {
                        // Reduce
                        let mut tail = block.split_off(nbytes);
                        ::std::mem::swap(&mut block, &mut tail);
                        // Return the unread portion
                        self.block = Some(block);
                    }

                    // Reduce the provided buffer slice to the unwritten portion.
                    let buf_ref = &mut buf;
                    let value: &mut [u8] = std::mem::take(buf_ref);
                    *buf_ref = &mut value[nbytes..];
                }
                None => {
                    // Read the next block.
                    match self.chain.next() {
                        Some(Ok(mut block)) => {
                            // discard the next-track/sector bytes
                            self.block = Some(block.data.split_off(2));
                            // Loop back to the Some(_) case to process the block.
                        }
                        Some(Err(e)) => {
                            self.eof = true;
                            return Err(e);
                        }
                        None => self.eof = true,
                    }
                }
            }
        }
        Ok(total_nbytes)
    }
}

/// A writer for writing data to a chain.  The chain is extended as needed according to the
/// allocation algorithm for the disk format.
pub struct ChainWriter {
    blocks: BlockDeviceRef,
    bam: BamRef,
    entry: DirectoryEntry,
    location: Location,
    block: Vec<u8>,
    dirty: bool,
}

impl ChainWriter {
    pub fn new(
        blocks: BlockDeviceRef,
        bam: BamRef,
        entry: DirectoryEntry,
        start: Location,
    ) -> io::Result<ChainWriter> {
        // Advance to the last block in the chain.
        let tail_block;
        let mut tail_location;
        {
            let blocks = blocks.borrow();

            let mut block = blocks.sector(start)?;
            tail_location = start;
            while let ChainLink::Next(location) = ChainLink::new(block)? {
                block = blocks.sector(location)?;
                tail_location = location;
            }
            tail_block = block.to_vec();
        }

        Ok(ChainWriter {
            blocks,
            bam,
            entry,
            location: tail_location,
            block: tail_block,
            dirty: true,
        })
    }

    fn increment_entry_blocks(&mut self) -> io::Result<()> {
        let mut blocks = self.blocks.borrow_mut();
        blocks.positioned_read(&mut self.entry)?;
        self.entry.file_size += 1;
        blocks.positioned_write(&self.entry)?;
        Ok(())
    }

    fn allocate_next_block(&mut self) -> io::Result<usize> {
        // NOTE: The ordering of these steps is important for consistency.  We don't
        // want a block to be allocated in BAM, then not used because an error
        // was thrown later.

        // Write the current block without the updated link.
        self.write_current_block()?;

        // Find a new block.
        let next_location = self.bam.borrow_mut().next_free_block(None)?;

        // Initialize a fresh block in memory with a link indicating a tail block with
        // zero bytes used. (Really, two bytes used for the link, but zero data
        // bytes used.)
        for i in 2..BLOCK_SIZE {
            self.block[i] = 0;
        }
        ChainLink::Tail(2).to_bytes(&mut self.block[..]);

        // Write the fresh block to the new location
        self.blocks
            .borrow_mut()
            .sector_mut(next_location)?
            .copy_from_slice(&self.block);

        // Allocate the next block.
        self.bam.borrow_mut().allocate(next_location)?;

        // Increment the directory entry's file size (measured in blocks)
        self.increment_entry_blocks()?;

        // If allocation succeeds, only then do we link the current block to the next
        // block.
        let mut blocks = self.blocks.borrow_mut();
        let block = match blocks.sector_mut(self.location) {
            Ok(block) => block,
            Err(e) => {
                // Roll back the allocation.
                self.bam.borrow_mut().free(next_location)?;
                return Err(e);
            }
        };
        next_location.write_bytes(block);

        // Update state
        self.location = next_location;

        // Return the available bytes in the newly loaded block, which is always two
        // less than the block size.
        Ok(BLOCK_SIZE - 2)
    }

    fn write_current_block(&mut self) -> io::Result<()> {
        // Write the current block
        let mut blocks = self.blocks.borrow_mut();
        blocks
            .sector_mut(self.location)?
            .copy_from_slice(&self.block);
        Ok(())
    }
}

impl Drop for ChainWriter {
    fn drop(&mut self) {
        let _result = self.flush();
    }
}

// NOTE: allocating and updating entry block size should be atomic.

impl io::Write for ChainWriter {
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
        self.dirty = true;
        let mut total_nbytes = 0;
        while !buf.is_empty() {
            let (offset, remaining) = match ChainLink::new(&self.block)? {
                ChainLink::Next(_) => unreachable!(), // The stored buffer is always a tail block.
                ChainLink::Tail(nbytes) if nbytes == BLOCK_SIZE => {
                    // Allocate a new block
                    let remaining = self.allocate_next_block()?;
                    (BLOCK_SIZE - remaining, remaining)
                }
                ChainLink::Tail(nbytes) => (nbytes, BLOCK_SIZE - nbytes),
            };

            // Copy as much of the caller-provided buffer as possible into the block.
            let nbytes = remaining.min(buf.len());
            let _ = &self.block[offset..offset + nbytes].copy_from_slice(&buf[0..nbytes]);
            total_nbytes += nbytes;

            // Update the block link's indication of used bytes.
            ChainLink::Tail(offset + nbytes).to_bytes(&mut self.block);

            // Reduce the provided buffer slice to the unwritten portion.
            buf = &buf[nbytes..];
        }
        Ok(total_nbytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.dirty {
            // Write the current block
            self.write_current_block()?;

            // Flush the BAM
            self.bam.borrow_mut().flush()?;

            // Flush the underlying medium.
            let mut blocks = self.blocks.borrow_mut();
            blocks.flush()?;

            self.dirty = false;
        }

        Ok(())
    }
}

pub fn remove_chain(blocks: BlockDeviceRef, bam: BamRef, start: Location) -> io::Result<()> {
    // Read the whole chain first to be sure we can visit every block with no
    // errors.
    let locations = ChainIterator::new(blocks, start).locations()?;

    // Deallocate
    let mut bam = bam.borrow_mut();
    for location in locations {
        bam.free(location)?;
    }
    bam.flush()?;

    Ok(())
}
