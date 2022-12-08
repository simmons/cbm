use std::collections::VecDeque;
use std::io::{self, Read};

use crate::disk::block::BlockDeviceRef;
use crate::disk::block::Location;
use crate::disk::block::BLOCK_SIZE;
use crate::disk::chain::{ChainIterator, ChainReader};
use crate::disk::directory::{self, DirectoryEntry};
use crate::disk::DiskError;

use crate::disk::geos::GEOSDirectoryEntry;

/// A block can store 254 bytes of data -- the 256 full block size, minus 2
/// bytes for the next track and sector pointer.
const BLOCK_DATA_SIZE: usize = BLOCK_SIZE - 2;

enum GEOSReaderState {
    DirectoryEntry,
    InfoBlock,
    VLIRIndexBlock,
    Records,
    Finished,
}

/// A GEOSReader serializes a complex GEOS file into a flat byte stream in a
/// manner compatible with the GEOS Convert utility.  This preserves directory
/// entry metadata, info blocks, and VLIR records.
pub struct GEOSReader {
    blocks: BlockDeviceRef,
    state: GEOSReaderState,
    buffer: Vec<u8>,
    is_vlir: bool,
    info_location: Location,
    start_location: Location,
    chains: VecDeque<ChainReader>,
}

impl GEOSReader {
    /// Create a new GEOSReader for serializing the GEOS file at the provided
    /// directory entry.
    pub fn new(blocks: BlockDeviceRef, entry: &DirectoryEntry) -> io::Result<GEOSReader> {
        const CVT_SIGNATURE_OFFSET: usize = 0x20;
        const CVT_SIGNATURE_PRG: &[u8] = b"PRG";
        const CVT_SIGNATURE_SEQ: &[u8] = b"SEQ";
        const CVT_SIGNATURE_SUFFIX: &[u8] = b" formatted GEOS file V1.0";

        // Construct the initial 254-byte data block which contains a copy of the
        // original directory entry and any extra implementation-specific data
        // such as the signature string.
        let mut buffer = [0; BLOCK_SIZE].to_vec();

        // Load the directory entry bytes
        entry.to_bytes(&mut buffer[0..directory::ENTRY_SIZE]);

        // Write the signature string.  I don't think this is required, but both the
        // GEOS Convert2.5 program and Vice's c1541 seem to add this.
        if entry.is_vlir()? {
            buffer[CVT_SIGNATURE_OFFSET..CVT_SIGNATURE_OFFSET + 3]
                .copy_from_slice(CVT_SIGNATURE_PRG);
        } else {
            buffer[CVT_SIGNATURE_OFFSET..CVT_SIGNATURE_OFFSET + 3]
                .copy_from_slice(CVT_SIGNATURE_SEQ);
        }
        buffer[CVT_SIGNATURE_OFFSET + 3..CVT_SIGNATURE_OFFSET + 3 + CVT_SIGNATURE_SUFFIX.len()]
            .copy_from_slice(CVT_SIGNATURE_SUFFIX);

        // Remove the would-be NTS (next track and sector) link.
        buffer.drain(0..2);
        assert!(buffer.len() == BLOCK_DATA_SIZE);

        Ok(GEOSReader {
            blocks,
            state: GEOSReaderState::DirectoryEntry,
            buffer,
            is_vlir: entry.is_vlir()?,
            info_location: entry
                .info_location()?
                .ok_or(DiskError::GEOSInfoNotFound.to_io_error())?,
            start_location: entry.first_sector,
            chains: VecDeque::new(),
        })
    }

    /// Process the VLIR index block by converting (track, sector) pairs into
    /// (block count, bytes used in the final block) pairs.  Along the way,
    /// accumulate the ChainReaders that will be needed to read the VLIR
    /// records in the Records state.
    fn process_vlir_index_block(&mut self) -> io::Result<()> {
        let blocks = self.blocks.borrow();
        let mut index_block = blocks.sector(self.start_location)?[2..].to_vec();

        /// Scan the chain and return the number of blocks used, and the number
        /// of bytes used in the final block.
        fn scan_chain(blocks: BlockDeviceRef, start: Location) -> io::Result<(u8, u8)> {
            let (count, last) = ChainIterator::new(blocks, start).fold(Ok((0, 0)), |acc, b| {
                acc.and_then(|(count, _last)| {
                    b.map(|b| (count + 1, b.data.len() - 1))
                })
            })?;
            if count > ::std::u8::MAX as usize {
                return Err(DiskError::RecordTooLarge.into());
            }
            assert!(last <= ::std::u8::MAX as usize);
            Ok((count as u8, last as u8))
        }

        // For every pair of bytes in the index block, generate an optional chain
        // reader, the block count, and the number of bytes used in the final
        // block.
        let (chains, conversions): (Vec<Option<ChainReader>>, Vec<(u8, u8)>) = index_block
            .chunks_mut(2)
            .take_while(|chunk| chunk[0] != 0x00 || chunk[1] != 0x00)
            .map(|chunk| {
                if chunk[0] == 0x00 && chunk[1] == 0xFF {
                    // Empty record
                    Ok((None, (0x00, 0xFF)))
                } else {
                    let record_start = Location::from_bytes(chunk);
                    let (count, last) = scan_chain(self.blocks.clone(), record_start)?;
                    let reader = ChainReader::new(self.blocks.clone(), record_start);
                    Ok((Some(reader), (count, last)))
                }
            })
            .collect::<io::Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        // Render the new VLIR block where (track, sector) pairs have been converted
        // into (block count, final bytes) pairs.
        let mut block = conversions.iter().fold(
            Vec::with_capacity(BLOCK_DATA_SIZE),
            |mut v, conversion| {
                v.push(conversion.0);
                v.push(conversion.1);
                v
            },
        );
        // Pad remainder of block with zeros.
        block.resize(BLOCK_DATA_SIZE, 0);

        // The converted index block will be fed to the reader in VLIRIndexBlock state.
        self.buffer = block;
        // Save the chain readers for later processing in the Records state.
        self.chains = chains.into_iter().flatten().collect();

        Ok(())
    }

    /// Transition to the next state, loading the buffer with the bytes to be
    /// read in the new state.
    fn next_state(&mut self) -> io::Result<()> {
        match self.state {
            GEOSReaderState::DirectoryEntry => {
                // Transition to reading the GEOS info block.
                self.state = GEOSReaderState::InfoBlock;
                let blocks = self.blocks.borrow();
                let info_block = blocks.sector(self.info_location)?;
                self.buffer = info_block[2..].to_vec();
            }
            GEOSReaderState::InfoBlock => {
                if self.is_vlir {
                    // Transition to reading the VLIR index block.
                    self.state = GEOSReaderState::VLIRIndexBlock;
                    // Convert the VLIR index block and initialize chain readers.
                    self.process_vlir_index_block()?;
                } else {
                    // If this is a sequential file, start reading the single record.
                    self.state = GEOSReaderState::Records;
                    self.chains
                        .push_back(ChainReader::new(self.blocks.clone(), self.start_location));
                }
            }
            GEOSReaderState::VLIRIndexBlock => {
                self.state = GEOSReaderState::Records;
                self.next_state()?;
            }
            GEOSReaderState::Records => {
                match self.chains.pop_front() {
                    Some(mut chain) => {
                        chain.read_to_end(&mut self.buffer)?;
                        // Pad to a multiple of BLOCK_DATA_SIZE unless this is the last record.
                        if !self.chains.is_empty() {
                            let remainder = self.buffer.len() % BLOCK_DATA_SIZE;
                            if remainder > 0 {
                                let padded_size = self.buffer.len() + BLOCK_DATA_SIZE - remainder;
                                self.buffer.resize(padded_size, 0);
                            }
                        }
                    }
                    None => self.state = GEOSReaderState::Finished,
                }
            }
            GEOSReaderState::Finished => {}
        };
        Ok(())
    }
}

impl Read for GEOSReader {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        if let GEOSReaderState::Finished = self.state {
            return Ok(0);
        }

        let mut bytes_written: usize = 0;
        loop {
            let bytes = buf.len().min(self.buffer.len());

            // Write bytes
            let _ = &mut buf[..bytes].copy_from_slice(&self.buffer[..bytes]);
            bytes_written += bytes;

            if bytes == buf.len() {
                // Output buffer filled -- nothing more to do for now.
                break;
            }

            // Reduce the output buffer to the unwritten portion.
            let buf_ref = &mut buf;
            let value: &mut [u8] = std::mem::take(buf_ref);
            *buf_ref = &mut value[bytes..];

            // Reduce the input buffer
            self.buffer.drain(0..bytes);

            // Input buffer drained -- transition to next step
            if self.buffer.is_empty() {
                // State transition
                self.next_state()?;
                if let GEOSReaderState::Finished = self.state {
                    break;
                }
            }
        }
        Ok(bytes_written)
    }
}
