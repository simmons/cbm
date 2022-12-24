use std::cell::RefCell;
use std::fmt;
use std::fmt::Write;
use std::io;
use std::rc::Rc;

use crate::disk::block::{BlockDeviceRef, Location};
use crate::disk::error::DiskError;
use crate::disk::DiskFormat;

/// A BamFormat describes how BAM information is stored for a particular disk
/// image format.
pub struct BamFormat {
    /// The list of sections where BAM entries are stored.
    pub sections: &'static [BamSection],
}

impl BamFormat {
    fn tracks(&self) -> usize {
        self.sections.iter().map(|s| s.tracks).sum()
    }
}

/// BAM can be stored in one or more sections, depending on the disk image
/// format. Each section stores BAM entries for a particular range of tracks.
pub struct BamSection {
    /// The track and sector where this section's bitmaps are stored.
    pub bitmap_location: Location,
    /// The offset within the block where entries start.
    pub bitmap_offset: usize,
    /// The size in bytes of the bitmap.  (E.g. 3 in 1541 BAM entries.)
    pub bitmap_size: usize,
    /// How many bytes apart are the BAM bitmap entries?  (E.g., on 1541 BAM,
    /// this 4 -- one more than the bitmap_size, since we skip over the
    /// free sectors byte when reading the bitmap.)
    pub bitmap_stride: usize,
    /// The track and sector where this section's free sector counts are stored.
    pub free_location: Location,
    /// The offset within the block where entries start.
    pub free_offset: usize,
    /// How many bytes apart are the free sector counts?
    pub free_stride: usize,
    /// The total number of tracks (and hence entries) in this section.
    pub tracks: usize,
}

/// A BamWriter writes BAM entries onto the disk image according to the
/// provided BamFormat.
pub struct BamWriter {
    blocks: BlockDeviceRef,
    format: &'static BamFormat,
}

impl BamWriter {
    pub fn new(blocks: BlockDeviceRef, format: &'static BamFormat) -> BamWriter {
        BamWriter { blocks, format }
    }

    fn write(&mut self, entries: &[BamEntry]) -> io::Result<()> {
        let mut previous_tracks = 0; // tracks handled in previous sections.
        for section in self.format.sections {
            // Read the block containing BAM bitmaps for this section.
            let mut block = self
                .blocks
                .borrow()
                .sector(section.bitmap_location)?
                .to_vec();

            // Render our BAM bitmaps into the block
            for i in 0..section.tracks {
                let offset = section.bitmap_offset + i * section.bitmap_stride;
                entries[previous_tracks + i]
                    .write_bitmap(&mut block[offset..offset + section.bitmap_size]);
            }

            // Do we need to load a different block for writing the free sector count?
            if section.bitmap_location != section.free_location {
                // Write the block containing BAM bitmaps for this section.
                self.blocks
                    .borrow_mut()
                    .sector_mut(section.bitmap_location)?
                    .copy_from_slice(&block);
                // Read the block containing BAM free sector counts for this section.
                block = self.blocks.borrow().sector(section.free_location)?.to_vec();
            }

            // Render our BAM free sector counts into the block
            for i in 0..section.tracks {
                let offset = section.free_offset + i * section.free_stride;
                block[offset] = entries[previous_tracks + i].free_sectors;
            }

            // Write the block containing BAM free sector counts for this section.
            self.blocks
                .borrow_mut()
                .sector_mut(section.free_location)?
                .copy_from_slice(&block);

            previous_tracks += section.tracks;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct BamEntry {
    pub free_sectors: u8,
    pub sector_map: u64,
}

impl BamEntry {
    pub fn new(sectors: u8) -> BamEntry {
        let mut map = 0;
        for _ in 0..sectors {
            map = (map << 1) | 1;
        }
        BamEntry {
            free_sectors: sectors,
            sector_map: map,
        }
    }

    pub fn from_bytes(free_sectors: u8, bitmap: &[u8]) -> BamEntry {
        // Read as many sector bitmap bytes as are present.
        // (1541/1571 has 3 bytes, 1581 has 5 bytes.)
        let mut sector_map: u64 = 0;
        for i in 0..bitmap.len() {
            let byte = bitmap.len() - i - 1;
            sector_map = (sector_map << 8) | bitmap[byte] as u64;
        }

        BamEntry {
            free_sectors,
            sector_map,
        }
    }

    pub fn write_bitmap(&self, bitmap: &mut [u8]) {
        // Write as many sector bitmap bytes as are present in the output buffer.
        // (1541/1571 has 3 bytes, 1581 has 5 bytes.)
        let mut sector_map = self.sector_map;
        for output_byte_ref in bitmap.iter_mut() {
            *output_byte_ref = (sector_map & 0xFF) as u8;
            sector_map >>= 8;
        }
    }

    #[inline]
    pub fn has_availability(&self) -> bool {
        self.free_sectors > 0
    }

    #[inline]
    pub fn sector_map(&self) -> u64 {
        self.sector_map
    }

    #[inline]
    pub fn allocate(&mut self, sector: u8) {
        self.sector_map &= !(1u64 << sector);
        self.update_free_sectors();
    }

    #[inline]
    pub fn free(&mut self, sector: u8) {
        self.sector_map |= 1u64 << sector;
        self.update_free_sectors();
    }

    fn update_free_sectors(&mut self) {
        let mut map = self.sector_map;
        let mut count = 0;
        for _ in 0..64 {
            if map & 1 == 1 {
                count += 1;
            }
            map >>= 1;
        }
        self.free_sectors = count;
    }
}

pub type BamRef = Rc<RefCell<Bam>>;

pub struct Bam {
    bam_rw: Box<BamWriter>,
    format: DiskFormat,
    entries: Vec<BamEntry>,
    invalidated: bool,
}

impl Bam {
    pub fn new(blocks_ref: BlockDeviceRef, disk_format: &DiskFormat) -> Bam {
        let mut bam_entries = vec![];
        for track in 1..=disk_format.bam.tracks() {
            bam_entries.push(BamEntry::new(disk_format.tracks[track as usize].sectors));
        }
        Bam::from_entries(
            disk_format,
            bam_entries,
            Box::new(BamWriter::new(blocks_ref, disk_format.bam)),
        )
    }

    pub fn read(blocks_ref: BlockDeviceRef, disk_format: &DiskFormat) -> io::Result<Bam> {
        let mut entries = Vec::with_capacity(disk_format.bam.tracks());
        let blocks = blocks_ref.borrow();

        for section in disk_format.bam.sections {
            // Read the block containing BAM free sector counts for this section.
            let free_counts_block = blocks.sector(section.free_location)?;

            // Do we need to load a different block for reading the allocation bitmaps?
            let bitmaps_block = if section.bitmap_location != section.free_location {
                // Read the block containing BAM bitmaps for this section.
                blocks.sector(section.bitmap_location)?
            } else {
                // Reuse the same block loaded for the free counts.
                free_counts_block
            };

            // Process each track in this section
            for i in 0..section.tracks {
                // Read the BAM free sector count for this track
                let free_sector_count =
                    free_counts_block[section.free_offset + i * section.free_stride];

                // Read the BAM bitmaps
                let offset = section.bitmap_offset + i * section.bitmap_stride;
                entries.push(BamEntry::from_bytes(
                    //free_sector_counts[i],
                    free_sector_count,
                    &bitmaps_block[offset..offset + section.bitmap_size],
                ));
            }
        }

        Ok(Bam::from_entries(
            disk_format,
            entries,
            Box::new(BamWriter::new(blocks_ref.clone(), disk_format.bam)),
        ))
    }

    fn from_entries(format: &DiskFormat, entries: Vec<BamEntry>, bam_rw: Box<BamWriter>) -> Bam {
        Bam {
            bam_rw,
            format: format.clone(),
            entries,
            invalidated: false,
        }
    }

    #[inline]
    pub fn set_format(&mut self, format: &DiskFormat) {
        self.format = format.clone();
    }

    /// Return the number of blocks free on the disk image, sans any
    /// unallocated directory blocks. This is equivalent to the "blocks
    /// free" output at the bottom of a directory listing. The number of
    /// free blocks is calculated based on the BAM entry free_sectors field.
    /// It is not based on the allocation bitmaps, nor is it based on the file
    /// usage as embedded in directory entries.
    pub fn blocks_free(&self) -> usize {
        let mut blocks_free = 0usize;
        for track in 0..(self.format.last_track as usize) {
            if track == (self.format.directory_track - 1) as usize {
                continue;
            }
            blocks_free += self.entries[track].free_sectors as usize;
        }
        blocks_free
    }

    pub fn entry(&self, track: u8) -> io::Result<&BamEntry> {
        self.check_validity()?;
        Ok(&self.entries[(track - 1) as usize])
    }

    pub fn entry_mut(&mut self, track: u8) -> io::Result<&mut BamEntry> {
        self.check_validity()?;
        Ok(&mut self.entries[(track - 1) as usize])
    }

    pub fn allocate(&mut self, location: Location) -> io::Result<()> {
        self.check_validity()?;
        self.entry_mut(location.0)?.allocate(location.1);
        Ok(())
    }

    pub fn free(&mut self, location: Location) -> io::Result<()> {
        self.check_validity()?;
        self.entry_mut(location.0)?.free(location.1);
        Ok(())
    }

    pub fn next_free_block(&self, previous: Option<Location>) -> io::Result<Location> {
        self.check_validity()?;
        self.format.next_free_block(self, previous)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.check_validity()?;
        self.bam_rw.write(&self.entries)
    }

    pub fn invalidate(&mut self) {
        self.invalidated = true;
    }

    fn check_validity(&self) -> io::Result<()> {
        if self.invalidated {
            Err(DiskError::InvalidBAM.into())
        } else {
            Ok(())
        }
    }

    pub fn allocated_sectors(&self) -> io::Result<Vec<Location>> {
        let mut locations = vec![];
        for track in 0..(self.format.last_track as usize) {
            let mut map = self.entries[track].sector_map;
            for sector in 0..self.format.tracks[track + 1].sectors {
                if map & 1 == 0 {
                    locations.push(Location::new(track as u8 + 1, sector));
                }
                map >>= 1;
            }
        }
        Ok(locations)
    }

    pub fn free_sectors(&self) -> io::Result<Vec<Location>> {
        let mut locations = vec![];
        for track in 0..(self.format.last_track as usize) {
            let mut map = self.entries[track].sector_map;
            for sector in 0..self.format.tracks[track + 1].sectors {
                if map & 1 == 1 {
                    locations.push(Location::new(track as u8 + 1, sector));
                }
                map >>= 1;
            }
        }
        Ok(locations)
    }
}

impl fmt::Debug for Bam {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for track in 0..(self.format.last_track as usize) {
            write!(
                f,
                "t{:02}: [{:02}/{:02}] ",
                track + 1,
                self.entries[track].free_sectors,
                self.format.tracks[track + 1].sectors
            )?;
            let mut map = self.entries[track].sector_map;
            for _ in 0..self.format.tracks[track + 1].sectors {
                let c: char = if map & 1 == 1 { '.' } else { 'x' };
                f.write_char(c)?;
                map >>= 1;
            }
            f.write_char('\n')?;
        }
        writeln!(f, "{} blocks free.", self.blocks_free())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_bam_entry() {
        let bytes: [u8; 4] = [0x12, 0xFF, 0xF9, 0x17];
        let bam_entry = super::BamEntry::from_bytes(bytes[0], &bytes[1..4]);
        assert_eq!(bam_entry.free_sectors, 0x12);
        assert_eq!(bam_entry.sector_map, 0x17F9FF);
    }
}
