use std::io;

use disk::bam::BAMFormat;
use disk::block::BLOCK_SIZE;
use disk::directory::ENTRY_SIZE;
use disk::error::DiskError;
use disk::header::HeaderFormat;
use disk::{BAMEntry, Location, BAM};

// The "next track" routines reflect the information in Peter Schepers'
// DISK.TXT document found at:
// http://ist.uwaterloo.ca/~schepers/formats/DISK.TXT
// (With a few notable exceptions, as commented below.)

pub struct Track {
    pub sectors: u8,
    pub sector_offset: u16,
    pub byte_offset: u32,
}

#[derive(Clone)]
pub struct DiskFormat {
    pub directory_track: u8,
    /// This should be pointed to from the header sector, but the various image
    /// format documents say not to trust it.
    pub first_directory_sector: u8,
    /// The 1571 has a "second directory track" on track 53 which contains a
    /// second BAM block on sector 0, and all other sectors are wasted.  We
    /// want to treat this entire track as reserved.  This field will be 53
    /// for 1571 images, and 0 for all other images to indicate no reserved
    /// track.  (0 is not otherwise a valid track number.)
    pub reserved_track: u8,
    /// The first track will normally be 1, but may be different in the case of
    /// 1581 partitions.
    pub first_track: u8,
    /// The last track in normal use.  (I.e., inclusive -- not the last track
    /// plus one.)
    pub last_track: u8,
    /// The 1541 drive mechanism (at least) can technically access up to 40
    /// tracks, although CBM DOS only provides access to 35.  This field
    /// contains the last track that can possibly be accessed using
    /// non-standard methods.
    pub last_nonstandard_track: u8,
    /// The default interleave.  This may be changed on a per-image basis to
    /// support other layout varients such as GEOS-formatted disks.
    pub interleave: u8,
    /// Drives may use a special interleave for directory tracks, since
    /// scanning directories usually doesn't involve I/O between the host
    /// and peripheral.
    pub directory_interleave: u8,
    /// Is this disk GEOS-formatted?  All per-drive objects will have this set
    /// to false, but it may be set to true on a per-image basis as needed.
    pub geos: bool,
    /// Per-track parameters for this format (e.g. sectors in each track, byte
    /// offsets, etc.)
    pub tracks: &'static [Track],
    /// A description of the header format for this disk format.
    pub header: &'static HeaderFormat,
    /// A description of the BAM format for this disk format.
    pub bam: &'static BAMFormat,
}

impl DiskFormat {
    #[inline]
    pub fn sectors_in_track(&self, track: u8) -> u8 {
        self.tracks[track as usize].sectors
    }

    #[inline]
    pub fn is_reserved_track(&self, track: u8) -> bool {
        track == self.directory_track || track == self.reserved_track
    }

    #[inline]
    pub fn first_directory_location(&self) -> Location {
        Location(self.directory_track, self.first_directory_sector)
    }

    #[inline]
    pub fn location_iter(&self) -> LocationIterator {
        LocationIterator::new(self)
    }

    /// Return the list of locations which are reserved by CBM DOS and marked
    /// as allocated when a disk image is newly formatted.
    pub fn system_locations(&self) -> Vec<Location> {
        let mut locations = vec![];

        // Header sector
        locations.push(self.header.location);

        // BAM sectors
        for section in self.bam.sections {
            locations.push(section.bitmap_location);
        }

        // The first directory sector
        locations.push(self.first_directory_location());

        // The reserved track (if present in this format)
        if self.reserved_track != 0 {
            for sector in 0..self.tracks[self.reserved_track as usize].sectors {
                locations.push(Location(self.reserved_track, sector));
            }
        }

        // Remove duplicates
        // (e.g., on the 1541 the BAM sector and header sector are the same.)
        locations.sort();
        locations.dedup();
        locations
    }

    /// Return the maximum number of directory entries that are possible for
    /// this format.
    pub fn max_directory_entries(&self) -> usize {
        // Total sectors on directory track
        let total_sectors = self.tracks[self.directory_track as usize].sectors as usize;

        // Sectors on directory track used for non-directory purposes
        let used_sectors = self
            .system_locations()
            .iter()
            .filter(|Location(t, _)| *t == self.directory_track)
            .count() - 1; // -1 because system_locations includes the first directory sector.

        // Sectors available for directory entries
        let sectors = total_sectors - used_sectors;
        // The total number of possible directory entries
        sectors * BLOCK_SIZE / ENTRY_SIZE
    }

    /// Return the total number of data blocks available for files on a freshly
    /// formatted disk. This is the equivalent of the listed "blocks free"
    /// on a blank disk.
    pub fn total_data_blocks(&self) -> usize {
        self.tracks
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                i >= &(self.first_track as usize)
                    && i <= &(self.last_track as usize)
                    && i != &(self.directory_track as usize)
                    && i != &(self.reserved_track as usize)
            })
            .map(|(_, t)| t.sectors as usize)
            .sum()
    }

    fn first_free_track<'a>(&self, bam: &'a BAM) -> io::Result<(u8, &'a BAMEntry)> {
        let max_distance = ::std::cmp::max(
            self.directory_track - self.first_track,
            self.last_track + 1 - self.directory_track,
        );
        for distance in 1..=max_distance {
            // Check bottom half
            if distance <= self.directory_track {
                let track = self.directory_track - distance;
                if track >= self.first_track {
                    let entry = bam.entry(track)?;
                    if entry.has_availability() {
                        return Ok((track, entry));
                    }
                }
            }
            // Check top half
            let track = self.directory_track + distance;
            if track <= self.last_track {
                let entry = bam.entry(track)?;
                if entry.has_availability() {
                    return Ok((track, entry));
                }
            }
        }
        Err(DiskError::DiskFull.into())
    }

    fn first_free_block(&self, bam: &BAM) -> io::Result<Location> {
        if self.geos {
            return self.next_free_block_from_previous(bam, Location(self.first_track, 0));
        }

        // Find available track
        let (track, entry) = self.first_free_track(bam)?;

        // Find available sector
        let mut map = entry.sector_map();
        for sector in 0..self.sectors_in_track(track) {
            if map & 1 == 1 {
                // Sector is available.
                return Ok(Location(track, sector));
            }
            map = map >> 1;
        }

        // Unless the BAM is corrupt (free_sectors is not consistent with the bitmap),
        // this should never happen.
        Err(DiskError::DiskFull.into())
    }

    // If a free track is successfully found, return the following tuple:
    // (track: u8, entry: &BAMEntry, reset_sector: bool)
    fn next_free_track_geos<'a>(
        &self,
        bam: &'a BAM,
        previous_track: u8,
    ) -> io::Result<(u8, &'a BAMEntry, bool)> {
        let mut track = previous_track;

        // If we get to the end (and we didn't start with track 1), make another pass
        // starting with track 1 to find any availability missed on the first
        // pass.  Note that this is slightly different from the algorithm in
        // DISK.TXT [1] which only scans from the current track to the
        // end of the disk.  It's not clear to me how that handles sequential append
        // cases where availability exists prior to the current track, but none
        // on the current track or after.
        const NUM_PASSES: usize = 2;
        let mut passes = if previous_track == self.first_track {
            1
        } else {
            NUM_PASSES
        };
        let mut reset_sector = false;

        // GEOS: Advance the track sequentially until we find availability
        while passes > 0 {
            // Does the current track have availability?
            let entry = bam.entry(track)?;
            if entry.has_availability() {
                return Ok((track, entry, reset_sector));
            }

            // Don't leave the directory track.
            if track == self.directory_track {
                // We're writing directory sectors, but there are no more directory tracks.
                // (The 1571 track 53 "second directory track" doesn't actually hold chained
                // directory sectors, and is mostly wasted except for a second
                // BAM sector.)
                return Err(DiskError::DiskFull.into());
            }

            // Advance to the next track, skipping reserved tracks.
            track += 1;
            if track > self.last_track {
                track = self.first_track;
                passes -= 1;
                reset_sector = true;
            }
            while self.is_reserved_track(track) {
                track += 1;
                // This shouldn't happen with the formats I know about, but just in case there's
                // some oddball format with a reserved track at the end of the disk...
                if track > self.last_track {
                    track = self.first_track;
                    passes -= 1;
                    reset_sector = true;
                }
            }
        }
        Err(DiskError::DiskFull.into())
    }

    // If a free track is successfully found, return the following tuple:
    // (track: u8, entry: &BAMEntry, reset_sector: bool)
    fn next_free_track_cbm<'a>(
        &self,
        bam: &'a BAM,
        previous_track: u8,
    ) -> io::Result<(u8, &'a BAMEntry, bool)> {
        // The CBM algorithm is to grow files away from the central directory track.
        // If the file's previous sector is on the bottom half, the next sector
        // will be on that track or below, if possible.  Likewise, if the
        // file's previous sector is on the top half, the next sector will be
        // on that track or above, if possible.

        // As best as I can tell from DISK.TXT [1], the three-pass scheme mimics the
        // approach used by the CBM DOS.  Three passes are necessary to fully
        // scan the disk for available sectors:  The first pass scans the disk
        // half containing the current track, from the current track outward
        // (away from the directory track), and misses any potential inward
        // availability on that half.  The second pass scans the other half in its
        // entirety, and the third pass scans the original half in its
        // entirety, catching any inward availability that was missed on the
        // first pass.
        const NUM_PASSES: usize = 3;
        let mut passes = NUM_PASSES;
        let mut reset_sector = false;

        let mut track = previous_track;

        // Iterate over every track from the current track outward (pass 1), the
        // entirety of the other half (pass 2), and then the entirety of the
        // original half (pass 3).
        while passes > 0 {
            // Does the current track have availability?
            let entry = bam.entry(track)?;
            if entry.has_availability() {
                return Ok((track, entry, reset_sector));
            }

            // The next candidate track is determined differently depending on whether the
            // previous track was on the directory track, below the directory
            // track (bottom half), or above the directory track (top half).
            if track == self.directory_track {
                // We're writing directory sectors, but there are no more directory tracks.
                // (The 1571 track 53 "second directory track" doesn't actually hold chained
                // directory sectors, and is mostly wasted except for a second
                // BAM sector.)
                return Err(DiskError::DiskFull.into());
            } else if track < self.directory_track {
                // Bottom half: Scan downwards.
                track -= 1;
                while track > 0 && self.is_reserved_track(track) {
                    track -= 1;
                }
                if track < self.first_track {
                    // No more availability downwards.  Jump to the top half.
                    track = self.directory_track + 1;
                    passes -= 1;
                    reset_sector = true;
                }
            } else {
                // Top half: Scan upwards.
                track += 1;
                while self.is_reserved_track(track) {
                    track += 1;
                }
                if track > self.last_track {
                    // No more availability upwards.  Jump to the bottom half.
                    track = self.directory_track - 1;
                    passes -= 1;
                    reset_sector = true;
                }
            }
        }
        Err(DiskError::DiskFull.into())
    }

    // If a free track is successfully found, return the following tuple:
    // (track: u8, entry: &BAMEntry, reset_sector: bool)
    fn next_free_track<'a>(
        &self,
        bam: &'a BAM,
        previous_track: u8,
    ) -> io::Result<(u8, &'a BAMEntry, bool)> {
        if self.geos {
            self.next_free_track_geos(bam, previous_track)
        } else {
            self.next_free_track_cbm(bam, previous_track)
        }
    }

    fn next_free_block_from_previous(&self, bam: &BAM, previous: Location) -> io::Result<Location> {
        let mut sector = previous.1;

        // Find the next track (which will be the current track, if it has
        // availability).
        let (track, entry, reset_sector) = self.next_free_track(bam, previous.0)?;
        let num_sectors = self.sectors_in_track(track);

        // Determine the interleave for this track.
        let interleave = if track == self.directory_track {
            self.directory_interleave
        } else {
            self.interleave
        };

        // Advance the sector by the interleave before scanning.
        if reset_sector {
            sector = 0;
        } else if !self.geos || track == previous.0 {
            // The CBM case and the GEOS same-track case: Apply normal interleave.
            sector += interleave;
            // From DISK.TXT:
            // "Empirical GEOS optimization, get one sector backwards if over track 25"
            if self.geos && (track >= 25) {
                sector -= 1;
            }
        } else {
            // The GEOS different-track case:  Apply GEOS's oddball interleave.

            // Note that "track - previous.0" is overflow-safe, because:
            // 1. The reset_sector case would be used if the track wrapped around to be
            // lower than the previous. 2. The highest (track-previous.0) is 79
            // (1581 case), and the highest possible interleave is 10 (1541
            // case), so the worst case result is 172, which still fits in a u8.

            // From DISK.TXT:
            // "For a different track of a GEOS-formatted disk, use sector skew"
            sector = ((track - previous.0) << 1) + 4 + interleave;
        }

        // Wrap sectors as needed to fit on the track.
        while sector >= num_sectors {
            sector -= num_sectors;
            // From DISK.TXT:
            // "Empirical optimization, get one sector backwards if beyond sector zero"
            if (sector > 0) && !self.geos {
                sector -= 1;
            }
        }

        // Scan for the next free sector
        let start_sector: u8 = sector;
        let map = entry.sector_map();
        loop {
            // Is this sector available?
            if map >> sector & 1 == 1 {
                return Ok(Location(track, sector));
            }

            // Advance to the next sector.
            sector += 1;
            if sector >= num_sectors {
                sector = 0;
            }
            if sector == start_sector {
                // The BAM entry's free sector count indicated free sectors,
                // but there were no free sectors in the bitmap.
                return Err(DiskError::InvalidBAM.into());
            }
        }
    }

    pub fn next_free_block(&self, bam: &BAM, previous: Option<Location>) -> io::Result<Location> {
        match previous {
            Some(previous) => self.next_free_block_from_previous(bam, previous),
            None => self.first_free_block(bam),
        }
    }
}

pub struct LocationIterator<'a> {
    format: &'a DiskFormat,
    next: Option<Location>,
}

impl<'a> LocationIterator<'a> {
    fn new(format: &'a DiskFormat) -> LocationIterator {
        LocationIterator {
            format,
            next: Some(Location::new(format.first_track, 0)),
        }
    }
}

impl<'a> Iterator for LocationIterator<'a> {
    type Item = Location;

    fn next(&mut self) -> Option<Location> {
        let current_location = self.next;
        self.next = match current_location {
            Some(Location(mut track, mut sector)) => {
                sector += 1;
                if sector == self.format.tracks[track as usize].sectors {
                    track += 1;
                    sector = 0;
                }
                if track > self.format.last_track {
                    None
                } else {
                    Some(Location(track, sector))
                }
            }
            None => None,
        };
        current_location
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use disk::{D64, D71, D81, Disk};

    const TOTAL_ALLOCABLE_BLOCKS: usize = 664;
    const REMAINING_DIRECTORY_BLOCKS: usize = 17;
    const TEST_ALLOCATION_LIMIT: usize = TOTAL_ALLOCABLE_BLOCKS * 4;

    fn get_fresh_d64() -> (DiskFormat, D64) {
        let mut d64 = D64::open_memory(D64::geometry(false)).unwrap();
        d64.write_format(&"test".into(), &"t1".into()).unwrap();
        let format = d64.disk_format().unwrap().clone();
        (format, d64)
    }

    fn get_fresh_d64_geos() -> (DiskFormat, D64) {
        let mut d64 = D64::open_memory(D64::geometry(false)).unwrap();
        d64.write_format(&"test".into(), &"t1".into()).unwrap();
        d64.disk_format_mut().unwrap().geos = true;
        let format = d64.disk_format().unwrap().clone();
        (format, d64)
    }

    fn allocate_chain(
        d64: &mut D64,
        limit: Option<usize>,
        start: Option<Location>,
    ) -> (usize, bool) {
        let mut blocks_allocated = 0usize;
        let mut disk_full = false;
        let mut location = start;
        let format = d64.disk_format().unwrap().clone();
        let bam = d64.bam().unwrap();
        let mut bam = bam.borrow_mut();
        loop {
            let next = match format.next_free_block(&bam, location) {
                Ok(l) => l,
                Err(ref e) => match DiskError::from_io_error(&e) {
                    Some(ref e) if *e == DiskError::DiskFull => {
                        disk_full = true;
                        break;
                    }
                    Some(_) => break,
                    None => break,
                },
            };
            bam.allocate(next).unwrap();
            blocks_allocated += 1;
            if let Some(limit) = limit {
                if blocks_allocated == limit {
                    break;
                }
            }
            if blocks_allocated > TEST_ALLOCATION_LIMIT {
                // Prevent infinite loop in case of failure
                panic!("runaway chain allocation.");
            }
            println!("next({:?}) -> {}", location, next);
            location = Some(next);
        }
        (blocks_allocated, disk_full)
    }

    #[test]
    fn test_next_for_all_sectors() {
        let (format, d64) = get_fresh_d64();
        for track in format.first_track..=format.last_track {
            for sector in 0..format.tracks[track as usize].sectors {
                let location = Location(track, sector);
                let bam = d64.bam().unwrap();
                let bam = bam.borrow();
                let next = format.next_free_block(&bam, Some(location)).unwrap();
                println!("next({}) -> {}", location, next);
            }
        }
    }

    #[test]
    fn test_next_for_all_sectors_with_allocation() {
        let (format, d64) = get_fresh_d64();
        let mut blocks_allocated = 0usize;
        'outer: for track in format.first_track..=format.last_track {
            if format.is_reserved_track(track) {
                continue;
            }
            for sector in 0..format.tracks[track as usize].sectors {
                let location = Location(track, sector);
                let bam = d64.bam().unwrap();
                let mut bam = bam.borrow_mut();
                let next = match format.next_free_block(&bam, Some(location)) {
                    Ok(l) => l,
                    Err(_) => break 'outer,
                };
                bam.allocate(next).unwrap();
                blocks_allocated += 1;
                println!("next({}) -> {}", location, next);
            }
        }
        println!("blocks allocated: {}", blocks_allocated);
        println!("BAM: {:?}", d64.bam());
        assert_eq!(blocks_allocated, TOTAL_ALLOCABLE_BLOCKS);
    }

    #[test]
    fn test_full_chain_allocation() {
        let (_format, mut d64) = get_fresh_d64();
        let (blocks_allocated, disk_full) = allocate_chain(&mut d64, None, None);
        println!(
            "blocks allocated: {} disk_full={:?}",
            blocks_allocated, disk_full
        );
        println!("BAM: {:?}", d64.bam());
        assert!(disk_full);
        assert_eq!(blocks_allocated, TOTAL_ALLOCABLE_BLOCKS);
    }

    #[test]
    fn test_full_chain_allocation_geos() {
        let (_format, mut d64) = get_fresh_d64_geos();
        let (blocks_allocated, disk_full) = allocate_chain(&mut d64, None, None);
        println!(
            "blocks allocated: {} disk_full={:?}",
            blocks_allocated, disk_full
        );
        println!("BAM: {:?}", d64.bam());
        assert!(disk_full);
        assert_eq!(blocks_allocated, TOTAL_ALLOCABLE_BLOCKS);
    }

    #[test]
    fn test_directory_chain_allocation() {
        let (format, mut d64) = get_fresh_d64();
        let start = Location(format.directory_track, 1);
        let (blocks_allocated, disk_full) = allocate_chain(&mut d64, None, Some(start));
        println!(
            "blocks allocated: {} disk_full={:?}",
            blocks_allocated, disk_full
        );
        println!("BAM: {:?}", d64.bam());
        assert!(disk_full);
        assert_eq!(blocks_allocated, REMAINING_DIRECTORY_BLOCKS);
    }

    #[test]
    fn test_directory_chain_allocation_geos() {
        let (format, mut d64) = get_fresh_d64_geos();
        let start = Location(format.directory_track, 1);
        let (blocks_allocated, disk_full) = allocate_chain(&mut d64, None, Some(start));
        println!(
            "blocks allocated: {} disk_full={:?}",
            blocks_allocated, disk_full
        );
        println!("BAM: {:?}", d64.bam());
        assert!(disk_full);
        assert_eq!(blocks_allocated, REMAINING_DIRECTORY_BLOCKS);
    }

    #[test]
    fn test_max_directory_entries() {
        let mut d64 = D64::open_memory(D64::geometry(false)).unwrap();
        d64.write_format(&"test".into(), &"t1".into()).unwrap();
        assert_eq!(d64.disk_format().unwrap().max_directory_entries(), 144);

        let mut d71 = D71::open_memory(D71::geometry(false)).unwrap();
        d71.write_format(&"test".into(), &"t1".into()).unwrap();
        assert_eq!(d71.disk_format().unwrap().max_directory_entries(), 144);

        let mut d81 = D81::open_memory(D81::geometry(false)).unwrap();
        d81.write_format(&"test".into(), &"t1".into()).unwrap();
        assert_eq!(d81.disk_format().unwrap().max_directory_entries(), 296);
    }

    #[test]
    fn test_total_data_blocks() {
        let mut d64 = D64::open_memory(D64::geometry(false)).unwrap();
        d64.write_format(&"test".into(), &"t1".into()).unwrap();
        assert_eq!(d64.disk_format().unwrap().total_data_blocks(), 664);

        let mut d71 = D71::open_memory(D71::geometry(false)).unwrap();
        d71.write_format(&"test".into(), &"t1".into()).unwrap();
        assert_eq!(d71.disk_format().unwrap().total_data_blocks(), 1328);

        let mut d81 = D81::open_memory(D81::geometry(false)).unwrap();
        d81.write_format(&"test".into(), &"t1".into()).unwrap();
        assert_eq!(d81.disk_format().unwrap().total_data_blocks(), 3160);
    }
}
