use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::error;
use std::error::Error;
use std::fmt;
use std::io;
use std::iter::FromIterator;

use disk::chain::ChainIterator;
use disk::error::DiskError;
use disk::file::FileOps;
use disk::format::DiskFormat;
use disk::DirectoryEntry;
use disk::Disk;
use disk::Location;
use petscii::Petscii;

/// A validation error represents an inconsistency in the disk image found by
/// the validate() function.
#[derive(Clone, Debug, PartialEq)]
pub enum ValidationError {
    Unknown,
    SystemSectorNotAllocated(Location),
    SectorMisallocated(Location),
    SectorMisoccupied(Location, Petscii),
    SectorOveroccupied(Location, Petscii, Petscii),
    FileScanError(DiskError, Petscii),
}

impl error::Error for ValidationError {
    /// Provide terse descriptions of the errors.
    fn description(&self) -> &str {
        use self::ValidationError::*;
        match *self {
            Unknown => "Unknown error",
            SystemSectorNotAllocated(_) => "System sector not allocated",
            SectorMisallocated(_) => "Sector misallocated",
            SectorMisoccupied(_, _) => "Sector misoccupied",
            SectorOveroccupied(_, _, _) => "Sector occupied by multiple files",
            FileScanError(_, _) => "File scan error",
        }
    }
}

impl fmt::Display for ValidationError {
    /// Provide human-readable descriptions of the errors.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::ValidationError::*;
        match *self {
            SystemSectorNotAllocated(l) => write!(f, "System sector not allocated: {}", l),
            SectorMisallocated(l) => write!(f, "Sector misallocated: {}", l),
            SectorMisoccupied(l, ref filename) => {
                write!(f, "Sector {} misoccupied by file: {:?}", l, filename)
            }
            SectorOveroccupied(location, ref filename1, ref filename2) => write!(
                f,
                "Sector {} occupied by multiple files, including at least: {:?} {:?}",
                location, filename1, filename2
            ),
            FileScanError(ref e, ref filename) => write!(f, "Error scanning {:?}: {}", filename, e),
            _ => f.write_str(self.description()),
        }
    }
}

/// Convert a slice of Locations to a HashSet.
fn hashset_from_locations(data: &[Location]) -> HashSet<Location> {
    HashSet::from_iter(data.iter().cloned())
}

/// Given a set of locations, return the inverse -- that is, a set of all of
/// the disk's locations that were not represented in the given set.
fn invert_locations(locations: &HashSet<Location>, format: &DiskFormat) -> HashSet<Location> {
    let mut inverse = HashSet::new();
    for track in format.first_track..=format.last_track {
        for sector in 0..format.tracks[track as usize].sectors {
            let location = Location::new(track, sector);
            if !locations.contains(&location) {
                inverse.insert(location);
            }
        }
    }
    inverse
}

/// Open the specified file and return a list of all occupied sectors.  This is
/// its own function so we can handle either of two possible error sources as
/// one.
fn scan_file<T: Disk>(disk: &T, entry: &DirectoryEntry) -> io::Result<Vec<Location>>
where
    T: ?Sized,
{
    disk.open_file_from_entry(entry)?.occupied_sectors()
}

/// Check the consistency of the provided disk.  Unlike the "validate" ("v0:")
/// command in CBM DOS, this is a read-only operation and does not attempt any
/// repairs.  A list of validation errors is returned.
pub fn validate<T: Disk>(disk: &T) -> io::Result<Vec<ValidationError>>
where
    T: ?Sized,
{
    static SYSTEM_OWNER: &str = "CBM DOS";
    let mut errors: Vec<ValidationError> = vec![];
    let format = disk.disk_format()?;
    let bam_ref = disk.bam()?;
    let bam = bam_ref.borrow_mut();
    let system_sectors = hashset_from_locations(&format.system_locations());
    let allocated_sectors = hashset_from_locations(&bam.allocated_sectors()?);
    let free_sectors = hashset_from_locations(&bam.free_sectors()?);

    // Build a list of all occupied sectors and their owners
    // 1. System sectors
    let mut occupied_sector_map: HashMap<Location, Petscii> = HashMap::new();
    for system_sector in system_sectors.iter() {
        occupied_sector_map.insert(*system_sector, SYSTEM_OWNER.into());
    }
    // 2. Add the directory chain as occupied sectors.
    let directory_start = Location::new(format.directory_track, format.first_directory_sector);
    let mut directory_locations = ChainIterator::new(disk.blocks(), directory_start)
        .map(|r| r.map(|cs| cs.location))
        .collect::<io::Result<Vec<_>>>()?;
    if let Some(ref geos_header) = disk.header()?.geos {
        // Add the GEOS border sector if this is a GEOS disk.
        directory_locations.push(geos_header.border);
    }
    for location in directory_locations {
        occupied_sector_map.insert(location, SYSTEM_OWNER.into());
    }
    // 3. All files
    for entry in disk.iter() {
        let entry = entry?;
        let file_occupied_sectors = match scan_file(disk, &entry) {
            Ok(file_occupied_sectors) => file_occupied_sectors,
            Err(e) => match DiskError::from_io_error(&e) {
                Some(e) => {
                    errors.push(ValidationError::FileScanError(e, entry.filename.clone()));
                    continue;
                }
                None => return Err(e),
            },
        };
        for location in file_occupied_sectors.iter() {
            match occupied_sector_map.entry(*location) {
                Entry::Occupied(owner) => {
                    errors.push(ValidationError::SectorOveroccupied(
                        *location,
                        owner.get().clone(),
                        entry.filename.clone(),
                    ));
                }
                Entry::Vacant(v) => {
                    v.insert(entry.filename.clone());
                }
            };
        }
    }
    let occupied_sectors = HashSet::from_iter(occupied_sector_map.keys().map(|l| l.clone()));
    let unoccupied_sectors = invert_locations(&occupied_sectors, format);

    // Confirm all system sectors are still allocated
    for location in system_sectors.iter() {
        if !allocated_sectors.contains(&location) {
            errors.push(ValidationError::SystemSectorNotAllocated(*location));
        }
    }

    // Look for sectors that are allocated but not occupied.
    for misallocated_sector in allocated_sectors.intersection(&unoccupied_sectors) {
        errors.push(ValidationError::SectorMisallocated(*misallocated_sector));
    }

    // Look for sectors that are occupied but not allocated.
    for misoccupied_sector in free_sectors.intersection(&occupied_sectors) {
        if system_sectors.contains(misoccupied_sector) {
            // System sector misoccupation was handled with SystemSectorNotAllocated above.
            continue;
        }
        let filename = occupied_sector_map
            .get(misoccupied_sector)
            .map(|f| f.clone())
            .unwrap_or("None".into());
        errors.push(ValidationError::SectorMisoccupied(
            *misoccupied_sector,
            filename.clone(),
        ));
    }

    Ok(errors)
}
