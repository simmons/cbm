use std::cell::RefCell;
use std::fmt;
use std::io::{self, Write};
use std::rc::Rc;

use crate::disk::error::DiskError;
use crate::disk::format::DiskFormat;
use crate::disk::image::Image;
use crate::disk::{Geometry, Track};
use crate::util;

pub const BLOCK_SIZE: usize = 256;

pub type BlockDeviceRef = Rc<RefCell<dyn BlockDevice>>;

pub trait BlockDevice {
    fn check_writability(&self) -> io::Result<()>;
    fn geometry(&self) -> &Geometry;
    fn sector(&self, location: Location) -> io::Result<&[u8]>;
    fn sector_mut(&mut self, location: Location) -> io::Result<&mut [u8]>;
    fn error_table(&self) -> io::Result<Option<&[u8]>>;
    fn error_table_mut(&mut self) -> io::Result<Option<&mut [u8]>>;
    fn flush(&mut self) -> io::Result<()>;

    fn sector_owned(&self, location: Location) -> io::Result<Vec<u8>> {
        Ok(self.sector(location)?.to_owned())
    }

    fn read_position<'a>(&'a self, position: &Position) -> io::Result<&'a [u8]> {
        let block = self.sector(position.location)?;
        Ok(&block[position.offset as usize..position.offset as usize + position.size as usize])
    }

    fn positioned_read(&self, positioned_data: &mut dyn PositionedData) -> io::Result<()> {
        let position = positioned_data.position()?;
        let block = self.sector(position.location)?;
        positioned_data.positioned_read(
            &block[position.offset as usize..position.offset as usize + position.size as usize],
        )?;
        Ok(())
    }

    fn positioned_write(&mut self, positioned_data: &dyn PositionedData) -> io::Result<()> {
        let position = positioned_data.position()?;
        let block = self.sector_mut(position.location)?;
        positioned_data.positioned_write(
            &mut block[position.offset as usize..position.offset as usize + position.size as usize],
        )?;
        Ok(())
    }

    fn dump(&self, writer: &mut dyn Write) -> io::Result<()> {
        let locations = LocationIterator::from_geometry(self.geometry());
        for location in locations {
            writeln!(writer)?;
            writeln!(writer, "track {:02} sector {:02}", location.0, location.1)?;
            let block = self.sector(location)?;
            writeln!(writer, "{}", util::hex(block))?;
        }
        if let Some(error_table) = self.error_table()? {
            writeln!(writer)?;
            writeln!(writer, "Error table:")?;
            let mut index = 0;
            for track in 1..=self.geometry().tracks {
                write!(writer, "track {:02}: ", track)?;
                for _sector in 0..self.geometry().track_layouts[track as usize].sectors {
                    write!(writer, "{:02x} ", error_table[index])?;
                    index += 1;
                }
                writeln!(writer)?;
            }
        }
        Ok(())
    }
}

pub struct ImageBlockDevice {
    image: Image,
    geometry: &'static Geometry,
}

impl ImageBlockDevice {
    pub fn new(image: Image, geometry: &'static Geometry) -> ImageBlockDevice {
        ImageBlockDevice { image, geometry }
    }

    pub fn get_offset(&self, location: Location) -> io::Result<usize> {
        let track: usize = location.0 as usize;
        let sector = location.1;
        if track < 1 || track > (self.geometry.tracks as usize) {
            return Err(DiskError::InvalidLocation.into());
        }
        if sector >= self.geometry.track_layouts[track].sectors {
            return Err(DiskError::InvalidLocation.into());
        }
        let offset =
            self.geometry.track_layouts[track].byte_offset as usize + sector as usize * BLOCK_SIZE;
        Ok(offset)
    }
}

impl BlockDevice for ImageBlockDevice {
    #[inline]
    fn check_writability(&self) -> io::Result<()> {
        self.image.check_writability()
    }

    #[inline]
    fn geometry(&self) -> &Geometry {
        self.geometry
    }

    fn sector(&self, location: Location) -> io::Result<&[u8]> {
        let offset = self.get_offset(location)?;
        self.image.slice(offset, BLOCK_SIZE)
    }

    fn sector_mut(&mut self, location: Location) -> io::Result<&mut [u8]> {
        self.image.check_writability()?;
        let offset = self.get_offset(location)?;
        self.image.slice_mut(offset, BLOCK_SIZE)
    }

    fn error_table(&self) -> io::Result<Option<&[u8]>> {
        match self.geometry.error_table_offset() {
            Some(offset) => Ok(Some(self.image.slice(offset, self.image.len() - offset)?)),
            None => Ok(None),
        }
    }

    fn error_table_mut(&mut self) -> io::Result<Option<&mut [u8]>> {
        self.image.check_writability()?;
        match self.geometry.error_table_offset() {
            Some(offset) => {
                let length = self.image.len();
                Ok(Some(self.image.slice_mut(offset, length - offset)?))
            }
            None => Ok(None),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.image.check_writability()?;
        self.image.flush()
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord)]
pub struct Location(pub u8, pub u8); // Track and sector

impl Location {
    #[inline]
    pub fn new(track: u8, sector: u8) -> Location {
        Location(track, sector)
    }
    pub fn from_bytes(bytes: &[u8]) -> Location {
        assert!(bytes.len() >= 2);
        Location(bytes[0], bytes[1])
    }
    pub fn write_bytes(&self, bytes: &mut [u8]) {
        assert!(bytes.len() >= 2);
        bytes[0] = self.0;
        bytes[1] = self.1;
    }

    pub fn format_locations(locations: &[Location]) -> String {
        locations
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({},{})", self.0, self.1)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Position {
    pub location: Location,
    pub offset: u8,
    pub size: u8,
}

pub trait PositionedData {
    fn position(&self) -> io::Result<Position>;
    fn positioned_read(&mut self, buffer: &[u8]) -> io::Result<()>;
    fn positioned_write(&self, buffer: &mut [u8]) -> io::Result<()>;
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "({},{}@0x{:02x})",
            self.location.0, self.location.1, self.offset
        )
    }
}

pub struct LocationIterator {
    last_track: u8,
    tracks: &'static [Track],
    next: Option<Location>,
}

impl LocationIterator {
    fn from_geometry(geometry: &Geometry) -> LocationIterator {
        const FIRST_TRACK: u8 = 1;
        LocationIterator {
            last_track: geometry.tracks,
            tracks: geometry.track_layouts,
            next: Some(Location::new(FIRST_TRACK, 0)),
        }
    }
    #[allow(unused)]
    fn from_format(format: &DiskFormat) -> LocationIterator {
        LocationIterator {
            last_track: format.last_track,
            tracks: format.tracks,
            next: Some(Location::new(format.first_track, 0)),
        }
    }
}

impl Iterator for LocationIterator {
    type Item = Location;

    fn next(&mut self) -> Option<Location> {
        let location = match self.next {
            Some(location) => location,
            None => return None,
        };

        let mut next_location = location;
        next_location.1 += 1;
        if next_location.1 >= self.tracks[next_location.0 as usize].sectors {
            next_location.0 += 1;
            next_location.1 = 0;
            if next_location.0 > self.last_track {
                self.next = None;
                return Some(location);
            }
        }
        self.next = Some(next_location);
        Some(location)
    }
}
