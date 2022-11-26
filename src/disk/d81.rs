use std::cell::RefCell;
use std::io;
use std::path::Path;
use std::rc::Rc;

use crate::disk::bam::{BAMFormat, BAMSection};
use crate::disk::block::{BlockDevice, BlockDeviceRef, ImageBlockDevice};
use crate::disk::error::DiskError;
use crate::disk::header::{Header, HeaderFormat};
use crate::disk::{self, BAMRef, Disk, DiskFormat, Geometry, Id, Image, Location, Track, BAM};

const TRACK_COUNT: usize = 80;

/// A description of the header format for this disk image type.
static HEADER_FORMAT: HeaderFormat = HeaderFormat {
    location: Location(40, 0),
    first_directory_offset: 0x00,
    disk_dos_version_offset: 0x02,
    disk_name_offset: 0x04,
    disk_id_offset: 0x16,
    dos_type_offset: 0x19,
    padding_offsets: &[0x14, 0x15, 0x18, 0x1B, 0x1C],
    expected_dos_version: 0x44,
    expected_dos_type: Id([b'3', b'D']),
    double_sided_flag_expectation: None,
};

/// A description of the BAM format for this disk image type.
static BAM_FORMAT: BAMFormat = BAMFormat {
    sections: &[
        BAMSection {
            bitmap_location: Location(40, 1),
            bitmap_offset: 0x11,
            bitmap_size: 5,
            bitmap_stride: 6,
            free_location: Location(40, 1),
            free_offset: 0x10,
            free_stride: 6,
            tracks: 40,
        },
        BAMSection {
            bitmap_location: Location(40, 2),
            bitmap_offset: 0x11,
            bitmap_size: 5,
            bitmap_stride: 6,
            free_location: Location(40, 2),
            free_offset: 0x10,
            free_stride: 6,
            tracks: 40,
        },
    ],
};

/// A description of the disk format for this disk image type.
static DISK_FORMAT: DiskFormat = DiskFormat {
    directory_track: 40,
    first_directory_sector: 3,
    reserved_track: 0,
    first_track: 1,
    last_track: 80,
    last_nonstandard_track: 80,
    interleave: 1,
    directory_interleave: 1,
    geos: false,
    tracks: &TRACKS,
    header: &HEADER_FORMAT,
    bam: &BAM_FORMAT,
};

pub static GEOMETRY: Geometry = Geometry {
    track_layouts: &TRACKS,
    tracks: TRACK_COUNT as u8,
    with_error_table: false,
};

pub static GEOMETRY_ERRORS: Geometry = Geometry {
    track_layouts: &TRACKS,
    tracks: TRACK_COUNT as u8,
    with_error_table: true,
};

static ALLOWED_GEOMETRIES: [&Geometry; 2] = [&GEOMETRY, &GEOMETRY_ERRORS];

#[cfg_attr(rustfmt, rustfmt_skip)]
static TRACKS: [Track; 81] = [
    Track { sectors: 0,  sector_offset:    0, byte_offset: 0, }, // There is no sector 0
    Track { sectors: 40, sector_offset:    0, byte_offset: 0x00000, }, // 1
    Track { sectors: 40, sector_offset:   40, byte_offset: 0x02800, }, // 2
    Track { sectors: 40, sector_offset:   80, byte_offset: 0x05000, }, // 3
    Track { sectors: 40, sector_offset:  120, byte_offset: 0x07800, }, // 4
    Track { sectors: 40, sector_offset:  160, byte_offset: 0x0A000, }, // 5
    Track { sectors: 40, sector_offset:  200, byte_offset: 0x0C800, }, // 6
    Track { sectors: 40, sector_offset:  240, byte_offset: 0x0F000, }, // 7
    Track { sectors: 40, sector_offset:  280, byte_offset: 0x11800, }, // 8
    Track { sectors: 40, sector_offset:  320, byte_offset: 0x14000, }, // 9
    Track { sectors: 40, sector_offset:  360, byte_offset: 0x16800, }, // 10
    Track { sectors: 40, sector_offset:  400, byte_offset: 0x19000, }, // 11
    Track { sectors: 40, sector_offset:  440, byte_offset: 0x1B800, }, // 12
    Track { sectors: 40, sector_offset:  480, byte_offset: 0x1E000, }, // 13
    Track { sectors: 40, sector_offset:  520, byte_offset: 0x20800, }, // 14
    Track { sectors: 40, sector_offset:  560, byte_offset: 0x23000, }, // 15
    Track { sectors: 40, sector_offset:  600, byte_offset: 0x25800, }, // 16
    Track { sectors: 40, sector_offset:  640, byte_offset: 0x28000, }, // 17
    Track { sectors: 40, sector_offset:  680, byte_offset: 0x2A800, }, // 18
    Track { sectors: 40, sector_offset:  720, byte_offset: 0x2D000, }, // 19
    Track { sectors: 40, sector_offset:  760, byte_offset: 0x2F800, }, // 20
    Track { sectors: 40, sector_offset:  800, byte_offset: 0x32000, }, // 21
    Track { sectors: 40, sector_offset:  840, byte_offset: 0x34800, }, // 22
    Track { sectors: 40, sector_offset:  880, byte_offset: 0x37000, }, // 23
    Track { sectors: 40, sector_offset:  920, byte_offset: 0x39800, }, // 24
    Track { sectors: 40, sector_offset:  960, byte_offset: 0x3C000, }, // 25
    Track { sectors: 40, sector_offset: 1000, byte_offset: 0x3E800, }, // 26
    Track { sectors: 40, sector_offset: 1040, byte_offset: 0x41000, }, // 27
    Track { sectors: 40, sector_offset: 1080, byte_offset: 0x43800, }, // 28
    Track { sectors: 40, sector_offset: 1120, byte_offset: 0x46000, }, // 29
    Track { sectors: 40, sector_offset: 1160, byte_offset: 0x48800, }, // 30
    Track { sectors: 40, sector_offset: 1200, byte_offset: 0x4B000, }, // 31
    Track { sectors: 40, sector_offset: 1240, byte_offset: 0x4D800, }, // 32
    Track { sectors: 40, sector_offset: 1280, byte_offset: 0x50000, }, // 33
    Track { sectors: 40, sector_offset: 1320, byte_offset: 0x52800, }, // 34
    Track { sectors: 40, sector_offset: 1360, byte_offset: 0x55000, }, // 35
    Track { sectors: 40, sector_offset: 1400, byte_offset: 0x57800, }, // 36
    Track { sectors: 40, sector_offset: 1440, byte_offset: 0x5A000, }, // 37
    Track { sectors: 40, sector_offset: 1480, byte_offset: 0x5C800, }, // 38
    Track { sectors: 40, sector_offset: 1520, byte_offset: 0x5F000, }, // 39
    Track { sectors: 40, sector_offset: 1560, byte_offset: 0x61800, }, // 40
    Track { sectors: 40, sector_offset: 1600, byte_offset: 0x64000, }, // 41
    Track { sectors: 40, sector_offset: 1640, byte_offset: 0x66800, }, // 42
    Track { sectors: 40, sector_offset: 1680, byte_offset: 0x69000, }, // 43
    Track { sectors: 40, sector_offset: 1720, byte_offset: 0x6B800, }, // 44
    Track { sectors: 40, sector_offset: 1760, byte_offset: 0x6E000, }, // 45
    Track { sectors: 40, sector_offset: 1800, byte_offset: 0x70800, }, // 46
    Track { sectors: 40, sector_offset: 1840, byte_offset: 0x73000, }, // 47
    Track { sectors: 40, sector_offset: 1880, byte_offset: 0x75800, }, // 48
    Track { sectors: 40, sector_offset: 1920, byte_offset: 0x78000, }, // 49
    Track { sectors: 40, sector_offset: 1960, byte_offset: 0x7A800, }, // 50
    Track { sectors: 40, sector_offset: 2000, byte_offset: 0x7D000, }, // 51
    Track { sectors: 40, sector_offset: 2040, byte_offset: 0x7F800, }, // 52
    Track { sectors: 40, sector_offset: 2080, byte_offset: 0x82000, }, // 53
    Track { sectors: 40, sector_offset: 2120, byte_offset: 0x84800, }, // 54
    Track { sectors: 40, sector_offset: 2160, byte_offset: 0x87000, }, // 55
    Track { sectors: 40, sector_offset: 2200, byte_offset: 0x89800, }, // 56
    Track { sectors: 40, sector_offset: 2240, byte_offset: 0x8C000, }, // 57
    Track { sectors: 40, sector_offset: 2280, byte_offset: 0x8E800, }, // 58
    Track { sectors: 40, sector_offset: 2320, byte_offset: 0x91000, }, // 59
    Track { sectors: 40, sector_offset: 2360, byte_offset: 0x93800, }, // 60
    Track { sectors: 40, sector_offset: 2400, byte_offset: 0x96000, }, // 61
    Track { sectors: 40, sector_offset: 2440, byte_offset: 0x98800, }, // 62
    Track { sectors: 40, sector_offset: 2480, byte_offset: 0x9B000, }, // 63
    Track { sectors: 40, sector_offset: 2520, byte_offset: 0x9D800, }, // 64
    Track { sectors: 40, sector_offset: 2560, byte_offset: 0xA0000, }, // 65
    Track { sectors: 40, sector_offset: 2600, byte_offset: 0xA2800, }, // 66
    Track { sectors: 40, sector_offset: 2640, byte_offset: 0xA5000, }, // 67
    Track { sectors: 40, sector_offset: 2680, byte_offset: 0xA7800, }, // 68
    Track { sectors: 40, sector_offset: 2720, byte_offset: 0xAA000, }, // 69
    Track { sectors: 40, sector_offset: 2760, byte_offset: 0xAC800, }, // 70
    Track { sectors: 40, sector_offset: 2800, byte_offset: 0xAF000, }, // 71
    Track { sectors: 40, sector_offset: 2840, byte_offset: 0xB1800, }, // 72
    Track { sectors: 40, sector_offset: 2880, byte_offset: 0xB4000, }, // 73
    Track { sectors: 40, sector_offset: 2920, byte_offset: 0xB6800, }, // 74
    Track { sectors: 40, sector_offset: 2960, byte_offset: 0xB9000, }, // 75
    Track { sectors: 40, sector_offset: 3000, byte_offset: 0xBB800, }, // 76
    Track { sectors: 40, sector_offset: 3040, byte_offset: 0xBE000, }, // 77
    Track { sectors: 40, sector_offset: 3080, byte_offset: 0xC0800, }, // 78
    Track { sectors: 40, sector_offset: 3120, byte_offset: 0xC3000, }, // 79
    Track { sectors: 40, sector_offset: 3160, byte_offset: 0xC5800, }, // 80
];

/// Represent a 1581 disk image in D81 format.  Geometries for this disk image
/// type can describe images with and without attached error tables.
pub struct D81 {
    blocks: Rc<RefCell<ImageBlockDevice>>,
    header: Option<Header>,
    bam: Option<BAMRef>,
    format: Option<DiskFormat>,
}

impl D81 {
    fn new(image: Image) -> io::Result<D81> {
        // Determine the disk image geometry
        let geometry = match Geometry::find_by_size(image.len(), &ALLOWED_GEOMETRIES[..]) {
            Some(geometry) => geometry,
            None => return Err(DiskError::InvalidLayout.into()),
        };

        let blocks = ImageBlockDevice::new(image, geometry);

        let mut d81 = D81 {
            blocks: Rc::new(RefCell::new(blocks)),
            format: None,
            header: None,
            bam: None,
        };
        d81.initialize();
        Ok(d81)
    }

    /// Open an existing D81 disk image as read-only (if `writable` is false)
    /// or read-write (if `writable` is true).
    pub fn open<P: AsRef<Path>>(path: P, writable: bool) -> io::Result<D81> {
        let image = if writable {
            Image::open_read_write(path)?
        } else {
            Image::open_read_only(path)?
        };
        Self::new(image)
    }

    /// Create a new D81 disk image.  If `create_new` is true, no file is
    /// allowed to exist at the target location.  If false, any existing
    /// file will be overwritten.
    pub fn create<P: AsRef<Path>>(
        path: P,
        geometry: &Geometry,
        create_new: bool,
    ) -> io::Result<D81> {
        Self::new(Image::create(path, geometry.size(), create_new)?)
    }

    /// Create a new in-memory D81 disk image.
    pub fn open_memory(geometry: &Geometry) -> io::Result<D81> {
        Self::new(Image::open_memory(geometry.size())?)
    }

    /// Return the geometry for D81 disks.
    #[inline]
    pub fn geometry(with_error_table: bool) -> &'static Geometry {
        if with_error_table {
            &GEOMETRY_ERRORS
        } else {
            &GEOMETRY
        }
    }
}

impl Disk for D81 {
    #[inline]
    fn native_disk_format(&self) -> &'static DiskFormat {
        &DISK_FORMAT
    }

    fn disk_format(&self) -> io::Result<&DiskFormat> {
        match self.format {
            Some(ref format) => Ok(format),
            None => Err(DiskError::Unformatted.into()),
        }
    }

    fn disk_format_mut(&mut self) -> io::Result<&mut DiskFormat> {
        match self.format {
            Some(ref mut format) => Ok(format),
            None => Err(DiskError::Unformatted.into()),
        }
    }

    fn set_disk_format(&mut self, disk_format: Option<DiskFormat>) {
        self.format = disk_format;
    }

    fn blocks(&self) -> BlockDeviceRef {
        self.blocks.clone()
    }

    fn blocks_ref(&self) -> ::std::cell::Ref<'_, dyn BlockDevice> {
        self.blocks.borrow()
    }

    fn blocks_ref_mut(&self) -> ::std::cell::RefMut<'_, dyn BlockDevice> {
        self.blocks.borrow_mut()
    }

    fn header<'a>(&'a self) -> io::Result<&'a Header> {
        match self.header {
            Some(ref header) => Ok(header),
            None => Err(DiskError::Unformatted.into()),
        }
    }

    fn header_mut<'a>(&'a mut self) -> io::Result<&'a mut Header> {
        self.blocks.borrow().check_writability()?;
        match &mut self.header {
            &mut Some(ref mut header) => Ok(header),
            &mut None => Err(DiskError::Unformatted.into()),
        }
    }

    fn set_header(&mut self, header: Option<Header>) {
        self.header = header;
    }

    fn flush_header(&mut self) -> io::Result<()> {
        let header = match self.header {
            Some(ref mut header) => header,
            None => return Err(DiskError::Unformatted.into()),
        };
        header.write(self.blocks.clone(), &HEADER_FORMAT)
    }

    fn bam(&self) -> io::Result<BAMRef> {
        match &self.bam {
            &Some(ref bam) => Ok(bam.clone()),
            &None => Err(DiskError::Unformatted.into()),
        }
    }

    fn set_bam(&mut self, bam: Option<BAM>) {
        disk::set_bam(&mut self.bam, bam);
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use crate::disk::BLOCK_SIZE;

    #[test]
    fn test_track_consistency() {
        let mut sector_offset = 0;
        let mut byte_offset = 0;
        for track in super::TRACKS.iter() {
            assert_eq!(track.sector_offset, sector_offset);
            assert_eq!(track.byte_offset, byte_offset);
            sector_offset += track.sectors as u16;
            byte_offset += track.sectors as u32 * BLOCK_SIZE as u32;
        }
    }
}
