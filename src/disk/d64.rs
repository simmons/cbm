use std::cell::RefCell;
use std::io;
use std::path::Path;
use std::rc::Rc;

use crate::disk::bam::{BAMFormat, BAMSection};
use crate::disk::block::{BlockDevice, BlockDeviceRef, ImageBlockDevice};
use crate::disk::error::DiskError;
use crate::disk::header::{Header, HeaderFormat};
use crate::disk::{self, BAMRef, Disk, DiskFormat, Geometry, Image, Location, Track, BAM};

/// A description of the header format for this disk image type.
static HEADER_FORMAT: HeaderFormat = HeaderFormat {
    location: Location(18, 0),
    first_directory_offset: 0x00,
    disk_format_type_offset: 0x02,
    disk_name_offset: 0x90,
    disk_id_offset: 0xA2,
    directory_dos_version_offset: 0xA5,
    directory_format_type_offset: 0xA6,
    padding_offsets: &[0xA0, 0xA1, 0xA4, 0xA7, 0xA8, 0xA9, 0xAA],
    default_disk_format_type: b'A',
    default_directory_dos_version: b'2',
    default_directory_format_type: b'A',
    double_sided_flag_expectation: None,
};

/// A description of the BAM format for this disk image type.
static BAM_FORMAT: BAMFormat = BAMFormat {
    sections: &[BAMSection {
        bitmap_location: Location(18, 0),
        bitmap_offset: 0x05,
        bitmap_size: 3,
        bitmap_stride: 4,
        free_location: Location(18, 0),
        free_offset: 0x04,
        free_stride: 4,
        tracks: 35,
    }],
};

/// A description of the disk format for this disk image type.
static DISK_FORMAT: DiskFormat = DiskFormat {
    directory_track: 18,
    first_directory_sector: 1,
    reserved_track: 0,
    first_track: 1,
    last_track: 35,
    last_nonstandard_track: 40,
    interleave: 10,
    directory_interleave: 3,
    geos: false,
    tracks: &TRACKS,
    header: &HEADER_FORMAT,
    bam: &BAM_FORMAT,
};

static GEOMETRY_35: Geometry = Geometry {
    track_layouts: &TRACKS,
    tracks: 35,
    with_error_table: false,
};

static GEOMETRY_35_ERRORS: Geometry = Geometry {
    track_layouts: &TRACKS,
    tracks: 35,
    with_error_table: true,
};

static GEOMETRY_40: Geometry = Geometry {
    track_layouts: &TRACKS,
    tracks: 40,
    with_error_table: false,
};

static GEOMETRY_40_ERRORS: Geometry = Geometry {
    track_layouts: &TRACKS,
    tracks: 40,
    with_error_table: true,
};

static ALLOWED_GEOMETRIES: [&Geometry; 4] = [
    &GEOMETRY_35,
    &GEOMETRY_35_ERRORS,
    &GEOMETRY_40,
    &GEOMETRY_40_ERRORS,
];

#[rustfmt::skip]
static TRACKS: [Track; 41] = [
    Track { sectors: 0,  sector_offset: 0,   byte_offset: 0, }, // There is no track 0.
    Track { sectors: 21, sector_offset: 0,   byte_offset: 0x00000, }, // 1
    Track { sectors: 21, sector_offset: 21,  byte_offset: 0x01500, }, // 2
    Track { sectors: 21, sector_offset: 42,  byte_offset: 0x02A00, }, // 3
    Track { sectors: 21, sector_offset: 63,  byte_offset: 0x03F00, }, // 4
    Track { sectors: 21, sector_offset: 84,  byte_offset: 0x05400, }, // 5
    Track { sectors: 21, sector_offset: 105, byte_offset: 0x06900, }, // 6
    Track { sectors: 21, sector_offset: 126, byte_offset: 0x07E00, }, // 7
    Track { sectors: 21, sector_offset: 147, byte_offset: 0x09300, }, // 8
    Track { sectors: 21, sector_offset: 168, byte_offset: 0x0A800, }, // 9
    Track { sectors: 21, sector_offset: 189, byte_offset: 0x0BD00, }, // 10
    Track { sectors: 21, sector_offset: 210, byte_offset: 0x0D200, }, // 11
    Track { sectors: 21, sector_offset: 231, byte_offset: 0x0E700, }, // 12
    Track { sectors: 21, sector_offset: 252, byte_offset: 0x0FC00, }, // 13
    Track { sectors: 21, sector_offset: 273, byte_offset: 0x11100, }, // 14
    Track { sectors: 21, sector_offset: 294, byte_offset: 0x12600, }, // 15
    Track { sectors: 21, sector_offset: 315, byte_offset: 0x13B00, }, // 16
    Track { sectors: 21, sector_offset: 336, byte_offset: 0x15000, }, // 17
    Track { sectors: 19, sector_offset: 357, byte_offset: 0x16500, }, // 18
    Track { sectors: 19, sector_offset: 376, byte_offset: 0x17800, }, // 19
    Track { sectors: 19, sector_offset: 395, byte_offset: 0x18B00, }, // 20
    Track { sectors: 19, sector_offset: 414, byte_offset: 0x19E00, }, // 21
    Track { sectors: 19, sector_offset: 433, byte_offset: 0x1B100, }, // 22
    Track { sectors: 19, sector_offset: 452, byte_offset: 0x1C400, }, // 23
    Track { sectors: 19, sector_offset: 471, byte_offset: 0x1D700, }, // 24
    Track { sectors: 18, sector_offset: 490, byte_offset: 0x1EA00, }, // 25
    Track { sectors: 18, sector_offset: 508, byte_offset: 0x1FC00, }, // 26
    Track { sectors: 18, sector_offset: 526, byte_offset: 0x20E00, }, // 27
    Track { sectors: 18, sector_offset: 544, byte_offset: 0x22000, }, // 28
    Track { sectors: 18, sector_offset: 562, byte_offset: 0x23200, }, // 29
    Track { sectors: 18, sector_offset: 580, byte_offset: 0x24400, }, // 30
    Track { sectors: 17, sector_offset: 598, byte_offset: 0x25600, }, // 31
    Track { sectors: 17, sector_offset: 615, byte_offset: 0x26700, }, // 32
    Track { sectors: 17, sector_offset: 632, byte_offset: 0x27800, }, // 33
    Track { sectors: 17, sector_offset: 649, byte_offset: 0x28900, }, // 34
    Track { sectors: 17, sector_offset: 666, byte_offset: 0x29A00, }, // 35
    Track { sectors: 17, sector_offset: 683, byte_offset: 0x2AB00, }, // 36
    Track { sectors: 17, sector_offset: 700, byte_offset: 0x2BC00, }, // 37
    Track { sectors: 17, sector_offset: 717, byte_offset: 0x2CD00, }, // 38
    Track { sectors: 17, sector_offset: 734, byte_offset: 0x2DE00, }, // 39
    Track { sectors: 17, sector_offset: 751, byte_offset: 0x2EF00, }, // 40
];

/// Represent a 1541 disk image in D64 format.  Geometries for this disk image
/// type can describe either 35-track or 40-track disks and images with and
/// without attached error tables.
pub struct D64 {
    blocks: Rc<RefCell<ImageBlockDevice>>,
    header: Option<Header>,
    bam: Option<BAMRef>,
    format: Option<DiskFormat>,
}

impl D64 {
    fn new(image: Image) -> io::Result<D64> {
        // Determine the disk image geometry
        let geometry = match Geometry::find_by_size(image.len(), &ALLOWED_GEOMETRIES[..]) {
            Some(geometry) => geometry,
            None => return Err(DiskError::InvalidLayout.into()),
        };

        let blocks = ImageBlockDevice::new(image, geometry);

        let mut d64 = D64 {
            blocks: Rc::new(RefCell::new(blocks)),
            format: None,
            header: None,
            bam: None,
        };
        d64.initialize();
        Ok(d64)
    }

    /// Open an existing D64 disk image as read-only (if `writable` is false)
    /// or read-write (if `writable` is true).
    pub fn open<P: AsRef<Path>>(path: P, writable: bool) -> io::Result<D64> {
        let image = if writable {
            Image::open_read_write(path)?
        } else {
            Image::open_read_only(path)?
        };
        Self::new(image)
    }

    /// Create a new D64 disk image.  If `create_new` is true, no file is
    /// allowed to exist at the target location.  If false, any existing
    /// file will be overwritten.
    pub fn create<P: AsRef<Path>>(
        path: P,
        geometry: &Geometry,
        create_new: bool,
    ) -> io::Result<D64> {
        Self::new(Image::create(path, geometry.size(), create_new)?)
    }

    /// Create a new in-memory D64 disk image.
    pub fn open_memory(geometry: &Geometry) -> io::Result<D64> {
        Self::new(Image::open_memory(geometry.size())?)
    }

    /// Return the 35-track geometry for D64 disks.
    #[inline]
    pub fn geometry(with_error_table: bool) -> &'static Geometry {
        if with_error_table {
            &GEOMETRY_35_ERRORS
        } else {
            &GEOMETRY_35
        }
    }

    /// Return the 40-track geometry for D64 disks.
    #[inline]
    pub fn geometry_40_tracks(with_error_table: bool) -> &'static Geometry {
        if with_error_table {
            &GEOMETRY_40_ERRORS
        } else {
            &GEOMETRY_40
        }
    }
}

impl Disk for D64 {
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

    fn header(&self) -> io::Result<&Header> {
        match self.header {
            Some(ref header) => Ok(header),
            None => Err(DiskError::Unformatted.into()),
        }
    }

    fn header_mut(&mut self) -> io::Result<&mut Header> {
        self.blocks.borrow().check_writability()?;
        match self.header {
            Some(ref mut header) => Ok(header),
            None => Err(DiskError::Unformatted.into()),
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
        match self.bam {
            Some(ref bam) => Ok(bam.clone()),
            None => Err(DiskError::Unformatted.into()),
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
