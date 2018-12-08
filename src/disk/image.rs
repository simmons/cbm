use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use memmap::{Mmap, MmapMut, MmapOptions};

use crate::disk::error::DiskError;

/// Provide backing storage (file or memory) for disk images.
pub enum Image {
    ReadOnlyMap(Mmap),
    ReadWriteMap(MmapMut),
    Memory(Box<[u8]>),
}

impl Image {
    pub fn open_memory(length: usize) -> io::Result<Image> {
        Ok(Image::Memory(vec![0; length].into_boxed_slice()))
    }

    pub fn open_read_only<P: AsRef<Path>>(path: P) -> io::Result<Image> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        Ok(Image::ReadOnlyMap(mmap))
    }

    pub fn open_read_write<P: AsRef<Path>>(path: P) -> io::Result<Image> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(Image::ReadWriteMap(mmap))
    }

    pub fn create<P: AsRef<Path>>(path: P, length: usize, create_new: bool) -> io::Result<Image> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(create_new)
            .open(path)?;
        file.set_len(length as u64)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(Image::ReadWriteMap(mmap))
    }

    pub fn len(&self) -> usize {
        match self {
            Image::ReadOnlyMap(mmap) => mmap.len(),
            Image::ReadWriteMap(mmap) => mmap.len(),
            Image::Memory(array) => array.len(),
        }
    }

    fn check_bounds(&self, offset: usize) -> io::Result<()> {
        if offset > self.len() {
            Err(DiskError::InvalidOffset.into())
        } else {
            Ok(())
        }
    }

    pub fn check_writability(&self) -> io::Result<()> {
        match self {
            Image::ReadOnlyMap(_) => Err(DiskError::ReadOnly.into()),
            Image::ReadWriteMap(_) => Ok(()),
            Image::Memory(_) => Ok(()),
        }
    }

    pub fn slice<'a>(&'a self, offset: usize, length: usize) -> io::Result<&'a [u8]> {
        self.check_bounds(offset + length)?;
        Ok(match self {
            Image::ReadOnlyMap(mmap) => &mmap[offset..offset + length],
            Image::ReadWriteMap(mmap) => &mmap[offset..offset + length],
            Image::Memory(array) => &array[offset..offset + length],
        })
    }

    pub fn slice_mut<'a>(&'a mut self, offset: usize, length: usize) -> io::Result<&'a mut [u8]> {
        self.check_bounds(offset + length)?;
        match self {
            Image::ReadOnlyMap(_) => Err(DiskError::ReadOnly.into()),
            Image::ReadWriteMap(mmap) => Ok(&mut mmap[offset..offset + length]),
            Image::Memory(array) => Ok(&mut array[offset..offset + length]),
        }
    }

    pub fn flush(&mut self) -> io::Result<()> {
        match self {
            Image::ReadOnlyMap(_) => Err(DiskError::ReadOnly.into()),
            Image::ReadWriteMap(mmap) => mmap.flush(),
            Image::Memory(_) => Ok(()),
        }
    }
}
