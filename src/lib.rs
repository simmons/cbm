//! This is a Rust library for working with formats found in Commodore Business
//! Machines (CBM) products from the 1980's, including the legendary Commodore
//! 64 home computer. Most of the provided functionality currently centers
//! around disk images.
//!
//! Features:
//!
//! * Perform operations on 1541 (D64), 1571 (D71), and 1581 (D81) disk images.
//! * Format disk images.
//! * Iterate directory entries.  (Including those in the GEOS border sector.)
//! * Read, write, delete, and rename files.
//! * Validate the consistency of disk images.
//! * Provide access to Relative (REL), GEOS Sequential, and GEOS VLIR files.
//! (Currently read-only.)
//! * Emulate the 1541/1571/1581 CBM DOS's "next available track and sector"
//! algorithm as closely as possible when writing files.
//! * Interpret GEOS info blocks.
//! * A sample `cdisk` program for operating on D64/D71/D81 disk images.
//! * Convert between Petscii and Unicode.
//! * Render sprite graphics (including GEOS icons) into text using Unicode
//! block elements.
//!
//! Current shortcomings:
//!
//! * 1581 partitions are not supported.
//! * Error tables embedded in disk images are accommodated, but not used for
//! anything useful.
//! * Relative, GEOS Sequential, and GEOS VLIR files are currently read-only.
//!
//! Ideas for future improvements might include:
//!
//! * Conversion of Commodore BASIC's binary tokenized format (e.g. petcat
//! functionality).
//! * Conversion of VIC-II native bitmap framebuffers.
//! * Format a disk image with GEOS disk headers.
//! * Access hard disk images.
//!
//! The potential scope of this library includes Commodore 8-bit computers
//! ranging from the Commodore PET through the Commodore 128, as there are
//! elements of commonality across all these machines.  The scope includes
//! hardware and software components that were produced by Commodore or
//! third-party components that were tightly associated with Commodore
//! computers (e.g. GEOS) or whose inner workings are necessary to interpret
//! certain disk formats (e.g. PrologicDOS).  Parsing of application data (e.g.
//! Word Writer or geoPaint) would be best handled in a separate crate.
//!
//! # Example
//!
//! The following example opens a disk image, reads the directory, and prints a list of directory
//! entries belonging to GEOS applications, along with their text description from the GEOS info
//! block:
//!
//! ```
//! use std::io;
//! use cbm::disk;
//! use cbm::disk::directory::Extra;
//! use cbm::disk::geos::GEOSFileType;
//! # fn list_geos_applications(disk_image_filename: &str) -> io::Result<()> {
//! # let disk_image_filename = "/tmp/disk.d64";
//!
//! // Open the disk image read-only
//! let disk = disk::open(disk_image_filename, false)?;
//!
//! // Print directory entries for GEOS applications
//! disk.directory()?
//!     .iter()
//!     .filter_map(|entry| {
//!         // Only regard directory entries with extra GEOS metadata
//!         match entry.extra {
//!             Extra::GEOS(ref geos_extra) => Some((entry, geos_extra)),
//!             _ => None,
//!         }
//!     })
//!     .filter(|(_, geos_extra)| {
//!         // Only regard applications
//!         geos_extra.geos_file_type == GEOSFileType::Application
//!     })
//!     .map(|(entry, geos_extra)| {
//!         // Read the info block and obtain the description string
//!         let description = match disk
//!             .open_file_from_entry(entry)
//!             .and_then(|f| f.geos_info())
//!         {
//!             Ok(Some(info)) => info.description.to_escaped_string(),
//!             Ok(None) => "No info block".to_string(),
//!             Err(e) => format!("error: {}", e),
//!         };
//!         (entry, description)
//!     })
//!     .for_each(|(entry, description)| {
//!         println!("{} {}", entry, description);
//!     });
//! # Ok(())
//! # }
//! ```
//!
//! On a particular 1581 GEOS disk image, the following output is generated:
//!
//! ```text
//! 18   "paint drivers"    usr  Creates drivers that print to geoPaint files, a file for each PAGE, or OVERLAID.
//! 141  "geowrite"         usr  geoWrite (64 version) is a WYSIWYG word processor.
//! 152  "geopaint"         usr  geoPaint is a full-featured graphics editor.
//! 67   "text grabber"     usr  Converts files created by other word processors into data files for geoWrite.
//! 60   "geolaser"         usr     GEOLASER prints geoWrite files on the LaserWriter printer.
//! 67   "geomerge"         usr     Use geoMerge to print- merge your geoWrite files.
//! 111  "geospell"         usr  Use geoSpell to correct your spelling in geoWrite documents.
//! 20   "convert"          usr  This version allows you to select multiple files!
//! ```
//!
//! For more examples, see the accompanying `cdisk` program, which allows various operations to be
//! performed on disk images from the command line.  For example, expanded directory listings:
//!
//! ```text
//! cdisk GEOS64.D81 dir -vv
//! ...
//! 13   "ALARM CLOCK"      usr  GEOS(Sequential, Desk Accessory, 1986-09-03-12:00)
//! GEOS info block:
//! ▛▀▀▀▀▀▀▀▀▀▀▜ Type: Desk Accessory (Sequential)
//! ▌ ▄▀▖▛▜▗▀▄ ▐ Program addresses: load=0x5400 end=0x5fd8 start=0x5400
//! ▌▗▘▟▞▀▀▚▙▝▖▐ Class: Alarm clock V1.0
//! ▌▝▟▘▗ ▌▗▝▙▘▐ Author: David Durran
//! ▌ ▞▗ ▐  ▖▚ ▐ Application:  Mgr V1.0
//! ▌ ▌▄ ▐▄▄▄▐ ▐ Description: Set the alarm clock to keep yourself time-conscious.
//! ▌ ▌▗    ▖▐ ▐
//! ▌ ▐ ▗ ▖▗ ▌ ▐
//! ▌ ▐▙▖▝ ▗▟▌ ▐
//! ▌ ▀▘▝▀▀▘▝▀ ▐
//! ▀▀▀▀▀▀▀▀▀▀▀▀
//! ...
//! ```
//!
//!
//! # Design of disk image access
//!
//! Support for disk images was built using a layered scheme:
//!
//! 1. `Image` provides access to the underlying storage containing the disk
//!    image -- either a disk image file or an in-memory array.
//! 2. `BlockDevice` divides the image into tracks and sectors according to
//!    a `Geometry`.
//! 3. `DiskFormat` describes how a particular disk format uses the tracks and
//!    sectors to store and retrieve common CBM DOS structures such as the disk
//!    header, Block Availability Map (BAM), and directories.
//! 4. The `Disk` trait exposes high-level functionality such as opening files,
//!    formatting the disk, validating, etc.
//! 5. Opening a file yields a `File` object which is accessed in varying ways
//!    according to the scheme used to implement its underlying structure
//!    (linear files, relative files, GEOS VLIR, etc.)
//!
//! # Design shortcomings
//!
//! CBM DOS tracks start at 1 instead of 0, which causes no end of
//! implementation confusion.  The API provided by this crate reflects this
//! 1-based indexing, but it's not used consistently throughout the internals.
//! In particular, `Geometry.track_layouts` uses 1-based track indexing (with
//! the zeroth track unused), while arrays of BAM entries use 0-based indexing.
//!
//! In the interest of providing a simple and flexible API to callers, many
//! components store their own `Rc<RefCell<BlockDevice>>` reference to the
//! disk's block storage.  This allows multiple `File` objects, readers, and
//! writers to be in use at the same time, and reduces the lifetime puzzling
//! that callers might otherwise need to do.  However, this means that a disk
//! image may remain open after its `Disk` implementation is dropped (e.g. if a
//! `File` is still present), and it's possible that some combinations of steps
//! may result in corrupted or inconsistent disk images.  (For example, what
//! happens if you start writing to a file, then format the disk, then continue
//! writing to the file?)  Better runtime checks may help with this.
//!
//! A purely read-only implementation could be built in such a way that slice
//! references to the mapped disk image could be passed all the way to the user
//! layer with no (or minimal) additional copying.  However, supporting write
//! access and multiple active readers/writers requires accessing the mapped
//! sectors through a `Rc<RefCell<_>>` , which limits our ability to return
//! references.  Thus, copies are made when reading sector chains.  (Perhaps
//! some sort of exotic data access scheme could be concocted to avoid this,
//! but I'm reluctant to go to a lot of trouble to avoid copying 256 bytes.)
//!
//! # License
//!
//! Cbm is distributed under the terms of both the MIT license and the
//! Apache License (Version 2.0).
//!
//! See LICENSE-APACHE and LICENSE-MIT for details.

pub mod disk;

mod petscii;
mod sprite;
mod util;

pub use crate::petscii::Petscii;
pub use crate::sprite::Sprite;
