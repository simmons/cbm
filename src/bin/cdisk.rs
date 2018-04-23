extern crate cbm;
extern crate clap;

use clap::{App, AppSettings, Arg, SubCommand};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::process;

use cbm::disk::directory::FileType;
use cbm::disk::file::{File, FileOps, Scheme};
use cbm::disk::geos::GEOSFile;
use cbm::disk::{self, D64, D71, D81, DiskType};
use cbm::Petscii;

// Possible exit codes
static _EXIT_SUCCESS: i32 = 0;
static EXIT_FAILURE: i32 = 1;

/// If a dash is specified for a filename, this indicates that the user wants
/// to read from standard input or write to standard output.
static STDINOUT_PSEUDOFILENAME: &str = "-";

fn main() {
    // Parse command-line arguments
    let app = App::new("Commodore Disk Image Utility")
        .version("0.1.0")
        .about("Read, write, and understand D64/D71/D81 disk images.")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(Arg::with_name("diskimage").required(true))
        .subcommand(
            SubCommand::with_name("bam")
                .about("Block Availability Map (BAM) commands")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(
                    SubCommand::with_name("show").about("Show the Block Availability Map (BAM)"),
                )
                .subcommand(
                    SubCommand::with_name("allocate")
                        .about("Mark block(s) as allocated in the BAM.")
                        .arg(
                            Arg::with_name("track")
                                .validator(optional_track_validator)
                                .required(true),
                        )
                        .arg(
                            Arg::with_name("sector")
                                .validator(optional_sector_validator)
                                .required(true),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("free")
                        .about("Mark block(s) as free in the BAM.")
                        .arg(
                            Arg::with_name("track")
                                .validator(optional_track_validator)
                                .required(true),
                        )
                        .arg(
                            Arg::with_name("sector")
                                .validator(optional_sector_validator)
                                .required(true),
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("read")
                .about("Read a file from a disk image.")
                .arg(Arg::with_name("source_filename").required(true))
                .arg(Arg::with_name("destination_filename").required(false)),
        )
        .subcommand(
            SubCommand::with_name("write")
                .about("Write a file to a disk image.")
                .arg(
                    Arg::with_name("type")
                        .short("t")
                        .long("type")
                        .takes_value(true)
                        .possible_values(&["prg", "seq", "usr"])
                        .default_value("seq")
                        .help("CBM file type"),
                )
                .arg(Arg::with_name("source_filename").required(true))
                .arg(Arg::with_name("destination_filename").required(false)),
        )
        .subcommand(
            SubCommand::with_name("append")
                .about("Append to a file.")
                .arg(Arg::with_name("source_filename").required(true))
                .arg(Arg::with_name("destination_filename").required(false)),
        )
        .subcommand(
            SubCommand::with_name("geosread")
                .about("Read a file.")
                .arg(Arg::with_name("source_filename").required(true))
                .arg(Arg::with_name("destination_filename").required(false)),
        )
        .subcommand(SubCommand::with_name("create").about("Create a blank disk image"))
        .subcommand(
            SubCommand::with_name("dir")
                .about("Show a directory listing")
                .arg(
                    Arg::with_name("verbose")
                        .short("v")
                        .long("verbose")
                        .multiple(true)
                        .help("Show more detail"),
                ),
        )
        .subcommand(
            SubCommand::with_name("format")
                .about("Format a disk image")
                .arg(Arg::with_name("name").required(true))
                .arg(Arg::with_name("id").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rename")
                .about("Rename a file.")
                .arg(Arg::with_name("original_filename").required(true))
                .arg(Arg::with_name("new_filename").required(true)),
        )
        .subcommand(
            SubCommand::with_name("delete")
                .about("Delete (scratch) a file.")
                .arg(Arg::with_name("filename").required(true)),
        )
        .subcommand(
            SubCommand::with_name("dump")
                .about("Provide a hex dump of a disk image or file.")
                .arg(Arg::with_name("filename").required(false)),
        )
        .subcommand(SubCommand::with_name("validate").about("Validate a disk image."));

    let mut app_clone = app.clone();
    let matches = app.get_matches();

    let diskimage = matches.value_of("diskimage").unwrap();
    let result = match matches.subcommand() {
        ("bam", Some(m)) => match m.subcommand() {
            ("show", Some(_)) => cmd_bam_show(diskimage),
            ("allocate", Some(m)) => cmd_bam_edit(
                diskimage,
                optional_track_parser(m.value_of("track").unwrap().to_string()),
                optional_sector_parser(m.value_of("sector").unwrap().to_string()),
                true,
            ),
            ("free", Some(m)) => cmd_bam_edit(
                diskimage,
                optional_track_parser(m.value_of("track").unwrap().to_string()),
                optional_sector_parser(m.value_of("sector").unwrap().to_string()),
                false,
            ),
            _ => {
                app_clone.print_help().unwrap();
                println!();
                process::exit(EXIT_FAILURE);
            }
        },
        ("read", Some(m)) => cmd_read(
            diskimage,
            m.value_of("source_filename").unwrap(),
            m.value_of("destination_filename"),
        ),
        ("write", Some(m)) => cmd_write(
            diskimage,
            m.value_of("source_filename").unwrap(),
            m.value_of("destination_filename"),
            m.value_of("type")
                .and_then(|t| FileType::from_string(t))
                .unwrap_or(FileType::SEQ),
        ),
        ("append", Some(m)) => cmd_append(
            diskimage,
            m.value_of("source_filename").unwrap(),
            m.value_of("destination_filename"),
        ),
        ("geosread", Some(m)) => cmd_geosread(
            diskimage,
            m.value_of("source_filename").unwrap(),
            m.value_of("destination_filename"),
        ),
        ("create", Some(_)) => cmd_create(diskimage),
        ("dir", Some(m)) => cmd_dir(diskimage, m.occurrences_of("verbose")),
        ("format", Some(m)) => cmd_format(
            diskimage,
            m.value_of("name").unwrap(),
            m.value_of("id").unwrap(),
        ),
        ("rename", Some(m)) => cmd_rename(
            diskimage,
            m.value_of("original_filename").unwrap(),
            m.value_of("new_filename").unwrap(),
        ),
        ("delete", Some(m)) => cmd_delete(diskimage, m.value_of("filename").unwrap()),
        ("dump", Some(m)) => cmd_dump(diskimage, m.value_of("filename")),
        ("validate", Some(_)) => cmd_validate(diskimage),
        _ => {
            app_clone.print_help().unwrap();
            println!();
            process::exit(EXIT_FAILURE);
        }
    };
    if let Err(e) = result {
        eprintln!("Error: {}", e);
    }
}

fn optional_u8_parser(v: String, min: u8, max: u8) -> Result<Option<u8>, ()> {
    if v == "all" {
        Ok(None)
    } else {
        match v.parse::<u8>() {
            Ok(n) if n >= min && n <= max => Ok(Some(n)),
            _ => Err(()),
        }
    }
}

fn optional_u8_validator(v: String, min: u8, max: u8) -> Result<(), String> {
    match optional_u8_parser(v, min, max) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "Expected a value from {}-{}, or \"all\".",
            min, max
        )),
    }
}

/// Require a track argument to be a number in the range 1-40 or "all".
fn optional_track_validator(v: String) -> Result<(), String> {
    optional_u8_validator(v, 1, 40)
}

fn optional_track_parser(v: String) -> Option<u8> {
    optional_u8_parser(v, 1, 40).unwrap()
}

/// Require a sector argument to be a number in the range 0-20 or "all".
fn optional_sector_validator(v: String) -> Result<(), String> {
    optional_u8_validator(v, 0, 20)
}

fn optional_sector_parser(v: String) -> Option<u8> {
    optional_u8_parser(v, 0, 20).unwrap()
}

/// A FillReader is simply a reader that supplies a certain number of the
/// provided fill byte.
struct FillReader {
    fill_byte: u8,
    size: usize,
}

impl FillReader {
    fn new(fill_byte: u8, size: usize) -> FillReader {
        FillReader { fill_byte, size }
    }
}

impl Read for FillReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_to_write = buf.len().min(self.size);
        for b in &mut buf[..bytes_to_write] {
            *b = self.fill_byte;
        }
        self.size -= bytes_to_write;
        Ok(bytes_to_write)
    }
}

fn cmd_bam_show(diskimage: &str) -> io::Result<()> {
    let disk = disk::open(diskimage, false)?;
    let bam = disk.bam()?;
    print!("{:?}", bam.borrow());
    Ok(())
}

fn cmd_bam_edit(
    diskimage: &str,
    track: Option<u8>,
    sector: Option<u8>,
    allocate: bool,
) -> io::Result<()> {
    let disk = disk::open(diskimage, true)?;
    let format = disk.disk_format()?.clone();
    let bam = disk.bam()?;
    let mut bam = bam.borrow_mut();
    let track_range = match track {
        Some(t) => t..=t,
        None => 1..=35,
    };
    for track in track_range {
        let entry = bam.entry_mut(track)?;
        let sector_range = match sector {
            Some(s) => s..=s,
            None => 0..=(format.sectors_in_track(track) - 1),
        };
        for sector in sector_range {
            if allocate {
                entry.allocate(sector);
            } else {
                entry.free(sector);
            }
        }
    }
    // Write the updated BAM
    bam.flush()?;

    Ok(())
}

/// Open an existing CBM file from the specified disk image for reading.
fn open_cbm_file(diskimage: &str, filename: &str) -> io::Result<File> {
    // Open the CBM file
    let disk = disk::open(diskimage, false)?;
    Ok(disk.open_file(&filename.into())?)
}

/// Open a file on a CBM disk image for reading.
fn open_cbm_reader(diskimage: &str, filename: &str) -> io::Result<Box<Read>> {
    Ok(Box::new(open_cbm_file(diskimage, filename)?.reader()?))
}

/// Open a file on a CBM disk image for appending.
fn open_cbm_appender(diskimage: &str, filename: &str) -> io::Result<Box<Write>> {
    Ok(Box::new(open_cbm_file(diskimage, filename)?.writer()?))
}

/// Open an existing GEOS file from the specified disk image for reading.
fn open_geos_file(diskimage: &str, filename: &str) -> io::Result<GEOSFile> {
    // Open the GEOS file
    let disk = disk::open(diskimage, false)?;
    let file = disk.open_file(&filename.into())?;
    let file = match file {
        File::GEOSVLIR(f) => f,
        File::GEOSSequential(f) => f,
        _ => unreachable!(),
    };
    Ok(file)
}

/// Open a GEOS file for reading.
fn open_geos_reader(diskimage: &str, filename: &str) -> io::Result<Box<Read>> {
    Ok(Box::new(open_geos_file(diskimage, filename)?.reader()?))
}

/// Open a file on a CBM disk image for writing.
fn open_cbm_writer(diskimage: &str, filename: &str, file_type: FileType) -> io::Result<Box<Write>> {
    let mut disk = disk::open(diskimage, true)?;
    let file = disk.create_file(&filename.into(), file_type, Scheme::Linear)?;
    let file = match file {
        File::Linear(f) => f,
        _ => unreachable!(),
    };
    let writer = file.writer()?;
    Ok(Box::new(writer))
}

/// Open a file for reading
fn open_fs_reader(filename: &str) -> io::Result<Box<Read>> {
    if filename == STDINOUT_PSEUDOFILENAME {
        Ok(Box::new(io::stdin()))
    } else {
        Ok(Box::new(fs::File::open(filename)?))
    }
}

/// Open a file for writing
fn open_fs_writer(filename: &str) -> io::Result<Box<Write>> {
    if filename == STDINOUT_PSEUDOFILENAME {
        Ok(Box::new(io::stdout()))
    } else {
        Ok(Box::new(fs::File::create(filename)?))
    }
}

fn cmd_read(
    diskimage: &str,
    source_filename: &str,
    destination_filename: Option<&str>,
) -> io::Result<()> {
    let destination_filename = destination_filename.unwrap_or(source_filename);
    let mut reader = open_cbm_reader(diskimage, source_filename)?;
    let mut writer = open_fs_writer(destination_filename)?;
    io::copy(&mut reader, &mut writer)?;
    writer.flush()?;
    Ok(())
}

fn cmd_write(
    diskimage: &str,
    source_filename: &str,
    destination_filename: Option<&str>,
    file_type: FileType,
) -> io::Result<()> {
    let destination_filename = destination_filename.unwrap_or(source_filename);
    let mut reader = open_fs_reader(source_filename)?;
    let mut writer = open_cbm_writer(diskimage, destination_filename, file_type)?;
    io::copy(&mut reader, &mut writer)?;
    writer.flush()?;
    Ok(())
}

fn cmd_append(
    diskimage: &str,
    source_filename: &str,
    destination_filename: Option<&str>,
) -> io::Result<()> {
    let destination_filename = destination_filename.unwrap_or(source_filename);
    let mut reader = open_fs_reader(source_filename)?;
    let mut writer = open_cbm_appender(diskimage, destination_filename)?;
    io::copy(&mut reader, &mut writer)?;
    writer.flush()?;
    Ok(())
}

fn cmd_geosread(
    diskimage: &str,
    source_filename: &str,
    destination_filename: Option<&str>,
) -> io::Result<()> {
    let destination_filename = destination_filename.unwrap_or(source_filename);
    let mut reader = open_geos_reader(diskimage, source_filename)?;
    let mut writer = open_fs_writer(destination_filename)?;
    io::copy(&mut reader, &mut writer)?;
    writer.flush()?;
    Ok(())
}

fn cmd_create(diskimage: &str) -> io::Result<()> {
    // Determine what kind of disk image to create based on the file extension.
    let geometry = match DiskType::from_extension(diskimage) {
        Some(DiskType::D64) => D64::geometry(false),
        Some(DiskType::D71) => D71::geometry(false),
        Some(DiskType::D81) => D81::geometry(false),
        None => {
            println!("Unknown file extension.  Assuming D64...");
            D64::geometry(false)
        }
    };

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(diskimage)?;
    io::copy(&mut FillReader::new(0x00, geometry.size()), &mut file)?;
    file.flush()?;
    Ok(())
}

fn cmd_dir(diskimage: &str, verbosity: u64) -> io::Result<()> {
    let disk = disk::open(diskimage, false)?;
    println!("{}", disk);
    for entry in disk.iter() {
        if verbosity > 0 {
            let entry = entry?;
            println!("{:#}", entry);
            if verbosity > 1 {
                let file = disk.open_file(&entry.filename)?;
                file.details(&mut io::stdout(), (verbosity as usize) - 2)?;
                println!();
            }
        } else {
            println!("{}", entry?);
        }
    }
    let bam = disk.bam()?;
    let bam = bam.borrow();
    println!("{} blocks free.", bam.blocks_free());
    Ok(())
}

fn cmd_format(diskimage: &str, name: &str, id: &str) -> io::Result<()> {
    let id: Petscii = id.into();
    let id_bytes: &[u8] = id.as_bytes();

    let mut disk = disk::open(diskimage, true)?;
    disk.write_format(&name.into(), &id_bytes.into())?;
    Ok(())
}

fn cmd_rename(diskimage: &str, original_filename: &str, new_filename: &str) -> io::Result<()> {
    let mut disk = disk::open(diskimage, true)?;
    disk.rename(&original_filename.into(), &new_filename.into())
}

fn cmd_delete(diskimage: &str, filename: &str) -> io::Result<()> {
    let disk = disk::open(diskimage, true)?;
    let mut file = disk.open_file(&filename.into())?;
    file.delete()
}

fn cmd_dump(diskimage: &str, filename: Option<&str>) -> io::Result<()> {
    let mut disk = disk::open(diskimage, true)?;
    match filename {
        Some(filename) => {
            let file = disk.open_file(&filename.into())?;
            file.dump(&mut io::stdout())?;
        }
        None => disk.dump(&mut io::stdout())?,
    }
    io::stdout().flush()?;
    Ok(())
}

fn cmd_validate(diskimage: &str) -> io::Result<()> {
    let disk = disk::open(diskimage, true)?;
    let errors = disk.validate()?;
    for e in errors.iter() {
        println!("{}", e);
    }
    if errors.is_empty() {
        println!("Disk validates successfully.");
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!("{} errors found during validation.", errors.len()),
        ))
    }
}
