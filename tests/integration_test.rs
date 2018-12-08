use std::fmt;
use std::io;

use cbm::disk::directory::FileType;
use cbm::disk::file::{FileOps, Scheme};
use cbm::disk::{D64, D71, D81, Disk, DiskError, DiskType, Id};
use cbm::Petscii;
use rand::{Rng, XorShiftRng};

const ITERATIONS: usize = 100;
const MIN_FILE_SIZE: usize = 0;
const MAX_FILE_SIZE: usize = 64 * 1024;
const MAX_ITERATIONS_PER_IMAGE: usize = 10_000;
const DELETE_CHANCE: f32 = 0.33;
const ADD_CHANCE: f32 = 0.66;
const RNG_SEED: [u8; 16] = [
    0x04, 0xC1, 0x1D, 0xB7, 0x1E, 0xDC, 0x6F, 0x41, 0x74, 0x1B, 0x8C, 0xD7, 0x32, 0x58, 0x34, 0x99,
];

const CONTENT_BYTES_PER_BLOCK: usize = 254;

static DISK_TYPES: &[DiskType] = &[DiskType::D64, DiskType::D71, DiskType::D81];

fn deterministic_rng() -> XorShiftRng {
    rand::SeedableRng::from_seed(RNG_SEED)
}

fn random_name(rng: &mut impl Rng) -> Petscii {
    const MIN_NAME_SIZE: usize = 1;
    const MAX_NAME_SIZE: usize = 16;
    let name_size = rng.gen_range(MIN_NAME_SIZE, MAX_NAME_SIZE + 1);
    let mut bytes = vec![0u8; name_size];
    rng.fill(&mut bytes[..]);
    Petscii::from_bytes(&bytes)
}

fn random_available_name(rng: &mut impl Rng, disk: &Box<Disk>) -> Petscii {
    loop {
        let name = random_name(rng);

        // Filenames can't end with 0xA0 since the field is padded with 0xA0 bytes.
        if name.as_bytes()[name.len() - 1] == 0xA0 {
            continue;
        }

        match disk.check_filename_availability(&name) {
            Ok(_) => return name,
            Err(ref e) => match DiskError::from_io_error(&e) {
                Some(ref e) if *e == DiskError::FileExists => {}
                Some(_) | None => panic!("cannot check filename availability: {}", e),
            },
        }
    }
}

fn random_id(rng: &mut impl Rng) -> Id {
    const ID_SIZE: usize = 2;
    let mut bytes = [0u8; ID_SIZE];
    rng.fill(&mut bytes);
    Id::from_bytes(&bytes)
}

fn random_file_type(rng: &mut impl Rng) -> FileType {
    static LINEAR_FILE_TYPES: &[FileType] = &[FileType::PRG, FileType::SEQ, FileType::USR];
    LINEAR_FILE_TYPES[rng.gen_range(0, LINEAR_FILE_TYPES.len())]
}

fn new_disk(mut rng: &mut impl Rng, disk_type: &DiskType) -> Box<Disk> {
    let name = random_name(&mut rng);
    let id = random_id(&mut rng);
    match disk_type {
        DiskType::D64 => {
            let mut d64 = D64::open_memory(D64::geometry(false)).unwrap();
            d64.write_format(&name, &id).unwrap();
            Box::new(d64)
        }
        DiskType::D71 => {
            let mut d71 = D71::open_memory(D71::geometry(false)).unwrap();
            d71.write_format(&name, &id).unwrap();
            Box::new(d71)
        }
        DiskType::D81 => {
            let mut d81 = D81::open_memory(D81::geometry(false)).unwrap();
            d81.write_format(&name, &id).unwrap();
            Box::new(d81)
        }
    }
}

struct RandomFile {
    name: Petscii,
    size: usize,
    file_type: FileType,
    contents: Vec<u8>,
}

impl RandomFile {
    fn new(mut rng: &mut XorShiftRng, disk: &Box<Disk>) -> RandomFile {
        let name = random_available_name(&mut rng, &disk);
        let size: usize = rng.gen_range(MIN_FILE_SIZE, MAX_FILE_SIZE);
        let file_type = random_file_type(&mut rng);
        let mut contents = vec![0u8; size];
        rng.fill(&mut contents[..]);
        RandomFile {
            name,
            size,
            file_type,
            contents,
        }
    }

    fn blocks(&self) -> usize {
        (self.size + CONTENT_BYTES_PER_BLOCK - 1) / CONTENT_BYTES_PER_BLOCK
    }

    fn write(&self, disk: &mut Box<Disk>) -> io::Result<()> {
        let file = disk.create_file(&self.name, self.file_type, Scheme::Linear)?;
        let mut writer = file.writer()?;
        writer.write_all(&self.contents)?;
        writer.flush()?;
        Ok(())
    }

    fn verify(&self, disk: &Box<Disk>) -> io::Result<()> {
        // Read file.
        let file = disk.open_file(&self.name)?;
        let mut reader = file.reader()?;
        let mut read_contents = Vec::new();
        reader.read_to_end(&mut read_contents)?;
        assert_eq!(self.contents, read_contents);

        // Check directory entry.
        let entry = disk.find_directory_entry(&self.name)?;
        assert_eq!(entry.filename, self.name);
        assert_eq!(entry.file_size, self.blocks() as u16);
        assert_eq!(entry.file_attributes.file_type, self.file_type);
        assert_eq!(entry.file_attributes.locked_flag, false);
        assert_eq!(entry.file_attributes.closed_flag, true);

        Ok(())
    }
}

impl fmt::Debug for RandomFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RandomFile {{ name: {:?}, size: {}, file_type: {} }}",
            self.name, self.size, self.file_type
        )
    }
}

fn verify_disk_state(disk: &Box<Disk>, files: &Vec<RandomFile>) -> io::Result<()> {
    // Validate disk image
    disk.validate().unwrap();
    // Confirm blocks free
    let total_data_blocks = disk.disk_format()?.total_data_blocks();
    let blocks_written: usize = files.iter().map(|f| f.blocks()).sum();
    let expected_blocks_free = total_data_blocks - blocks_written;
    assert_eq!(disk.blocks_free().unwrap(), expected_blocks_free);
    Ok(())
}

#[test]
#[ignore]
fn integration_test() {
    let mut rng = deterministic_rng();

    for i in 0..ITERATIONS {
        for disk_type in DISK_TYPES {
            println!("Iteration: {} disk type: {:?}", i, disk_type);

            let mut disk = new_disk(&mut rng, disk_type);
            assert!(disk.directory().unwrap().is_empty());
            assert_eq!(
                disk.blocks_free().unwrap(),
                disk.disk_format().unwrap().total_data_blocks()
            );
            let mut written_files = vec![];
            let mut disk_full = false;
            for _i in 0..MAX_ITERATIONS_PER_IMAGE {
                // Randomly add files
                if rng.gen::<f32>() < ADD_CHANCE {
                    let random_file = RandomFile::new(&mut rng, &disk);
                    // println!("Add: {:?}", random_file);
                    match random_file.write(&mut disk) {
                        Ok(_) => {}
                        Err(ref e) if e == &DiskError::DiskFull => {
                            // Confirm the legitimacy of this "disk full" error
                            let entries = disk.iter().count();
                            let free = disk.blocks_free().unwrap();
                            if entries != disk.disk_format().unwrap().max_directory_entries()
                                && free > random_file.size
                            {
                                panic!("Disk full unexpectedly.");
                            }

                            // Remove the underwritten file
                            match disk.open_file(&random_file.name) {
                                Ok(mut f) => f.delete().unwrap(),
                                Err(ref e) if e == &DiskError::NotFound => {},
                                Err(ref e) => panic!("error opening underwritten file: {}", e),
                            }

                            // Conclude the test of this disk image.
                            disk_full = true;
                            break;
                        }
                        Err(ref e) => panic!("error writing file: {}", e),
                    };
                    random_file.verify(&disk).unwrap();
                    written_files.push(random_file);

                    verify_disk_state(&disk, &written_files).unwrap();
                }

                // Randomly delete files
                if rng.gen::<f32>() < DELETE_CHANCE {
                    if !written_files.is_empty() {
                        let target_index = rng.gen_range(0, written_files.len());
                        let target = written_files.remove(target_index);
                        // println!("Delete: {:?}", target);
                        let mut file = disk.open_file(&target.name).unwrap();
                        file.delete().unwrap();
                    }

                    verify_disk_state(&disk, &written_files).unwrap();
                }
            }
            assert!(disk_full);

            // Re-verify all remaining files.
            written_files.iter().for_each(|f| {
                f.verify(&disk).unwrap();
            });

            // Delete all remaining files
            written_files.drain(..).for_each(|f| {
                let mut file = disk.open_file(&f.name).unwrap();
                file.delete().unwrap();
            });
            verify_disk_state(&disk, &written_files).unwrap();
        }
    }
}
