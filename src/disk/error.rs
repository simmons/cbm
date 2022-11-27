use std::error;
use std::fmt;
use std::io;

/// Errors that can be returned from disk image operations.  These are
/// generally converted into `io::Error`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiskError {
    /// Unknown error
    Unknown,
    /// Write access attempted to read-only media
    ReadOnly,
    /// Bad track or sector
    InvalidLocation,
    /// Offset out of bounds
    InvalidOffset,
    /// Invalid header
    InvalidHeader,
    /// Invalid BAM
    InvalidBAM,
    /// Invalid layout
    InvalidLayout,
    /// Record out of bounds
    InvalidRecord,
    /// Read overflow
    ReadOverflow,
    /// Read underrun
    ReadUnderrun,
    /// Write underrun
    WriteUnderrun,
    /// Attempt to use unformatted media
    Unformatted,
    /// File not found
    NotFound,
    /// Chain loop detected
    ChainLoop,
    /// Invalid chain link
    InvalidChainLink,
    /// Invalid relative file layout
    InvalidRelativeFile,
    /// Attempt to write a resource with no embedded position
    Unpositioned,
    /// Filename exceeds maximum length
    FilenameTooLong,
    /// A file with the specified filename already exists
    FileExists,
    /// Disk is full
    DiskFull,
    /// Invalid record index
    InvalidRecordIndex,
    /// Unknown format
    UnknownFormat,
    /// A required GEOS info block was not found.
    GEOSInfoNotFound,
    /// A record exceeded the maximum size.
    RecordTooLarge,
    /// Attempt to linearly access non-linear file.
    NonLinearFile,
}

impl error::Error for DiskError {
    /// Provide terse descriptions of the errors.
    fn description(&self) -> &str {
        self.message()
    }

    /// For errors which encapsulate another error, allow the caller to fetch
    /// the contained error.
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            _ => None,
        }
    }
}

impl fmt::Display for DiskError {
    /// Provide human-readable descriptions of the errors
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.message())
    }
}

impl Into<io::Error> for DiskError {
    fn into(self) -> io::Error {
        use self::DiskError::*;
        use std::io::ErrorKind::*;
        match self {
            Unknown => io::Error::new(Other, self),
            ReadOnly => io::Error::new(Other, self),
            InvalidLocation => io::Error::new(InvalidInput, self),
            InvalidOffset => io::Error::new(InvalidInput, self),
            InvalidHeader => io::Error::new(InvalidData, self),
            InvalidBAM => io::Error::new(InvalidData, self),
            InvalidLayout => io::Error::new(InvalidData, self),
            InvalidRecord => io::Error::new(InvalidInput, self),
            ReadOverflow => io::Error::new(InvalidInput, self),
            ReadUnderrun => io::Error::new(InvalidInput, self),
            WriteUnderrun => io::Error::new(InvalidInput, self),
            Unformatted => io::Error::new(InvalidData, self),
            self::DiskError::NotFound => io::Error::new(io::ErrorKind::NotFound, self),
            ChainLoop => io::Error::new(InvalidData, self),
            InvalidChainLink => io::Error::new(InvalidData, self),
            InvalidRelativeFile => io::Error::new(InvalidData, self),
            Unpositioned => io::Error::new(InvalidInput, self),
            FilenameTooLong => io::Error::new(InvalidInput, self),
            FileExists => io::Error::new(InvalidInput, self),
            DiskFull => io::Error::new(Other, self),
            InvalidRecordIndex => io::Error::new(InvalidInput, self),
            UnknownFormat => io::Error::new(InvalidData, self),
            GEOSInfoNotFound => io::Error::new(InvalidData, self),
            RecordTooLarge => io::Error::new(InvalidData, self),
            NonLinearFile => io::Error::new(InvalidInput, self),
        }
    }
}

impl From<io::Error> for DiskError {
    fn from(error: io::Error) -> DiskError {
        match error.into_inner() {
            Some(e) => match e.downcast_ref::<DiskError>() {
                Some(disk_error) => disk_error.clone(),
                None => DiskError::Unknown,
            },
            None => DiskError::Unknown,
        }
    }
}

impl DiskError {
    /// If the provided `io::Error` contains a `DiskError`, return the
    /// underlying `DiskError`.  If not, return None.
    pub fn from_io_error(error: &io::Error) -> Option<DiskError> {
        match error.get_ref() {
            Some(e) => match e.downcast_ref::<DiskError>() {
                Some(disk_error) => Some(disk_error.clone()),
                None => None,
            },
            None => None,
        }
    }

    /// This is sometimes useful instead of .into() when the compiler doesn't
    /// have enough information to perform type inference.
    /// (Type ascription can't come soon enough.)
    pub fn to_io_error(&self) -> io::Error {
        let io_error: io::Error = self.clone().into();
        io_error
    }

    /// Provide terse descriptions of the errors.
    fn message(&self) -> &str {
        use self::DiskError::*;
        match *self {
            Unknown => "unknown error",
            ReadOnly => "write access attempted to read-only media",
            InvalidLocation => "bad track or sector",
            InvalidOffset => "offset out of bounds",
            InvalidHeader => "invalid header",
            InvalidBAM => "invalid BAM",
            InvalidLayout => "invalid layout",
            InvalidRecord => "record out of bounds",
            ReadOverflow => "read overflow",
            ReadUnderrun => "read underrun",
            WriteUnderrun => "write underrun",
            Unformatted => "attempt to use unformatted media",
            NotFound => "file not found",
            ChainLoop => "chain loop detected",
            InvalidChainLink => "invalid chain link",
            InvalidRelativeFile => "invalid relative file layout",
            Unpositioned => "attempt to write a resource with no embedded position",
            FilenameTooLong => "filename exceeds maximum length",
            FileExists => "a file with the specified filename already exists",
            DiskFull => "disk is full",
            InvalidRecordIndex => "invalid record index",
            UnknownFormat => "unknown format",
            GEOSInfoNotFound => "a required GEOS info block was not found",
            RecordTooLarge => "a record exceeded the maximum size",
            NonLinearFile => "attempt to linearly access non-linear file",
        }
    }
}

impl PartialEq<io::Error> for DiskError {
    fn eq(&self, other: &io::Error) -> bool {
        match DiskError::from_io_error(&other) {
            Some(ref e) if e == self => true,
            _ => false,
        }
    }
}

impl PartialEq<DiskError> for io::Error {
    fn eq(&self, other: &DiskError) -> bool {
        match DiskError::from_io_error(self) {
            Some(ref e) if e == other => true,
            _ => false,
        }
    }
}
