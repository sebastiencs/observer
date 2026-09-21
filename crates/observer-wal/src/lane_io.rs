use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};

/// How a lane file is opened. Modes match the existing `OpenOptions` combinations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenMode {
    Read,
    /// Read and write, creating the file if it is absent. Does not truncate.
    ReadWriteCreate,
    /// Read and write a file that must not already exist.
    ReadWriteCreateNew,
    /// Write, creating or truncating.
    WriteTruncate,
}

/// One open file inside a WAL lane.
pub(crate) trait LaneFile: Send {
    fn seek(&mut self, pos: u64) -> io::Result<()>;
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()>;
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize>;
    fn len(&self) -> io::Result<u64>;
    fn set_len(&mut self, len: u64) -> io::Result<()>;
    fn sync_data(&mut self) -> io::Result<()>;
}

/// Filesystem operations scoped to one `lane-0000` directory.
pub(crate) trait LaneIo: Send + Sync {
    fn ensure_dir(&self) -> io::Result<()>;
    fn sync_dir(&self) -> io::Result<()>;
    fn exists(&self, name: &str) -> io::Result<bool>;
    fn list_files(&self) -> io::Result<Vec<String>>;
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>>;
    fn file_len(&self, name: &str) -> io::Result<u64>;
    fn rename(&self, from: &str, to: &str) -> io::Result<()>;
    fn remove_file(&self, name: &str) -> io::Result<()>;
    fn open(&self, name: &str, mode: OpenMode) -> io::Result<Box<dyn LaneFile>>;
}

pub(crate) type SharedLane = Arc<dyn LaneIo>;

/// Production backend: the real lane directory.
pub(crate) struct StdLaneIo {
    lane_dir: PathBuf,
}

impl StdLaneIo {
    pub(crate) fn new(lane_dir: impl Into<PathBuf>) -> Self {
        Self {
            lane_dir: lane_dir.into(),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.lane_dir.join(name)
    }
}

impl LaneIo for StdLaneIo {
    fn ensure_dir(&self) -> io::Result<()> {
        fs::create_dir_all(&self.lane_dir)
    }

    fn sync_dir(&self) -> io::Result<()> {
        File::open(&self.lane_dir)?.sync_all()
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        Ok(self.path(name).exists())
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        if !self.lane_dir.exists() {
            return Ok(Vec::new());
        }
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.lane_dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-utf8 segment file name",
                ));
            };
            names.push(name.to_owned());
        }
        Ok(names)
    }

    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        fs::read(self.path(name))
    }

    fn file_len(&self, name: &str) -> io::Result<u64> {
        Ok(fs::metadata(self.path(name))?.len())
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        fs::rename(self.path(from), self.path(to))
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        fs::remove_file(self.path(name))
    }

    fn open(&self, name: &str, mode: OpenMode) -> io::Result<Box<dyn LaneFile>> {
        let mut options = OpenOptions::new();
        match mode {
            OpenMode::Read => {
                options.read(true);
            }
            OpenMode::ReadWriteCreate => {
                options.read(true).write(true).create(true);
            }
            OpenMode::ReadWriteCreateNew => {
                options.read(true).write(true).create_new(true);
            }
            OpenMode::WriteTruncate => {
                options.write(true).create(true).truncate(true);
            }
        }
        let file = options.open(self.path(name))?;
        Ok(Box::new(StdLaneFile { file }))
    }
}

struct StdLaneFile {
    file: File,
}

impl LaneFile for StdLaneFile {
    fn seek(&mut self, pos: u64) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.file.write_all(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.file.read_exact(buf)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.file.read_to_end(buf)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}
