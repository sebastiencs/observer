use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::{Arc, Mutex},
};

use crate::lane_io::{LaneFile, LaneIo, OpenMode, SharedLane};

/// In-memory lane whose durable image advances only at `sync_data` and directory fsync.
///
/// A crash restores the volatile image from that durable image and invalidates open handles.
#[derive(Clone)]
pub(crate) struct CrashFs {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    next_inode: u64,
    generation: u64,
    inodes: BTreeMap<u64, Inode>,
    volatile: BTreeMap<String, u64>,
    durable: BTreeMap<String, u64>,
    journal: Vec<String>,
    mutation: u64,
    crash_after: Option<u64>,
}

struct Inode {
    volatile: Vec<u8>,
    durable: Vec<u8>,
}

struct CrashFile {
    fs: CrashFs,
    inode: u64,
    generation: u64,
    pos: u64,
}

impl CrashFs {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                next_inode: 1,
                generation: 1,
                inodes: BTreeMap::new(),
                volatile: BTreeMap::new(),
                durable: BTreeMap::new(),
                journal: Vec::new(),
                mutation: 0,
                crash_after: None,
            })),
        }
    }

    pub(crate) fn shared(&self) -> SharedLane {
        Arc::new(self.clone())
    }

    pub(crate) fn arm_crash_after(&self, step: u64) {
        self.inner.lock().expect("crash fs").crash_after = Some(step);
    }

    pub(crate) fn disarm(&self) {
        self.inner.lock().expect("crash fs").crash_after = None;
    }

    pub(crate) fn mutations(&self) -> u64 {
        self.inner.lock().expect("crash fs").mutation
    }

    /// Discard unsynced bytes and directory entries that were not fsynced.
    pub(crate) fn crash(&self) {
        let mut inner = self.inner.lock().expect("crash fs");
        inner.generation = inner.generation.wrapping_add(1);
        inner.volatile = inner.durable.clone();
        let live: BTreeSet<u64> = inner.durable.values().copied().collect();
        inner.inodes.retain(|id, inode| {
            if live.contains(id) {
                inode.volatile = inode.durable.clone();
                true
            } else {
                false
            }
        });
        inner.crash_after = None;
    }

    pub(crate) fn dump(&self) -> String {
        let inner = self.inner.lock().expect("crash fs");
        let mut out = format!("mutations={}\n", inner.mutation);
        for entry in &inner.journal {
            out.push_str(entry);
            out.push('\n');
        }
        out.push_str("durable:\n");
        for (name, id) in &inner.durable {
            let len = inner
                .inodes
                .get(id)
                .map(|inode| inode.durable.len())
                .unwrap_or(0);
            out.push_str(&format!("  {name} inode={id} len={len}\n"));
        }
        out
    }
}

impl LaneIo for CrashFs {
    fn ensure_dir(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_dir(&self) -> io::Result<()> {
        let mut inner = self.inner.lock().expect("crash fs");
        inner.durable = inner.volatile.clone();
        gc(&mut inner);
        inner.note("dir-sync")
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        let inner = self.inner.lock().expect("crash fs");
        Ok(inner.volatile.contains_key(name))
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        let inner = self.inner.lock().expect("crash fs");
        Ok(inner.volatile.keys().cloned().collect())
    }

    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        let inner = self.inner.lock().expect("crash fs");
        let id = lookup(&inner, name)?;
        Ok(inner.inodes[&id].volatile.clone())
    }

    fn file_len(&self, name: &str) -> io::Result<u64> {
        let inner = self.inner.lock().expect("crash fs");
        let id = lookup(&inner, name)?;
        Ok(u64::try_from(inner.inodes[&id].volatile.len()).expect("len"))
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut inner = self.inner.lock().expect("crash fs");
        let id = inner
            .volatile
            .remove(from)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing file"))?;
        inner.volatile.insert(to.to_owned(), id);
        inner.note(format!("rename {from} -> {to}"))
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        let mut inner = self.inner.lock().expect("crash fs");
        if inner.volatile.remove(name).is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "missing file"));
        }
        inner.note(format!("unlink {name}"))
    }

    fn open(&self, name: &str, mode: OpenMode) -> io::Result<Box<dyn LaneFile>> {
        let mut inner = self.inner.lock().expect("crash fs");
        let generation = inner.generation;
        let inode = match mode {
            OpenMode::Read => lookup(&inner, name)?,
            OpenMode::ReadWriteCreate => {
                if let Some(id) = inner.volatile.get(name).copied() {
                    id
                } else {
                    create_file(&mut inner, name)?
                }
            }
            OpenMode::ReadWriteCreateNew => {
                if inner.volatile.contains_key(name) {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "file already exists",
                    ));
                }
                create_file(&mut inner, name)?
            }
            OpenMode::WriteTruncate => {
                if inner.volatile.contains_key(name) {
                    let id = inner.volatile[name];
                    inner.inodes.get_mut(&id).expect("inode").volatile.clear();
                    inner.note(format!("truncate {name}"))?;
                    id
                } else {
                    create_file(&mut inner, name)?
                }
            }
        };
        Ok(Box::new(CrashFile {
            fs: self.clone(),
            inode,
            generation,
            pos: 0,
        }))
    }
}

impl Inner {
    fn note(&mut self, entry: impl AsRef<str>) -> io::Result<()> {
        self.mutation += 1;
        self.journal
            .push(format!("{}: {}", self.mutation, entry.as_ref()));
        if self.crash_after == Some(self.mutation) {
            Err(io::Error::other("injected crash"))
        } else {
            Ok(())
        }
    }
}

fn lookup(inner: &Inner, name: &str) -> io::Result<u64> {
    inner
        .volatile
        .get(name)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing file"))
}

fn create_file(inner: &mut Inner, name: &str) -> io::Result<u64> {
    let id = inner.next_inode;
    inner.next_inode += 1;
    inner.inodes.insert(
        id,
        Inode {
            volatile: Vec::new(),
            durable: Vec::new(),
        },
    );
    inner.volatile.insert(name.to_owned(), id);
    inner.note(format!("create {name}"))?;
    Ok(id)
}

fn gc(inner: &mut Inner) {
    let mut live = BTreeSet::new();
    live.extend(inner.volatile.values().copied());
    live.extend(inner.durable.values().copied());
    inner.inodes.retain(|id, _| live.contains(id));
}

impl CrashFile {
    fn with_inner<T>(&self, body: impl FnOnce(&Inner) -> io::Result<T>) -> io::Result<T> {
        let inner = self.fs.inner.lock().expect("crash fs");
        self.ensure_live(&inner)?;
        body(&inner)
    }

    fn with_inner_mut<T>(
        &mut self,
        body: impl FnOnce(&mut Inner) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut inner = self.fs.inner.lock().expect("crash fs");
        self.ensure_live(&inner)?;
        body(&mut inner)
    }

    fn ensure_live(&self, inner: &Inner) -> io::Result<()> {
        if inner.generation != self.generation || !inner.inodes.contains_key(&self.inode) {
            Err(io::Error::other("handle invalidated by crash"))
        } else {
            Ok(())
        }
    }
}

impl LaneFile for CrashFile {
    fn seek(&mut self, pos: u64) -> io::Result<()> {
        self.with_inner(|_| Ok(()))?;
        self.pos = pos;
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let pos = self.pos;
        let inode = self.inode;
        self.with_inner_mut(|inner| {
            let file = inner.inodes.get_mut(&inode).expect("inode");
            let start = usize::try_from(pos).expect("pos");
            let end = start + buf.len();
            if file.volatile.len() < end {
                file.volatile.resize(end, 0);
            }
            file.volatile[start..end].copy_from_slice(buf);
            inner.note(format!("write inode={inode} at={pos} len={}", buf.len()))
        })?;
        self.pos = pos + u64::try_from(buf.len()).expect("len");
        Ok(())
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let pos = self.pos;
        let inode = self.inode;
        self.with_inner(|inner| {
            let file = &inner.inodes[&inode];
            let start = usize::try_from(pos).expect("pos");
            let end = start + buf.len();
            if end > file.volatile.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "read past end of file",
                ));
            }
            buf.copy_from_slice(&file.volatile[start..end]);
            Ok(())
        })?;
        self.pos = pos + u64::try_from(buf.len()).expect("len");
        Ok(())
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let pos = self.pos;
        let inode = self.inode;
        let read = self.with_inner(|inner| {
            let file = &inner.inodes[&inode];
            let start = usize::try_from(pos).expect("pos");
            let bytes = if start >= file.volatile.len() {
                &[][..]
            } else {
                &file.volatile[start..]
            };
            buf.extend_from_slice(bytes);
            Ok(bytes.len())
        })?;
        self.pos = pos + u64::try_from(read).expect("len");
        Ok(read)
    }

    fn len(&self) -> io::Result<u64> {
        let inode = self.inode;
        self.with_inner(
            |inner| Ok(u64::try_from(inner.inodes[&inode].volatile.len()).expect("len")),
        )
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        let inode = self.inode;
        self.with_inner_mut(|inner| {
            let file = inner.inodes.get_mut(&inode).expect("inode");
            file.volatile.resize(usize::try_from(len).expect("len"), 0);
            inner.note(format!("set-len inode={inode} len={len}"))
        })
    }

    fn sync_data(&mut self) -> io::Result<()> {
        let inode = self.inode;
        self.with_inner_mut(|inner| {
            let file = inner.inodes.get_mut(&inode).expect("inode");
            file.durable.clone_from(&file.volatile);
            inner.note(format!("sync-data inode={inode}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsynced_write_disappears_and_synced_write_survives() {
        let fs = CrashFs::new();
        {
            let mut file = fs.open("data", OpenMode::ReadWriteCreate).expect("open");
            file.write_all(b"hello").expect("write");
            fs.crash();
            assert!(fs.read_file("data").is_err());

            let mut file = fs.open("data", OpenMode::ReadWriteCreate).expect("open");
            file.write_all(b"hello").expect("write");
            file.sync_data().expect("sync");
            fs.sync_dir().expect("dir");
            fs.crash();
            assert_eq!(fs.read_file("data").expect("read"), b"hello");
        }
    }

    #[test]
    fn rename_and_unlink_require_directory_sync() {
        let fs = CrashFs::new();
        let mut file = fs.open("old", OpenMode::ReadWriteCreate).expect("open");
        file.write_all(b"v").expect("write");
        file.sync_data().expect("sync");
        fs.sync_dir().expect("dir");
        drop(file);

        fs.rename("old", "new").expect("rename");
        fs.crash();
        assert_eq!(fs.read_file("old").expect("old"), b"v");
        assert!(fs.read_file("new").is_err());

        fs.rename("old", "new").expect("rename");
        fs.sync_dir().expect("dir");
        fs.remove_file("new").expect("unlink");
        fs.crash();
        assert_eq!(fs.read_file("new").expect("still durable"), b"v");

        fs.remove_file("new").expect("unlink");
        fs.sync_dir().expect("dir");
        fs.crash();
        assert!(fs.read_file("new").is_err());
    }

    #[test]
    fn crash_after_stops_on_the_requested_mutation() {
        let fs = CrashFs::new();
        fs.arm_crash_after(1);
        let error = match fs.open("data", OpenMode::ReadWriteCreate) {
            Ok(_) => panic!("expected crash"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(fs.mutations(), 1);
        fs.crash();
        assert!(fs.read_file("data").is_err());
    }
}
