use crate::prelude::*;
use bytes::Bytes;
use futures::io::ErrorKind;
use log::*;
use std::fmt::{Debug, Formatter, Error};
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

pub trait VolumeStorage {
    fn load(&self) -> GzippedData;
    fn change<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>),
        Self: Sized;
}

#[derive(Debug)]
pub enum Volume {
    Open {
        storage: MemStorage,
        ty: EntityType,
        path: String,
    },
    Closed(GzChunkStorage),
}

impl Volume {
    pub(super) fn new_open(ty: EntityType, path: String) -> Self {
        Volume::Open {
            storage: MemStorage::new(),
            ty,
            path,
        }
    }

    pub(super) fn new_closed(ty: EntityType, path: String) -> Self {
        Volume::Closed(GzChunkStorage::new(ty, path))
    }

    pub fn close(self) -> Self {
        match &self {
            Volume::Open { storage, path, ty } => {
                debug!("Closing storage with path {:?}", path);

                Volume::Closed(GzChunkStorage::new_initialized(
                    *ty,
                    path.clone(),
                    storage.load(),
                ))
            }
            Volume::Closed(_) => {
                warn!("Trying to close Gz storage");
                self
            }
        }
    }
}

impl VolumeStorage for Volume {
    fn load(&self) -> GzippedData {
        match &self {
            Volume::Open { storage, .. } => storage.load(),
            Volume::Closed(storage) => storage.load(),
        }
    }

    fn change<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>),
        Self: Sized,
    {
        match self {
            Volume::Open { storage, .. } => storage.change(f),
            Volume::Closed(storage) => storage.change(f),
        }
    }
}

pub struct MemStorage {
    inner: BTreeMap<EntityId, SerializedEntity>,
}

impl MemStorage {
    fn new() -> Self {
        MemStorage {
            inner: BTreeMap::new(),
        }
    }

    fn len(&self) -> usize {
        let data_len: usize = self.inner.values().map(|e| e.data.len()).sum();

        let nl_len = self.inner.len();

        data_len + nl_len
    }
}

impl Debug for MemStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_str(&format!("MemStorage(len={})", self.len()))
            .unwrap();
        Ok(())
    }
}

impl VolumeStorage for MemStorage {
    fn load(&self) -> GzippedData {
        let data = self
            .inner
            .values()
            .map(|e| &e.data[..])
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";

        GzippedData::compress(&data)
    }

    fn change<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>),
        Self: Sized,
    {
        f(&mut self.inner);

        self.len()
    }
}

#[derive(Debug)]
pub struct GzChunkStorage {
    ty: EntityType,
    path: String,
}

impl GzChunkStorage {
    pub(super) fn new(ty: EntityType, path: String) -> Self {
        GzChunkStorage { ty, path }
    }

    pub(super) fn new_initialized(ty: EntityType, path: String, data: GzippedData) -> Self {
        let s = GzChunkStorage {
            ty,
            path,
        };
        s.store(data);
        s
    }

    fn file_path(&self) -> &Path {
        self.path.as_ref()
    }

    fn tmp_file_path(&self) -> impl AsRef<Path> {
        let x: &Path = self.path.as_ref();
        let x1: &'static str = "tmp.zst";
        x.with_extension(x1)
    }

    pub fn load(&self) -> GzippedData {
        use std::fs;

        let file_path = self.file_path();

        trace!("Reading a file '{}'", file_path.display());

        let read_result: io::Result<_> = fs::read(file_path);

        match read_result {
            Ok(data) => GzippedData::from_binary(data),
            Err(err) => match err.kind() {
                ErrorKind::NotFound => GzippedData::compress(""),
                _ => panic!("Failed to read file '{}': {:?}", file_path.display(), err),
            },
        }
    }

    fn store(&self, data: GzippedData) {
        use std::fs;

        let dir_path = self
            .file_path()
            .parent()
            .expect("Expected file with parent");

        {
            trace!("Writing a file '{:?}' len={}", self.file_path(), data.len());
            fs::create_dir_all(dir_path)
                .unwrap_or_else(|_| panic!("Failed to create directory '{:?}'", &dir_path));
            fs::write(self.tmp_file_path(), data).unwrap_or_else(|e| {
                panic!(
                    "Writing file '{}' failed: {:?}",
                    self.tmp_file_path().as_ref().display(),
                    e
                )
            });

            fs::rename(self.tmp_file_path(), self.file_path()).unwrap_or_else(|e| {
                panic!(
                    "Failed to move tmp file '{}' to '{}': {:?}",
                    self.tmp_file_path().as_ref().display(),
                    self.file_path().display(),
                    e
                )
            })
        }
    }
}

impl VolumeStorage for GzChunkStorage {
    fn load(&self) -> GzippedData {
        self.load()
    }

    fn change<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>),
    {
        let (data, raw_size) = self.load().change(self.ty, f);
        self.store(data);
        raw_size
    }
}

pub struct GzippedData {
    inner: Vec<u8>,
}

impl CompressedData for GzippedData {
    fn compress(data: &str) -> GzippedData {
        use std::io::Write;

        let mut e = zstd::Encoder::new(Vec::with_capacity(data.len() / 6), 0).unwrap();
        e.write_all(data.as_bytes()).unwrap();

        GzippedData {
            inner: e.finish().unwrap(),
        }
    }

    fn decompress(&self) -> String {
        use std::io::Read;

        let mut d = zstd::Decoder::new(&self.inner[..]).unwrap();
        let mut s = String::with_capacity(self.inner.len() * 5);
        d.read_to_string(&mut s).expect("Incorrect GZip format");
        s
    }
    fn from_binary(data: Vec<u8>) -> GzippedData {
        GzippedData { inner: data }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn into_bytes(self) -> Bytes {
        Bytes::from(self.inner)
    }
}

pub trait CompressedData {
    fn compress(data: &str) -> Self;

    fn decompress(&self) -> String;

    fn change<F>(&self, ty: EntityType, f: F) -> (Self, usize)
        where
            F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>),
            Self: Sized
    {
        use measure_time::*;
        // 1152ms for the function with raw data len=22202461 and 1291 entities

        debug_time!("Total time for changing gzipped {:?} chunk", ty);
        let mut entities = {
            self.decompress()
                .split('\n')
                .filter(|l| !l.is_empty())
                .map(|e: &str| {
                    #[derive(serde::Deserialize)]
                    struct Entity {
                        id: String,
                        lastrevid: u64,
                    }

                    let entity = serde_json::from_str::<Entity>(e).expect("Failed to unserialize");
                    let id = ty.parse_id(&entity.id).unwrap();
                    let revision = RevisionId(entity.lastrevid);

                    SerializedEntity {
                        id,
                        revision,
                        data: e.to_owned(),
                    }
                })
                .map(|e| (e.id, e))
                .collect::<BTreeMap<EntityId, SerializedEntity>>()
        };

        f(&mut entities);

        let entities = entities
            .values()
            .map(|e| {
                let SerializedEntity { data, .. } = e;
                &data[..]
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";

        let raw_size = entities.len();

        (Self::compress(&entities), raw_size)
    }

    fn from_binary(data: Vec<u8>) -> Self;

     fn len(&self) -> usize;

     fn into_bytes(self) -> Bytes;
}

impl AsRef<[u8]> for GzippedData {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}
