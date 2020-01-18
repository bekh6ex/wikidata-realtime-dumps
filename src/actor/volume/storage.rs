use crate::actor::SerializedEntity;
use crate::prelude::{EntityId, EntityType, RevisionId};
use actix_web::web::Bytes;
use log::*;
use serde::export::fmt::{Debug, Error};
use serde::export::Formatter;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

pub trait VolumeStorage {
    fn load(&self) -> GzippedData;
    fn change<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>) -> (),
        Self: Sized;
}

#[derive(Debug)]
pub enum ClosableStorage<P: AsRef<Path> + Debug> {
    Mem {
        storage: MemStorage,
        ty: EntityType,
        path: P,
    },
    Gz(GzChunkStorage<P>),
}

impl<P: AsRef<Path> + Clone + Debug> ClosableStorage<P> {
    pub(super) fn new_open(ty: EntityType, path: P) -> Self {
        ClosableStorage::Mem {
            storage: MemStorage::new(),
            ty,
            path,
        }
    }

    pub(super) fn new_closed(ty: EntityType, path: P) -> Self {
        ClosableStorage::Gz(GzChunkStorage::new(ty, path))
    }

    pub fn close(self) -> Self {
        match &self {
            ClosableStorage::Mem { storage, path, ty } => {
                debug!("Closing storage with path {:?}", path);

                ClosableStorage::Gz(GzChunkStorage::new_initialized(
                    *ty,
                    path.clone(),
                    storage.load(),
                ))
            }
            ClosableStorage::Gz(_) => {
                warn!("Trying to close Gz storage");
                self
            }
        }
    }
}

impl<P: AsRef<Path> + Debug> VolumeStorage for ClosableStorage<P> {
    fn load(&self) -> GzippedData {
        match &self {
            ClosableStorage::Mem { storage, .. } => storage.load(),
            ClosableStorage::Gz(storage) => storage.load(),
        }
    }

    fn change<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>) -> (),
        Self: Sized,
    {
        match self {
            ClosableStorage::Mem { storage, .. } => storage.change(f),
            ClosableStorage::Gz(storage) => storage.change(f),
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
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>) -> (),
        Self: Sized,
    {
        f(&mut self.inner);

        self.len()
    }
}

#[derive(Debug)]
pub struct GzChunkStorage<P: AsRef<Path>> {
    ty: EntityType,
    path: P,
}

impl<P: AsRef<Path>> GzChunkStorage<P> {
    pub(super) fn new(ty: EntityType, path: P) -> Self {
        GzChunkStorage { ty, path }
    }

    pub(super) fn new_initialized(ty: EntityType, path: P, data: GzippedData) -> Self {
        let s = GzChunkStorage { ty, path };
        s.store(data);
        s
    }

    fn file_path(&self) -> &Path {
        self.path.as_ref()
    }

    pub fn load(&self) -> GzippedData {
        use std::fs;

        let file_path = self.file_path();
        let file_path1 = self.file_path();

        {
            trace!("Reading a file '{}'", file_path.to_str().unwrap());

            let read_result: io::Result<_> = fs::read(file_path);

            if read_result.is_err() {
                // TODO Match the error
                // Custom { kind: NotFound, error: VerboseError { source: Os { code: 2, kind: NotFound, message: "No such file or directory" }, message: "could not read file `/tmp/wd-rt-dumps/chunk/933.gz`" } }
                info!(
                    "Failed to read a file '{}'. Might be just missing. Error: {:?}",
                    file_path1.to_str().unwrap(),
                    read_result.unwrap_err()
                );
                GzippedData::compress("")
            } else {
                GzippedData::from_binary(read_result.unwrap())
            }
        }
    }

    fn store(&self, data: GzippedData) {
        use std::fs;
        let dir_path = self
            .path
            .as_ref()
            .parent()
            .expect("Expected file with parent");

        {
            trace!(
                "Writing a file '{:?}' len={}",
                self.path.as_ref(),
                data.len()
            );
            fs::create_dir_all(dir_path).expect("Failed to create a directory");
            fs::write(self.path.as_ref(), data).expect("Writing failed");
        }
    }
}

impl<P: AsRef<Path>> VolumeStorage for GzChunkStorage<P> {
    fn load(&self) -> GzippedData {
        self.load()
    }

    fn change<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>) -> (),
    {
        let (data, raw_size) = self.load().change(self.ty, f);
        self.store(data);
        raw_size
    }
}

pub struct GzippedData {
    inner: Vec<u8>,
}

impl GzippedData {
    fn compress(data: &str) -> GzippedData {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::with_capacity(data.len() / 3), Compression::best());
        encoder.write_all(data.as_bytes()).unwrap();

        GzippedData {
            inner: encoder.finish().unwrap(),
        }
    }

    fn decompress(&self) -> String {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut d = GzDecoder::new(&self.inner[..]);
        let mut s = String::with_capacity(self.inner.len() * 3);
        d.read_to_string(&mut s).expect("Incorrect GZip format");
        s
    }

    fn change<F>(&self, ty: EntityType, f: F) -> (Self, usize)
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>) -> (),
    {
        use measure_time::*;
        // 1152ms for the function with raw data len=22202461 and 1291 entities

        debug_time!("Total time for changing gzipped {:?} chunk", ty);
        let mut entities = {
            self.decompress()
                .split("\n")
                .filter(|l| !l.is_empty())
                .map(|e: &str| {
                    #[derive(serde::Deserialize)]
                    struct Entity {
                        id: String,
                        lastrevid: u64,
                    }

                    let entity = serde_json::from_str::<Entity>(&e).expect("Failed to unserialize");
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

    fn from_binary(data: Vec<u8>) -> GzippedData {
        GzippedData { inner: data }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn to_bytes(self) -> Bytes {
        Bytes::from(self.inner)
    }
}

impl AsRef<[u8]> for GzippedData {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}
