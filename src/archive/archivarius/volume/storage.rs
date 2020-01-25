use crate::archive::SerializedEntity;
use crate::prelude::{EntityId, EntityType, RevisionId};
use bytes::Bytes;
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
pub enum Volume<P: AsRef<Path> + Debug> {
    Open {
        storage: MemStorage,
        ty: EntityType,
        path: P,
    },
    Closed(GzChunkStorage<P>),
}

impl<P: AsRef<Path> + Clone + Debug> Volume<P> {
    pub(super) fn new_open(ty: EntityType, path: P) -> Self {
        Volume::Open {
            storage: MemStorage::new(),
            ty,
            path,
        }
    }

    pub(super) fn new_closed(ty: EntityType, path: P) -> Self {
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

impl<P: AsRef<Path> + Debug> VolumeStorage for Volume<P> {
    fn load(&self) -> GzippedData {
        match &self {
            Volume::Open { storage, .. } => storage.load(),
            Volume::Closed(storage) => storage.load(),
        }
    }

    fn change<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>) -> (),
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
            fs::create_dir_all(dir_path)
                .unwrap_or_else(|_| panic!("Failed to create directory '{:?}'", dir_path));
            // TODO: Maybe use sync_all() here?
            // TODO: Write to tmp file and them move, to reduce the risk of having broken file
            fs::write(self.path.as_ref(), data)
                .unwrap_or_else(|_| panic!("Writing to file '{:?}' failed", self.path.as_ref()));
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
                .split('\n')
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

    pub fn into_bytes(self) -> Bytes {
        Bytes::from(self.inner)
    }
}

impl AsRef<[u8]> for GzippedData {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}
