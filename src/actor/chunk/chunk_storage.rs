use crate::actor::SerializedEntity;
use crate::prelude::{EntityId, EntityType, RevisionId};
use actix_web::web::Bytes;
use log::*;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

pub trait ChunkStorage {
    fn load(&self) -> GzippedData;
    fn change<F>(&mut self, f: F)  -> usize
    where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>) -> (),
        Self: Sized;
}

struct MemStorage {
    inner: BTreeMap<EntityId, SerializedEntity>
}

impl ChunkStorage for MemStorage {
    fn load(&self) -> GzippedData {
        let data = self.inner.iter().map(|(_, e)| {
            &e.data[..]
        }).collect::<Vec<_>>().join("\n") + "\n";

        GzippedData::compress(&data)
    }

    fn change<F>(&mut self, f: F) -> usize where
        F: FnOnce(&mut BTreeMap<EntityId, SerializedEntity>) -> (),
        Self: Sized {
        f(&mut self.inner);

        let data_len: usize = self.inner.iter().map(|(_, e)| {
            e.data.len()
        }).sum();

        let nl_len = self.inner.len();

        data_len + nl_len
    }
}

pub struct GzChunkStorage<P: AsRef<Path>> {
    ty: EntityType,
    path: P,
}

impl<P: AsRef<Path>> GzChunkStorage<P> {
    pub fn new(ty: EntityType, path: P) -> Self {
        GzChunkStorage { ty, path }
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

impl<P: AsRef<Path>> ChunkStorage for GzChunkStorage<P> {
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
            .iter()
            .map(|(_, e)| {
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
