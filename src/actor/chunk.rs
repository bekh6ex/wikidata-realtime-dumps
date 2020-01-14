use crate::actor::UpdateChunkCommand;
use actix::{Actor, Context, Handler, Message};
use actix_web::web::Bytes;
use super::SerializedEntity;

use log::*;

use std::io;
use serde_json;
use serde;
use crate::prelude::{EntityType, RevisionId, EntityId};
use std::collections::BTreeMap;

pub struct ChunkActor {
    i: i32,
    dir_path: String,
}

impl ChunkActor {
    pub fn new(i: i32) -> ChunkActor {
        ChunkActor {
            i,
            dir_path: "/tmp/wd-rt-dumps/chunk".to_owned(),
        }
    }

    fn file_path(&self) -> String {
        format!("{}/{}.gz", self.dir_path, self.i)
    }

    fn load(&self) -> GzippedData {
        use std::fs;

        let file_path = self.file_path();
        let file_path1 = self.file_path();

        {
            trace!("Reading a file '{}'", file_path);

            let read_result: io::Result<_> = fs::read(file_path);

            if read_result.is_err() {
                // TODO Match the error
                // Custom { kind: NotFound, error: VerboseError { source: Os { code: 2, kind: NotFound, message: "No such file or directory" }, message: "could not read file `/tmp/wd-rt-dumps/chunk/933.gz`" } }
                info!(
                    "Failed to read a file '{}'. Might be just missing. Error: {:?}",
                    file_path1,
                    read_result.unwrap_err()
                );
                GzippedData::compress("")
            } else {
                GzippedData::from_binary(read_result.unwrap())
            }
        }
    }

    fn store(index: i32, data: GzippedData) {
        use std::fs;
        let dir_path = "/tmp/wd-rt-dumps/chunk";
        let file_path = format!("/tmp/wd-rt-dumps/chunk/{}.gz", index);

        {
            trace!("Writing a file '{}' len={}", file_path, data.len());
            fs::create_dir_all(dir_path).expect("Failed to create a directory");
            fs::write(file_path, data).expect("Writing failed");
        }
    }
}

impl Handler<UpdateChunkCommand> for ChunkActor {
    type Result = Result<(), ()>;

    fn handle(&mut self, msg: UpdateChunkCommand, _ctx: &mut Self::Context) -> Self::Result {
        let thread = {
            let thread1 = std::thread::current();
            thread1.name().unwrap_or("<unknown>").to_owned()
        };

        debug!(
            "thread={} UpdateCommand[actor_id={}]: entity_id={}",
            thread, self.i, msg.id
        );

        let UpdateChunkCommand {
            id,
            revision,
            data,
        } = msg;
        let new = SerializedEntity{id,
            revision,
            data
        };

        let gzipped_data = self.load();
        let index = self.i;

        let res = {

            let data = gzipped_data.change(EntityType::Property, move |mut entities: BTreeMap<EntityId, SerializedEntity>| {
                if entities.contains_key(&id) {
                    entities.remove(&id);
                    // TODO: Check revision
                    entities.insert(id, new);
                } else {
                    entities.insert(id, new);
                }

                entities
            });

            let thread1 = {
                let thread1 = std::thread::current();
                thread1.name().unwrap_or("<unknown>").to_owned()
            };

            debug!("thread={} Will store, i={}", thread1, index);
            let r = Self::store(index, data);
            debug!("Done storing");
            r
        };

        Ok(res)
    }
}

pub(super) struct GetChunk;

pub type GetChunkResult = Result<Bytes, ()>;

impl Message for GetChunk {
    type Result = GetChunkResult;
}

impl Handler<GetChunk> for ChunkActor {
    type Result = GetChunkResult;

    fn handle(&mut self, _msg: GetChunk, _ctx: &mut Self::Context) -> Self::Result {
        let thread1 = std::thread::current();
        let thread = thread1.name().unwrap_or("<unknown>").to_owned();
        debug!("thread={} Get chunk: i={}", thread, self.i);
        let res = self.load().to_bytes();
        Ok(res)
    }
}

impl Actor for ChunkActor {
    type Context = Context<Self>;
}

struct GzippedData {
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

    fn change<F>(&self, ty: EntityType, f: F) -> Self
    where F: FnOnce(BTreeMap<EntityId, SerializedEntity>) -> BTreeMap<EntityId, SerializedEntity> {
        let entities = {
            self.decompress().split("\n")
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

                    SerializedEntity {id, revision, data: e.to_owned()}
                })
                .map(|e| (e.id, e))
                .collect::<BTreeMap<EntityId, SerializedEntity>>()
        };

        let entities = f(entities);

        let entities = entities.iter().map(|(_,e)| {
            let SerializedEntity {data, ..} = e;
            &data[..]
        }).collect::<Vec<_>>().join("\n") + "\n";

        Self::compress(&entities)
    }

    fn from_binary(data: Vec<u8>) -> GzippedData {
        GzippedData { inner: data }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn to_bytes(self) -> Bytes {
        Bytes::from(self.inner)
    }
}

impl AsRef<[u8]> for GzippedData {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}
