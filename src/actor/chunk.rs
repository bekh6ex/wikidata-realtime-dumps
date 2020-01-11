use crate::actor::UpdateCommand;
use actix::{Actor, Context, Handler, Message};
use actix_web::web::Bytes;
use futures::FutureExt;
use log::*;
use std::future::Future;
use std::io;
use std::pin::Pin;

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

    fn load(&self) -> impl Future<Output = GzippedData> {
        use async_std::fs;

        let file_path = self.file_path();
        let file_path1 = self.file_path();

        async move {
            trace!("Reading a file '{}'", file_path);

            let read_result: io::Result<_> = fs::read(file_path).await;

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

    fn store(index: i32, data: GzippedData) -> impl Future<Output = ()> {
        use async_std::fs;
        let dir_path = "/tmp/wd-rt-dumps/chunk";
        let file_path = format!("/tmp/wd-rt-dumps/chunk/{}.gz", index);

        async move {
            trace!("Writing a file '{}' len={}", file_path, data.len());
            fs::create_dir_all(dir_path)
                .await
                .expect("Failed to create a directory");
            fs::write(file_path, data).await.expect("Writing failed");
        }
    }
}

impl Handler<UpdateCommand> for ChunkActor {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;

    fn handle(&mut self, msg: UpdateCommand, _ctx: &mut Self::Context) -> Self::Result {
        debug!("UpdateCommand[actor_id={}]: entity_id={}", self.i, msg.id);

        let data = self.load();
        let index = self.i;

        let res = async move {
            let mut data = data.await.decompress();
            data.push_str(&msg.data);
            data.push_str("\n");

            Self::store(index, GzippedData::compress(&data)).await
        };

        //        let res = self.store(GzippedData::compress(&self.data));

        Ok(Box::pin(res))
    }
}

pub(super) struct GetChunk;

pub type GetChunkResult = Result<Pin<Box<dyn Future<Output = Bytes> + Send + Sync>>, ()>;

impl Message for GetChunk {
    type Result = GetChunkResult;
}

impl Handler<GetChunk> for ChunkActor {
    type Result = GetChunkResult;

    fn handle(&mut self, _msg: GetChunk, _ctx: &mut Self::Context) -> Self::Result {
        let res = self.load().map(|d| d.to_bytes());
        Ok(Box::pin(res))
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
