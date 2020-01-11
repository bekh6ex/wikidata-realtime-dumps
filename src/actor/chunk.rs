use crate::actor::UpdateCommand;
use actix::{Actor, Context, Handler, Message};
use actix_web::web::Bytes;
use log::*;
use std::future::Future;
use std::pin::Pin;

pub struct ChunkActor {
    i: i32,
    data: String,
}

impl ChunkActor {
    pub fn new(i: i32) -> ChunkActor {
        ChunkActor {
            i,
            data: "".to_owned(),
        }
    }
}

impl Handler<UpdateCommand> for ChunkActor {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;

    fn handle(&mut self, msg: UpdateCommand, _ctx: &mut Self::Context) -> Self::Result {
        debug!("UpdateCommand[actor_id={}]: entity_id={}", self.i, msg.id);
        self.data.push_str(&msg.data);
        self.data.push_str("\n");

        use async_std::fs;

        use std::io::Write;

        fn gzip(data: String) -> Vec<u8> {
            use deflate::write::GzEncoder;
            use deflate::Compression;

            let mut encoder = GzEncoder::new(Vec::new(), Compression::Best);
            encoder.write_all(data.as_bytes()).unwrap();
            encoder.finish().unwrap()
        }

        let data_to_write = self.data.clone();

        let compressed_data = gzip(data_to_write);

        let dir_path = "/tmp/wd-rt-dumps/chunk";
        let file_path = format!("/tmp/wd-rt-dumps/chunk/{}.gz", self.i);

        let res = async move {
            trace!(
                "Writing a file '{}' len={}",
                file_path,
                compressed_data.len()
            );
            fs::create_dir_all(dir_path)
                .await
                .expect("Failed to create a directory");
            fs::write(file_path, compressed_data)
                .await
                .expect("Writing failed");
            ()
        };

        Ok(Box::pin(res))
    }
}

pub(super) struct GetChunk;

type GetChunkResult = Result<Bytes, ()>;

impl Message for GetChunk {
    type Result = GetChunkResult;
}

impl Handler<GetChunk> for ChunkActor {
    type Result = GetChunkResult;

    fn handle(&mut self, _msg: GetChunk, _ctx: &mut Self::Context) -> Self::Result {
        if self.data.len() > 0 {
            debug!("GetChunk[actor_id={}]: len={}", self.i, self.data.len());
        }
        Ok(Bytes::from(self.data.clone()))
    }
}

impl Actor for ChunkActor {
    type Context = Context<Self>;
}
