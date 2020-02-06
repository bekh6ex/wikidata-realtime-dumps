
use actix_rt;
use crate::archive::{Archivarius, Initialization, GetDump, GetDumpResult};
use crate::prelude::{EntityType, SerializedEntity, RevisionId};
use crate::archive::arbiter_pool::ArbiterPool;
use actix::prelude::*;
use crate::events::EventId;
use futures::stream::*;
use std::io::Read;
use serde::{Deserialize, Serialize};
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

#[actix_rt::test] #[test]
async fn just_initialized_archivarius_should_return_item_from_dump() {
    let dir: u64 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let dir = format!("/tmp/wd-tests/{}", dir);
    let archivarius = Archivarius::create(|ctx| {
        Archivarius::new(&dir, EntityType::Item, pool(), ctx.address())
    });

    let mut revision = (1u64..).into_iter();

    let mut next_rev = move || {
        revision.next().unwrap()
    };

    let start_event_id = EventId::test(1);

    archivarius.send(Initialization::Start(start_event_id)).await.unwrap().await;
    archivarius.send(Initialization::UpdateEntity(item(1, next_rev()))).await.unwrap().await;
    archivarius.send(Initialization::Finished).await.unwrap().await;

    let dump_stream: GetDumpResult = archivarius.send(GetDump).await.unwrap();

    let mut entities = to_entities(dump_stream).await;

    assert_eq!(entities.len(), 1);
    let entity = entities.pop().unwrap();
    assert_eq!(entity.id, "Q1");
    assert_eq!(entity.lastrevid, 1);
}

async fn to_entities(stream: GetDumpResult) -> Vec<Entity> {
    let chunks: Vec<_> = stream.collect().await;

    let dump: Vec<u8> = chunks.iter().map(|c: &Bytes| c.to_vec()).flatten().collect();

    let mut decoder = flate2::read::GzDecoder::new(&dump[..]);
    let mut s = "".to_owned();
    decoder.read_to_string(&mut s).unwrap();

    s.split("\n")
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<Entity>(&l).unwrap())
        .collect()
}


fn pool() -> ArbiterPool {
    use core::num::NonZeroUsize;
    ArbiterPool::new(NonZeroUsize::new(1).unwrap())
}

fn item(id: u32, rev: u64) -> SerializedEntity {
    let id = EntityType::Item.id(id);
    let data = Entity {
        id: id.to_string(),
        lastrevid: rev
    };
    SerializedEntity {
        id,
        revision: RevisionId(rev),
        data: serde_json::to_string(&data).unwrap()
    }
}

#[derive(Serialize, Deserialize)]
struct Entity {
    id: String,
    lastrevid: u64,
}
