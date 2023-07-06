use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

use actix::prelude::*;
use actix_rt;
use bytes::Bytes;
use futures::stream::*;
use serde::{Deserialize, Serialize};

use crate::archive::{Archivarius, GetDump, GetDumpResult, Initialization};
use crate::archive::arbiter_pool::ArbiterPool;
use crate::events::EventId;
use crate::prelude::{EntityType, RevisionId, SerializedEntity};

use proptest_attr_macro::proptest;
use crate::archive::archivarius::VolumeKeeperConfig;

#[proptest]
fn just_initialized_archivarius_should_return_item_from_dump(id: u32) {
    with_temp_dir(move |dir| async move {
        let archivarius = item_archivarius(dir);

        let mut revision = (1u64..).into_iter();
        let mut next_rev = move || {
            revision.next().unwrap()
        };

        let start_event_id = EventId::test(1);

        archivarius.send(Initialization::Start(start_event_id)).await.unwrap().await;
        archivarius.send(Initialization::UpdateEntity(item(id, next_rev()))).await.unwrap().await;
        archivarius.send(Initialization::Finished).await.unwrap().await;

        let dump_stream: GetDumpResult = archivarius.send(GetDump).await.unwrap();

        let mut entities = to_entities(dump_stream).await;

        assert_eq!(entities.len(), 1);
        let entity = entities.pop().unwrap();
        let expected_id = EntityType::Item.id(id).to_string();
        assert_eq!(entity.id, expected_id);
        assert_eq!(entity.lastrevid, 1);
    })
}

fn with_temp_dir<Fn, Fut>(f: Fn)
    where
        Fn: FnOnce(String) -> Fut + 'static,
        Fut: Future<Output=()> + 'static
{
    let dir: u64 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let dir = format!("/tmp/wd-tests/{}", dir);
    actix_rt::System::new("test")
        .block_on({
            let dir = dir.clone();
            async move {
                f(dir).await;
            }
        });

    std::fs::remove_dir_all(dir).unwrap();
}

fn item_archivarius(dir: String) -> Addr<Archivarius> {
    Archivarius::create(|ctx| {
        let config = VolumeKeeperConfig{
            max_volume_size: 0, // Each entity goes into separate volume
            write_down_delay: Duration::from_millis(1),
        };
        Archivarius::new(&dir, EntityType::Item, config, pool())
    })
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
