#![forbid(unsafe_code)]
#![type_length_limit = "1383138"]
#![warn(unused_extern_crates)]

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use actix::prelude::*;
use futures::stream;
use futures::{self, StreamExt};
use log::*;

use crate::archive::UpdateCommand;
use crate::archive::{Archivarius, QueryState};
use crate::events::{update_command_stream, EventId};
use crate::prelude::EntityType;

use self::archive::{start, ArchivariusMap};
use crate::init::{ArchiveFormat, DumpConfig, DumpFormat};
use crate::get_entity::GetEntityClient;

mod archive;
mod events;
mod get_entity;
mod http_client;
mod init;
mod prelude;
mod stream_ext;
mod warp_server;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    init_logger();

    info!("Starting...");

//    let dump_config: BTreeMap<EntityType, DumpConfig> = get_dump_config();
    let dump_config: BTreeMap<EntityType, DumpConfig> = BTreeMap::new();

    // TODO: Lock storage file

    let types = vec![EntityType::Lexeme, EntityType::Property, EntityType::Item];

    let map: ArchivariusMap = start(types);

    let ws = warp_server::start(&map);
    let client = GetEntityClient::default();


    let update_streams = get_streams(client, map.clone(), dump_config).await;

    let futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![Box::pin(ws), update_streams];

    let _ = futures::future::join_all(futures).await;
    Ok(())
}

fn get_dump_config() -> BTreeMap<EntityType, DumpConfig> {
    let dump_event_id = "[{\"topic\":\"eqiad.mediawiki.recentchange\",\"partition\":0,\"timestamp\":1579993200000},{\"topic\":\"codfw.mediawiki.recentchange\",\"partition\":0,\"offset\":-1}]";
    let dump_event_id = EventId::new(dump_event_id.to_owned());
    let mut map = BTreeMap::new();
    let item_dump_config = DumpConfig {
        url: "https://dumps.wikimedia.org/other/wikibase/wikidatawiki/20200127/wikidata-20200127-all.json.bz2".to_owned(),
        event_stream_start: dump_event_id,
        ty: EntityType::Item,
        archive_format: ArchiveFormat::Bzip2,
        dump_format: DumpFormat::WikidataJsonArray,
    };
    map.insert(EntityType::Item, item_dump_config);
    map
}

async fn get_streams(
    client: GetEntityClient,
    map: Arc<BTreeMap<EntityType, Addr<Archivarius>>>,
    dump_config_map: BTreeMap<EntityType, DumpConfig>,
) -> Pin<Box<dyn Future<Output = ()>>> {
    fn to_vec(
        map: Arc<BTreeMap<EntityType, Addr<Archivarius>>>,
    ) -> Vec<(EntityType, Addr<Archivarius>)> {
        map.iter()
            .map(|(ty, ad)| (*ty, ad.clone()))
            .collect::<Vec<_>>()
    }

    let map = to_vec(map);
    let res = stream::iter(map).for_each_concurrent(None, move |(entity_type, archive_actor)| {
        let dump_config = dump_config_map.get(&entity_type).map(|c| c.clone());
        let client = client.clone();
        async move {
            let entity_type = entity_type;
            let initial_event_id =
                initialize(client.clone(), entity_type, archive_actor.clone(), dump_config).await;

            let update_stream = update_command_stream(client, entity_type, initial_event_id).await;

            let send_forward = move |e: UpdateCommand| {
                let result = archive_actor.send(e);
                async_std::task::spawn(async move {
                    result.await.expect("Actor communication failed").await;
                    trace!("Got command result.");
                })
            };

            // TODO: Increase rewind EventId value on start to be sure
            update_stream
                .for_each_concurrent(num_cpus::get() * 3, send_forward)
                .await
        }
    });

    Box::pin(res)
}

async fn initialize(
    client: GetEntityClient,
    ty: EntityType,
    actor: Addr<Archivarius>,
    dump_config: Option<DumpConfig>,
) -> EventId {
    let response = actor.send(QueryState).await.expect("Failed commun");

    let init_stream = init::init(client, ty, response.initialized_up_to, dump_config).await;

    let init_stream = init_stream.for_each({
        let actor = actor.clone();
        move |e| {
            let actor = actor.clone();
            async move {
                actor
                    .send(e)
                    .await
                    .expect("Failed to initialize actor")
                    .await;
            }
        }
    });

    init_stream.await;

    let response = actor.send(QueryState).await.expect("Failed commun");

    let event_id = response
        .last_event_id
        .expect("Archivarius does not seem to be initialized");

    info!("Finished initialization. Next event: {:?}", event_id);

    event_id
}

fn init_logger() {
    log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
}
