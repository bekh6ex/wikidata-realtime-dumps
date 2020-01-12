use crate::actor::UpdateCommand;
use crate::prelude::*;
use actix_web::client::Client;
use actix_web::http::StatusCode;
use actix_web::web::Bytes;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

pub async fn get_entity(client: Arc<Client>, id: EntityId) -> Option<GetEntityResult> {
    let req = client.get(format!(
        "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
        id
    ));

    let mut response = req
        .send()
        .await
        .expect(&format!("Didn't get the response: {}", &id));

    if response.status() == StatusCode::NOT_FOUND {
        return None;
    }

    let body: Bytes = response
        .body()
        .limit(8 * 1024 * 1024)
        .await
        .expect("Entity response body");

    let response = serde_json::from_slice::<WikidataResponse>(body.as_ref()).expect(&format!(
        "Invalid response format: {}\n{:?}",
        &id,
        std::str::from_utf8(body.as_ref())
    ));
    // Entity might be a redirect to another one which will be automatically resolved.
    // The response will then contains some other entity which should be ignored.
    let value = response.entities.get(&id.to_string())?;

    let data =
        serde_json::to_string(value).expect(&format!("Serialize {} entity back failed O_o", id));

    let revision = RevisionId(extract_revision_id(id, value));

    Some(GetEntityResult { id, revision, data })
}

fn extract_revision_id(id: EntityId, value: &Value) -> u64 {
    value
        .as_object()
        .expect(&format!("Entity {} representation was not an object", id))
        .get("lastrevid")
        .expect(&format!("Entity {} does not contain revision ID", id))
        .as_u64()
        .expect(&format!("Entity {} revision ID is not a u64", id))
}

pub struct GetEntityResult {
    id: EntityId,
    revision: RevisionId,
    data: String,
}

impl Into<UpdateCommand> for GetEntityResult {
    fn into(self) -> UpdateCommand {
        let GetEntityResult { id, revision, data } = self;
        UpdateCommand { id, revision, data }
    }
}

#[derive(Deserialize, Debug)]
struct WikidataResponse {
    entities: serde_json::Map<String, serde_json::Value>,
}
