use serde::export::fmt::Error;
use serde::export::Formatter;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityType {
    Item,
    Property,
    Lexeme,
}

impl EntityType {
    pub fn id(self, n: u32) -> EntityId {
        EntityId { ty: self, id: n }
    }

    pub fn namespace(self) -> NamespaceId {
        match self {
            EntityType::Item => NamespaceId { n: 0, s: None },
            EntityType::Property => NamespaceId {
                n: 120,
                s: Some("Property"),
            },
            EntityType::Lexeme => NamespaceId {
                n: 146,
                s: Some("Lexeme"),
            },
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            EntityType::Item => "Q",
            EntityType::Property => "P",
            EntityType::Lexeme => "L",
        }
    }

    pub fn parse_from_title(self, title: &str) -> Result<EntityId, String> {
        match self.namespace().s {
            None => self.parse_id(title),
            Some(ns) => {
                let expected_prefix: String = ns.to_owned() + ":";
                if title.starts_with(&expected_prefix) {
                    let rest: &str = &title[expected_prefix.len()..];
                    self.parse_id(rest)
                } else {
                    Err(format!(
                        "Cannot parse ID type={:?}. Wrong title: title={}",
                        self, title
                    ))
                }
            }
        }
    }

    pub fn parse_id(self, s: &str) -> Result<EntityId, String> {
        if s.is_empty() {
            return Err(format!("Cannot parse empty ID: type={:?}", self));
        }
        let prefix = &s[0..1];
        if prefix != self.prefix() {
            Err(format!(
                "Wrong ID prefix for type: type={:?}, id={}",
                self, s
            ))
        } else {
            let rest = &s[1..];
            let id = rest
                .parse()
                .map_err(|e| format!("Error while parsing ID '{}': {:?}", s, e));

            id.map(|n| EntityId { ty: self, id: n })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityId {
    ty: EntityType,
    id: u32, //TODO: Use non-zero type
}

impl EntityId {
    pub fn ty(self) -> EntityType {
        self.ty
    }
    pub fn n(self) -> u32 {
        self.id
    }
    pub fn next(self) -> EntityId {
        EntityId {
            ty: self.ty,
            id: self.id + 1,
        }
    }
}

impl Display for EntityId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_str(self.ty.prefix()).unwrap();
        f.write_str(&self.id.to_string()).unwrap();
        Ok(())
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct RevisionId(pub u64);

#[derive(Debug)]
pub struct NamespaceId {
    n: i64,
    s: Option<&'static str>,
}

impl NamespaceId {
    pub fn n(&self) -> i64 {
        self.n
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::EntityType;

    #[test]
    fn entity_id_order() {
        assert!(EntityType::Item.id(1) < EntityType::Item.id(2));
    }
}
