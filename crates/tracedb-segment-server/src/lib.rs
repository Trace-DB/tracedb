#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRef {
    pub path: String,
    pub checksum: [u8; 32],
}

impl ObjectRef {
    pub fn new(path: impl Into<String>, checksum: [u8; 32]) -> Self {
        Self {
            path: path.into(),
            checksum,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SegmentServer {
    objects: BTreeMap<String, ObjectRef>,
}

impl SegmentServer {
    pub fn publish(&mut self, object: ObjectRef) -> Result<(), String> {
        if object.checksum == [0u8; 32] {
            return Err("object checksum cannot be zero".to_string());
        }
        self.objects.insert(object.path.clone(), object);
        Ok(())
    }

    pub fn get(&self, path: &str) -> Option<&ObjectRef> {
        self.objects.get(path)
    }
}
