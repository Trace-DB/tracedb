#![forbid(unsafe_code)]

use std::collections::VecDeque;
use tracedb_segment_server::ObjectRef;

#[derive(Clone, Debug)]
pub struct SegmentCache {
    capacity: usize,
    entries: VecDeque<ObjectRef>,
}

impl SegmentCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::new(),
        }
    }

    pub fn insert(&mut self, object: ObjectRef) {
        self.entries.retain(|entry| entry.path != object.path);
        self.entries.push_front(object);
        while self.entries.len() > self.capacity {
            self.entries.pop_back();
        }
    }

    pub fn get(&self, path: &str) -> Option<&ObjectRef> {
        self.entries.iter().find(|entry| entry.path == path)
    }
}
