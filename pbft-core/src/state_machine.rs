use std::collections::BTreeMap;

use crate::{Operation, OperationResult};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to create checkpoint: {0}")]
    CheckpointCreateError(serde_json::Error),
}

pub trait StateMachie: Send + Sync {
    fn apply_operation(&mut self, operation: &Operation) -> OperationResult;
    fn checkpoint(&self, sequence: u64) -> Result<String, Error>;
}

pub struct InMemoryKVStore {
    store: BTreeMap<String, String>,
}

impl InMemoryKVStore {
    pub fn new() -> Self {
        InMemoryKVStore {
            store: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.store.get(key).cloned()
    }
}

impl Default for InMemoryKVStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachie for InMemoryKVStore {
    fn apply_operation(&mut self, operation: &Operation) -> OperationResult {
        match operation {
            Operation::Set { key, value } => {
                self.store.insert(key.clone(), value.clone());
                OperationResult::Set {
                    key: key.clone(),
                    value: value.clone(),
                }
            }
            Operation::Get { key } => {
                let value = self.store.get(key).cloned();
                OperationResult::Get {
                    key: key.clone(),
                    value,
                }
            }
            Operation::Noop => OperationResult::Noop,
        }
    }

    fn checkpoint(&self, _sequence: u64) -> Result<String, Error> {
        let checkpoint =
            serde_json::to_string(&self.store).map_err(Error::CheckpointCreateError)?;
        Ok(checkpoint)
    }
}
