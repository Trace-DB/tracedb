#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use tracedb_core::{
    source_hash, value_as_f32_vec, DerivedFeatureState, Epoch, FeatureInvalidation, FeatureStatus,
    RecordDeletion, RecordInput, Result, TableSchema, TraceDbError, VersionId,
};
use tracedb_log::CommitRecord;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordHeader {
    pub record_id: String,
    pub table_id: String,
    pub tenant_id: String,
    pub schema_version: u64,
    pub begin_epoch: Epoch,
    pub end_epoch: Option<Epoch>,
    pub version_id: VersionId,
    pub tombstone: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredRecord {
    pub header: RecordHeader,
    pub fields: Map<String, Value>,
    pub features: BTreeMap<String, DerivedFeatureState>,
}

#[derive(Clone, Debug, Default)]
pub struct RecordStore {
    versions: BTreeMap<String, Vec<StoredRecord>>,
}

impl RecordStore {
    pub fn from_checkpoint_records(records: Vec<StoredRecord>) -> Result<Self> {
        let mut store = Self::default();
        for record in records {
            let key = record_key(
                &record.header.table_id,
                &record.header.tenant_id,
                &record.header.record_id,
            );
            store.versions.entry(key).or_default().push(record);
        }
        Ok(store)
    }

    pub fn validate_mutation(schema: &TableSchema, mutation: &RecordInput) -> Result<()> {
        validate_record_identity(schema, mutation, None)?;
        validate_vector_dimensions(schema, mutation)
    }

    pub fn from_commits(schemas: &[TableSchema], commits: &[CommitRecord]) -> Result<Self> {
        let mut store = Self::default();
        store.apply_commits(schemas, commits)?;
        Ok(store)
    }

    pub fn apply_commits(
        &mut self,
        schemas: &[TableSchema],
        commits: &[CommitRecord],
    ) -> Result<()> {
        for commit in commits {
            for replacement in &commit.replacements {
                let schema = schemas
                    .iter()
                    .find(|schema| schema.name == replacement.table)
                    .ok_or_else(|| TraceDbError::UnknownTable(replacement.table.clone()))?;
                self.apply_replacement(schema, replacement, commit.epoch)?;
            }
            for mutation in &commit.mutations {
                let schema = schemas
                    .iter()
                    .find(|schema| schema.name == mutation.table)
                    .ok_or_else(|| TraceDbError::UnknownTable(mutation.table.clone()))?;
                self.apply_mutation(schema, mutation, commit.epoch)?;
            }
            for deletion in &commit.deletions {
                let schema = schemas
                    .iter()
                    .find(|schema| schema.name == deletion.table)
                    .ok_or_else(|| TraceDbError::UnknownTable(deletion.table.clone()))?;
                self.apply_delete(schema, deletion, commit.epoch)?;
            }
            for invalidation in &commit.feature_invalidations {
                let invalidation = if invalidation.tenant_id.trim().is_empty() {
                    self.resolve_legacy_feature_invalidation(invalidation, commit)?
                } else {
                    invalidation.clone()
                };
                self.apply_feature_invalidation(&invalidation, commit.epoch)?;
            }
        }
        Ok(())
    }

    pub fn checkpoint_records(&self, epoch: Epoch) -> Vec<StoredRecord> {
        let mut records = Vec::new();
        for versions in self.versions.values() {
            if let Some(record) = versions.iter().rev().find(|record| {
                record.header.begin_epoch <= epoch
                    && record
                        .header
                        .end_epoch
                        .map(|end| end > epoch)
                        .unwrap_or(true)
            }) {
                records.push(record.clone());
            }
        }
        records
    }

    pub fn apply_replacement(
        &mut self,
        schema: &TableSchema,
        input: &RecordInput,
        epoch: Epoch,
    ) -> Result<StoredRecord> {
        let (key, record) = build_replacement_record(schema, input, epoch)?;
        let returned = record.clone();
        self.install_replacement(key, record, epoch);
        Ok(returned)
    }

    pub fn apply_replacement_without_return(
        &mut self,
        schema: &TableSchema,
        input: &RecordInput,
        epoch: Epoch,
    ) -> Result<()> {
        let (key, record) = build_replacement_record(schema, input, epoch)?;
        self.install_replacement(key, record, epoch);
        Ok(())
    }

    fn install_replacement(&mut self, key: String, record: StoredRecord, epoch: Epoch) {
        let versions = self.versions.entry(key).or_default();

        if let Some(current) = versions
            .iter_mut()
            .rev()
            .find(|record| record.header.end_epoch.is_none())
        {
            current.header.end_epoch = Some(epoch);
        }

        versions.push(record);
    }

    pub fn apply_mutation(
        &mut self,
        schema: &TableSchema,
        mutation: &RecordInput,
        epoch: Epoch,
    ) -> Result<StoredRecord> {
        validate_record_identity(schema, mutation, None)?;
        validate_vector_dimensions(schema, mutation)?;
        let tenant_id = if mutation.tenant_id.is_empty() {
            return Err(TraceDbError::InvalidRecord(
                "tenant id cannot be empty".to_string(),
            ));
        } else {
            mutation.tenant_id.clone()
        };
        let key = record_key(&mutation.table, &tenant_id, &mutation.id);
        let versions = self.versions.entry(key).or_default();
        let previous = versions
            .iter()
            .rev()
            .find(|record| record.header.end_epoch.is_none() && record.header.tombstone.is_none())
            .cloned();

        if let Some(current) = versions
            .iter_mut()
            .rev()
            .find(|record| record.header.end_epoch.is_none())
        {
            current.header.end_epoch = Some(epoch);
        }

        let mut merged_fields = previous
            .as_ref()
            .map(|record| record.fields.clone())
            .unwrap_or_default();
        for (key, value) in &mutation.fields {
            merged_fields.insert(key.clone(), value.clone());
        }

        if !merged_fields.contains_key(&schema.primary_id_column) {
            merged_fields.insert(
                schema.primary_id_column.clone(),
                Value::String(mutation.id.clone()),
            );
        }
        if !merged_fields.contains_key(&schema.tenant_id_column) {
            merged_fields.insert(
                schema.tenant_id_column.clone(),
                Value::String(tenant_id.clone()),
            );
        }
        validate_record_identity(schema, mutation, Some(&merged_fields))?;

        let features = build_features(schema, mutation, &merged_fields, previous.as_ref(), epoch);
        let record = StoredRecord {
            header: RecordHeader {
                record_id: mutation.id.clone(),
                table_id: mutation.table.clone(),
                tenant_id,
                schema_version: 1,
                begin_epoch: epoch,
                end_epoch: None,
                version_id: VersionId::new(epoch.get()),
                tombstone: None,
            },
            fields: merged_fields,
            features,
        };
        versions.push(record.clone());
        Ok(record)
    }

    pub fn apply_delete(
        &mut self,
        schema: &TableSchema,
        deletion: &RecordDeletion,
        epoch: Epoch,
    ) -> Result<StoredRecord> {
        validate_deletion_identity(schema, deletion)?;
        let key = record_key(&deletion.table, &deletion.tenant_id, &deletion.id);
        let Some(versions) = self.versions.get_mut(&key) else {
            return Err(TraceDbError::NotFound(format!(
                "{}:{}:{}",
                deletion.table, deletion.tenant_id, deletion.id
            )));
        };
        let previous = versions
            .iter()
            .rev()
            .find(|record| record.header.end_epoch.is_none() && record.header.tombstone.is_none())
            .cloned()
            .ok_or_else(|| {
                TraceDbError::NotFound(format!(
                    "{}:{}:{}",
                    deletion.table, deletion.tenant_id, deletion.id
                ))
            })?;

        if let Some(current) = versions
            .iter_mut()
            .rev()
            .find(|record| record.header.end_epoch.is_none() && record.header.tombstone.is_none())
        {
            current.header.end_epoch = Some(epoch);
        }

        let record = StoredRecord {
            header: RecordHeader {
                record_id: deletion.id.clone(),
                table_id: deletion.table.clone(),
                tenant_id: deletion.tenant_id.clone(),
                schema_version: previous.header.schema_version,
                begin_epoch: epoch,
                end_epoch: None,
                version_id: VersionId::new(epoch.get()),
                tombstone: Some(if deletion.tombstone.trim().is_empty() {
                    "user_delete".to_string()
                } else {
                    deletion.tombstone.clone()
                }),
            },
            fields: previous.fields,
            features: previous.features,
        };
        versions.push(record.clone());
        Ok(record)
    }

    pub fn get_record(
        &self,
        table: &str,
        tenant_id: &str,
        record_id: &str,
        epoch: Epoch,
    ) -> Option<StoredRecord> {
        self.versions
            .get(&record_key(table, tenant_id, record_id))
            .and_then(|versions| visible_record_at(versions, epoch))
    }

    pub fn scan_records(
        &self,
        table: &str,
        tenant_id: &str,
        limit: usize,
        epoch: Epoch,
    ) -> Vec<StoredRecord> {
        let mut records = self.visible_records_at(table, tenant_id, epoch);
        records.sort_by(|left, right| left.header.record_id.cmp(&right.header.record_id));
        records.truncate(limit);
        records
    }

    pub fn visible_records_at(
        &self,
        table: &str,
        tenant_id: &str,
        epoch: Epoch,
    ) -> Vec<StoredRecord> {
        let prefix = record_key_prefix(table, tenant_id);
        self.versions
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .filter_map(|(_, versions)| visible_record_at(versions, epoch))
            .collect()
    }

    pub fn snapshot(&self, epoch: Epoch) -> ReadSnapshot {
        let mut records = Vec::new();
        for versions in self.versions.values() {
            if let Some(record) = versions.iter().rev().find(|record| {
                record.header.begin_epoch <= epoch
                    && record
                        .header
                        .end_epoch
                        .map(|end| end > epoch)
                        .unwrap_or(true)
                    && record.header.tombstone.is_none()
            }) {
                records.push(record.clone());
            }
        }
        ReadSnapshot { epoch, records }
    }

    pub fn apply_feature_invalidation(
        &mut self,
        invalidation: &FeatureInvalidation,
        epoch: Epoch,
    ) -> Result<DerivedFeatureState> {
        let key = record_key(
            &invalidation.table,
            &invalidation.tenant_id,
            &invalidation.record_id,
        );
        let Some(versions) = self.versions.get_mut(&key) else {
            return Err(TraceDbError::NotFound(format!(
                "{}:{}:{}",
                invalidation.table, invalidation.tenant_id, invalidation.record_id
            )));
        };
        let Some(record) = versions
            .iter_mut()
            .rev()
            .find(|record| record.header.end_epoch.is_none() && record.header.tombstone.is_none())
        else {
            return Err(TraceDbError::NotFound(format!(
                "{}:{}:{}",
                invalidation.table, invalidation.tenant_id, invalidation.record_id
            )));
        };
        let Some(state) = record.features.get_mut(&invalidation.feature) else {
            return Err(TraceDbError::NotFound(format!(
                "feature {}.{}.{}",
                invalidation.table, invalidation.record_id, invalidation.feature
            )));
        };

        state.status = invalidation.status.clone();
        state.valid_for_epoch = epoch;
        Ok(state.clone())
    }

    fn resolve_legacy_feature_invalidation(
        &self,
        invalidation: &FeatureInvalidation,
        commit: &CommitRecord,
    ) -> Result<FeatureInvalidation> {
        let mut commit_tenants = commit
            .mutations
            .iter()
            .chain(commit.replacements.iter())
            .filter(|input| input.table == invalidation.table && input.id == invalidation.record_id)
            .filter_map(|input| {
                let tenant_id = input.tenant_id.trim();
                (!tenant_id.is_empty()).then(|| tenant_id.to_string())
            })
            .collect::<BTreeSet<_>>();

        if commit_tenants.len() == 1 {
            let mut resolved = invalidation.clone();
            resolved.tenant_id = commit_tenants.pop_first().expect("one tenant");
            return Ok(resolved);
        }
        if commit_tenants.len() > 1 {
            return Err(TraceDbError::WalCorruption(format!(
                "ambiguous feature invalidation for {}.{}.{} in commit {}",
                invalidation.table,
                invalidation.record_id,
                invalidation.feature,
                commit.transaction_id
            )));
        }

        let active_tenants = self
            .active_feature_tenants(invalidation)
            .into_iter()
            .collect::<BTreeSet<_>>();
        if active_tenants.len() == 1 {
            let mut resolved = invalidation.clone();
            resolved.tenant_id = active_tenants.into_iter().next().expect("one tenant");
            return Ok(resolved);
        }
        if active_tenants.is_empty() {
            return Err(TraceDbError::NotFound(format!(
                "feature {}.{}.{}",
                invalidation.table, invalidation.record_id, invalidation.feature
            )));
        }

        Err(TraceDbError::WalCorruption(format!(
            "ambiguous feature invalidation for {}.{}.{}",
            invalidation.table, invalidation.record_id, invalidation.feature
        )))
    }

    fn active_feature_tenants(&self, invalidation: &FeatureInvalidation) -> Vec<String> {
        let mut tenants = Vec::new();
        for versions in self.versions.values() {
            if let Some(record) = versions.iter().rev().find(|record| {
                record.header.table_id == invalidation.table
                    && record.header.record_id == invalidation.record_id
                    && record.header.end_epoch.is_none()
                    && record.header.tombstone.is_none()
            }) {
                if record.features.contains_key(&invalidation.feature) {
                    tenants.push(record.header.tenant_id.clone());
                }
            }
        }
        tenants
    }

    pub fn feature_state(
        &self,
        table: &str,
        tenant_id: &str,
        record_id: &str,
        feature: &str,
        epoch: Epoch,
    ) -> Option<DerivedFeatureState> {
        self.versions.values().find_map(|versions| {
            versions
                .iter()
                .rev()
                .find(|record| {
                    record.header.table_id == table
                        && record.header.tenant_id == tenant_id
                        && record.header.record_id == record_id
                        && record.header.tombstone.is_none()
                        && record.header.begin_epoch <= epoch
                        && record
                            .header
                            .end_epoch
                            .map(|end| end > epoch)
                            .unwrap_or(true)
                })
                .and_then(|record| record.features.get(feature).cloned())
        })
    }

    pub fn is_tombstoned_at(
        &self,
        table: &str,
        tenant_id: &str,
        record_id: &str,
        epoch: Epoch,
    ) -> bool {
        self.versions
            .get(&record_key(table, tenant_id, record_id))
            .and_then(|versions| {
                versions.iter().rev().find(|record| {
                    record.header.begin_epoch <= epoch
                        && record
                            .header
                            .end_epoch
                            .map(|end| end > epoch)
                            .unwrap_or(true)
                })
            })
            .map(|record| record.header.tombstone.is_some())
            .unwrap_or(false)
    }
}

#[derive(Clone, Debug)]
pub struct ReadSnapshot {
    pub epoch: Epoch,
    records: Vec<StoredRecord>,
}

impl ReadSnapshot {
    pub fn visible_records(&self, table: &str, tenant_id: &str) -> Vec<StoredRecord> {
        self.records
            .iter()
            .filter(|record| {
                record.header.table_id == table && record.header.tenant_id == tenant_id
            })
            .cloned()
            .collect()
    }

    pub fn all_visible_records(&self, table: &str) -> Vec<StoredRecord> {
        self.records
            .iter()
            .filter(|record| record.header.table_id == table)
            .cloned()
            .collect()
    }

    pub fn get_record(
        &self,
        table: &str,
        tenant_id: &str,
        record_id: &str,
    ) -> Option<StoredRecord> {
        self.records
            .iter()
            .find(|record| {
                record.header.table_id == table
                    && record.header.tenant_id == tenant_id
                    && record.header.record_id == record_id
            })
            .cloned()
    }
}

fn visible_record_at(versions: &[StoredRecord], epoch: Epoch) -> Option<StoredRecord> {
    versions
        .iter()
        .rev()
        .find(|record| {
            record.header.begin_epoch <= epoch
                && record
                    .header
                    .end_epoch
                    .map(|end| end > epoch)
                    .unwrap_or(true)
                && record.header.tombstone.is_none()
        })
        .cloned()
}

fn build_replacement_record(
    schema: &TableSchema,
    input: &RecordInput,
    epoch: Epoch,
) -> Result<(String, StoredRecord)> {
    validate_record_identity(schema, input, None)?;
    validate_vector_dimensions(schema, input)?;
    let tenant_id = if input.tenant_id.is_empty() {
        return Err(TraceDbError::InvalidRecord(
            "tenant id cannot be empty".to_string(),
        ));
    } else {
        input.tenant_id.clone()
    };
    let key = record_key(&input.table, &tenant_id, &input.id);

    let mut fields = input.fields.clone();
    fields.insert(
        schema.primary_id_column.clone(),
        Value::String(input.id.clone()),
    );
    fields.insert(
        schema.tenant_id_column.clone(),
        Value::String(tenant_id.clone()),
    );
    validate_record_identity(schema, input, Some(&fields))?;

    let features = build_features(schema, input, &fields, None, epoch);
    let record = StoredRecord {
        header: RecordHeader {
            record_id: input.id.clone(),
            table_id: input.table.clone(),
            tenant_id,
            schema_version: 1,
            begin_epoch: epoch,
            end_epoch: None,
            version_id: VersionId::new(epoch.get()),
            tombstone: None,
        },
        fields,
        features,
    };
    Ok((key, record))
}

fn build_features(
    schema: &TableSchema,
    mutation: &RecordInput,
    merged_fields: &Map<String, Value>,
    previous: Option<&StoredRecord>,
    epoch: Epoch,
) -> BTreeMap<String, DerivedFeatureState> {
    let mut features = previous
        .map(|record| record.features.clone())
        .unwrap_or_default();

    for vector in &schema.vector_columns {
        let source_changed = vector
            .source_columns
            .iter()
            .any(|source| mutation.fields.contains_key(source));
        let vector_changed = mutation.fields.contains_key(&vector.name);
        let state = if vector_changed {
            DerivedFeatureState::ready(
                vector.source_columns.clone(),
                source_hash(merged_fields, &vector.source_columns),
                epoch,
            )
        } else if source_changed {
            let new_source_hash = source_hash(merged_fields, &vector.source_columns);
            features
                .get(&vector.name)
                .map(|state| DerivedFeatureState::dirty_from(state, new_source_hash, epoch))
                .unwrap_or_else(|| {
                    let mut state =
                        DerivedFeatureState::missing(vector.source_columns.clone(), epoch);
                    state.source_hash = new_source_hash;
                    state.status = FeatureStatus::Dirty;
                    state
                })
        } else {
            features.get(&vector.name).cloned().unwrap_or_else(|| {
                if merged_fields
                    .get(&vector.name)
                    .and_then(value_as_f32_vec)
                    .is_some()
                {
                    DerivedFeatureState::ready(
                        vector.source_columns.clone(),
                        source_hash(merged_fields, &vector.source_columns),
                        epoch,
                    )
                } else {
                    DerivedFeatureState::missing(vector.source_columns.clone(), epoch)
                }
            })
        };
        features.insert(vector.name.clone(), state);
    }

    features
}

fn validate_vector_dimensions(schema: &TableSchema, mutation: &RecordInput) -> Result<()> {
    for vector in &schema.vector_columns {
        if let Some(value) = mutation.fields.get(&vector.name) {
            let actual = vector_dimension_count(value).unwrap_or(0);
            if actual != vector.dimensions {
                return Err(TraceDbError::InvalidVectorDimensions {
                    column: vector.name.clone(),
                    expected: vector.dimensions,
                    actual,
                });
            }
        }
    }
    Ok(())
}

fn vector_dimension_count(value: &Value) -> Option<usize> {
    let array = value.as_array()?;
    if array.iter().all(|item| item.as_f64().is_some()) {
        Some(array.len())
    } else {
        None
    }
}

fn validate_record_identity(
    schema: &TableSchema,
    mutation: &RecordInput,
    fields: Option<&Map<String, Value>>,
) -> Result<()> {
    if mutation.id.is_empty() {
        return Err(TraceDbError::InvalidRecord(
            "record id cannot be empty".to_string(),
        ));
    }
    if mutation.tenant_id.is_empty() {
        return Err(TraceDbError::InvalidRecord(
            "tenant id cannot be empty".to_string(),
        ));
    }
    let fields = fields.unwrap_or(&mutation.fields);
    if let Some(value) = fields.get(&schema.primary_id_column) {
        if value.as_str() != Some(mutation.id.as_str()) {
            return Err(TraceDbError::InvalidRecord(format!(
                "primary id field {} must match record id",
                schema.primary_id_column
            )));
        }
    }
    if let Some(value) = fields.get(&schema.tenant_id_column) {
        if value.as_str() != Some(mutation.tenant_id.as_str()) {
            return Err(TraceDbError::InvalidRecord(format!(
                "tenant field {} must match tenant id",
                schema.tenant_id_column
            )));
        }
    }
    Ok(())
}

fn validate_deletion_identity(schema: &TableSchema, deletion: &RecordDeletion) -> Result<()> {
    if deletion.table != schema.name {
        return Err(TraceDbError::InvalidRecord(format!(
            "record table {} does not match schema {}",
            deletion.table, schema.name
        )));
    }
    if deletion.id.trim().is_empty() {
        return Err(TraceDbError::InvalidRecord(
            "record id cannot be empty".to_string(),
        ));
    }
    if deletion.tenant_id.trim().is_empty() {
        return Err(TraceDbError::InvalidRecord(
            "tenant id cannot be empty".to_string(),
        ));
    }
    Ok(())
}

fn record_key(table: &str, tenant: &str, id: &str) -> String {
    format!("{}{id}", record_key_prefix(table, tenant))
}

fn record_key_prefix(table: &str, tenant: &str) -> String {
    format!("{table}\u{0}{tenant}\u{0}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tracedb_core::VectorColumnSchema;

    fn schema() -> TableSchema {
        TableSchema {
            name: "docs".to_string(),
            primary_id_column: "id".to_string(),
            tenant_id_column: "tenant".to_string(),
            scalar_columns: vec![],
            text_indexed_columns: vec!["body".to_string()],
            vector_columns: vec![VectorColumnSchema {
                name: "embedding".to_string(),
                dimensions: 2,
                source_columns: vec!["body".to_string()],
            }],
        }
    }

    fn record(id: &str, tenant: &str) -> RecordInput {
        RecordInput {
            table: "docs".to_string(),
            id: id.to_string(),
            tenant_id: tenant.to_string(),
            fields: json!({
                "id": id,
                "tenant": tenant,
                "body": format!("{tenant} {id}"),
                "embedding": [0.1, 0.2],
            })
            .as_object()
            .expect("object")
            .clone(),
        }
    }

    #[test]
    fn visible_records_at_uses_table_tenant_partition_prefix() {
        let schema = schema();
        let mut store = RecordStore::default();
        store
            .apply_mutation(&schema, &record("a", "tenant-a"), Epoch::new(1))
            .expect("insert tenant a");
        store
            .apply_mutation(&schema, &record("b", "tenant-b"), Epoch::new(2))
            .expect("insert tenant b");
        store
            .apply_delete(
                &schema,
                &RecordDeletion {
                    table: "docs".to_string(),
                    tenant_id: "tenant-a".to_string(),
                    id: "a".to_string(),
                    tombstone: "delete".to_string(),
                },
                Epoch::new(3),
            )
            .expect("delete tenant a");

        let tenant_a = store.visible_records_at("docs", "tenant-a", Epoch::new(2));
        assert_eq!(tenant_a.len(), 1);
        assert_eq!(tenant_a[0].header.record_id, "a");
        assert!(store
            .visible_records_at("docs", "tenant-a", Epoch::new(3))
            .is_empty());
        let tenant_b = store.visible_records_at("docs", "tenant-b", Epoch::new(3));
        assert_eq!(tenant_b.len(), 1);
        assert_eq!(tenant_b[0].header.record_id, "b");
    }

    #[test]
    fn apply_replacement_without_return_preserves_visible_replacement_behavior() {
        let schema = schema();
        let mut store = RecordStore::default();
        store
            .apply_replacement(&schema, &record("a", "tenant-a"), Epoch::new(1))
            .expect("insert original");
        let mut replacement = record("a", "tenant-a");
        replacement.fields.insert(
            "body".to_string(),
            Value::String("tenant-a replacement".to_string()),
        );

        store
            .apply_replacement_without_return(&schema, &replacement, Epoch::new(2))
            .expect("replace without returned clone");

        let old_epoch = store.visible_records_at("docs", "tenant-a", Epoch::new(1));
        assert_eq!(old_epoch.len(), 1);
        assert_eq!(
            old_epoch[0].fields.get("body"),
            Some(&Value::String("tenant-a a".to_string()))
        );

        let new_epoch = store.visible_records_at("docs", "tenant-a", Epoch::new(2));
        assert_eq!(new_epoch.len(), 1);
        assert_eq!(
            new_epoch[0].fields.get("body"),
            Some(&Value::String("tenant-a replacement".to_string()))
        );
    }
}
