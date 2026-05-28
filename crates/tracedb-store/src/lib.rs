#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReplacementApplyTiming {
    pub validate_identity_ms: f64,
    pub validate_vector_ms: f64,
    pub key_ms: f64,
    pub fields_ms: f64,
    pub finalize_identity_ms: f64,
    pub features_ms: f64,
    pub install_ms: f64,
}

#[derive(Clone, Debug, Default)]
pub struct RecordStore {
    versions: BTreeMap<String, Vec<StoredRecord>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordStoreDelta {
    versions_by_key: BTreeMap<String, Vec<StoredRecord>>,
}

impl RecordStoreDelta {
    pub fn is_empty(&self) -> bool {
        self.versions_by_key.is_empty()
    }

    pub fn merge(&mut self, other: RecordStoreDelta) {
        self.versions_by_key.extend(other.versions_by_key);
    }

    fn versions_for_key(&self, key: &str) -> Option<&Vec<StoredRecord>> {
        self.versions_by_key.get(key)
    }

    fn replace_versions(&mut self, key: String, versions: Vec<StoredRecord>) {
        self.versions_by_key.insert(key, versions);
    }
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

    pub fn plan_replacement(
        &self,
        schema: &TableSchema,
        input: &RecordInput,
        epoch: Epoch,
    ) -> Result<RecordStoreDelta> {
        let mut delta = RecordStoreDelta::default();
        self.plan_replacement_into(&mut delta, schema, input, epoch)?;
        Ok(delta)
    }

    pub fn plan_replacement_with_timing(
        &self,
        schema: &TableSchema,
        input: &RecordInput,
        epoch: Epoch,
    ) -> Result<(RecordStoreDelta, ReplacementApplyTiming)> {
        let mut delta = RecordStoreDelta::default();
        let timing = self.plan_replacement_into_with_timing(&mut delta, schema, input, epoch)?;
        Ok((delta, timing))
    }

    pub fn plan_replacement_into(
        &self,
        delta: &mut RecordStoreDelta,
        schema: &TableSchema,
        input: &RecordInput,
        epoch: Epoch,
    ) -> Result<()> {
        let (key, record) = build_replacement_record(schema, input, epoch)?;
        self.plan_replacement_record(delta, key, record, epoch);
        Ok(())
    }

    pub fn plan_replacement_into_with_timing(
        &self,
        delta: &mut RecordStoreDelta,
        schema: &TableSchema,
        input: &RecordInput,
        epoch: Epoch,
    ) -> Result<ReplacementApplyTiming> {
        let (key, record, mut timing) = build_replacement_record_with_timing(schema, input, epoch)?;
        let install_started = Instant::now();
        self.plan_replacement_record(delta, key, record, epoch);
        timing.install_ms = elapsed_ms(install_started);
        Ok(timing)
    }

    fn plan_replacement_record(
        &self,
        delta: &mut RecordStoreDelta,
        key: String,
        record: StoredRecord,
        epoch: Epoch,
    ) {
        let mut versions = self.versions_for_planning(delta, &key);
        if let Some(current) = versions
            .iter_mut()
            .rev()
            .find(|record| record.header.end_epoch.is_none())
        {
            current.header.end_epoch = Some(epoch);
        }
        versions.push(record);
        delta.replace_versions(key, versions);
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

    pub fn apply_replacement_without_return_with_timing(
        &mut self,
        schema: &TableSchema,
        input: &RecordInput,
        epoch: Epoch,
    ) -> Result<ReplacementApplyTiming> {
        let (key, record, mut timing) = build_replacement_record_with_timing(schema, input, epoch)?;
        let install_started = Instant::now();
        self.install_replacement(key, record, epoch);
        timing.install_ms = elapsed_ms(install_started);
        Ok(timing)
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

    pub fn plan_mutation(
        &self,
        schema: &TableSchema,
        mutation: &RecordInput,
        epoch: Epoch,
    ) -> Result<RecordStoreDelta> {
        let mut delta = RecordStoreDelta::default();
        self.plan_mutation_into(&mut delta, schema, mutation, epoch)?;
        Ok(delta)
    }

    pub fn plan_mutation_into(
        &self,
        delta: &mut RecordStoreDelta,
        schema: &TableSchema,
        mutation: &RecordInput,
        epoch: Epoch,
    ) -> Result<()> {
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
        let mut versions = self.versions_for_planning(delta, &key);
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
        versions.push(record);
        delta.replace_versions(key, versions);
        Ok(())
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

    pub fn plan_delete(
        &self,
        schema: &TableSchema,
        deletion: &RecordDeletion,
        epoch: Epoch,
    ) -> Result<RecordStoreDelta> {
        let mut delta = RecordStoreDelta::default();
        self.plan_delete_into(&mut delta, schema, deletion, epoch)?;
        Ok(delta)
    }

    pub fn plan_delete_into(
        &self,
        delta: &mut RecordStoreDelta,
        schema: &TableSchema,
        deletion: &RecordDeletion,
        epoch: Epoch,
    ) -> Result<()> {
        validate_deletion_identity(schema, deletion)?;
        let key = record_key(&deletion.table, &deletion.tenant_id, &deletion.id);
        let mut versions = self.versions_for_planning(delta, &key);
        if versions.is_empty() {
            return Err(TraceDbError::NotFound(format!(
                "{}:{}:{}",
                deletion.table, deletion.tenant_id, deletion.id
            )));
        }
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
        versions.push(record);
        delta.replace_versions(key, versions);
        Ok(())
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

    pub fn plan_feature_invalidation(
        &self,
        invalidation: &FeatureInvalidation,
        epoch: Epoch,
    ) -> Result<RecordStoreDelta> {
        let mut delta = RecordStoreDelta::default();
        self.plan_feature_invalidation_into(&mut delta, invalidation, epoch)?;
        Ok(delta)
    }

    pub fn plan_feature_invalidation_into(
        &self,
        delta: &mut RecordStoreDelta,
        invalidation: &FeatureInvalidation,
        epoch: Epoch,
    ) -> Result<()> {
        let key = record_key(
            &invalidation.table,
            &invalidation.tenant_id,
            &invalidation.record_id,
        );
        let mut versions = self.versions_for_planning(delta, &key);
        if versions.is_empty() {
            return Err(TraceDbError::NotFound(format!(
                "{}:{}:{}",
                invalidation.table, invalidation.tenant_id, invalidation.record_id
            )));
        }
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
        delta.replace_versions(key, versions);
        Ok(())
    }

    pub fn apply_delta(&mut self, delta: RecordStoreDelta) {
        for (key, versions) in delta.versions_by_key {
            self.versions.insert(key, versions);
        }
    }

    fn versions_for_planning(&self, delta: &RecordStoreDelta, key: &str) -> Vec<StoredRecord> {
        delta
            .versions_for_key(key)
            .cloned()
            .or_else(|| self.versions.get(key).cloned())
            .unwrap_or_default()
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

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn measure_result<T>(slot: Option<&mut f64>, operation: impl FnOnce() -> Result<T>) -> Result<T> {
    if let Some(slot) = slot {
        let started = Instant::now();
        let value = operation()?;
        *slot = elapsed_ms(started);
        Ok(value)
    } else {
        operation()
    }
}

fn measure_value<T>(slot: Option<&mut f64>, operation: impl FnOnce() -> T) -> T {
    if let Some(slot) = slot {
        let started = Instant::now();
        let value = operation();
        *slot = elapsed_ms(started);
        value
    } else {
        operation()
    }
}

fn build_replacement_record(
    schema: &TableSchema,
    input: &RecordInput,
    epoch: Epoch,
) -> Result<(String, StoredRecord)> {
    build_replacement_record_core(schema, input, epoch, None)
}

fn build_replacement_record_with_timing(
    schema: &TableSchema,
    input: &RecordInput,
    epoch: Epoch,
) -> Result<(String, StoredRecord, ReplacementApplyTiming)> {
    let mut timing = ReplacementApplyTiming::default();
    let (key, record) = build_replacement_record_core(schema, input, epoch, Some(&mut timing))?;
    Ok((key, record, timing))
}

fn build_replacement_record_core(
    schema: &TableSchema,
    input: &RecordInput,
    epoch: Epoch,
    mut timing: Option<&mut ReplacementApplyTiming>,
) -> Result<(String, StoredRecord)> {
    measure_result(
        timing
            .as_mut()
            .map(|timing| &mut (**timing).validate_identity_ms),
        || validate_record_identity(schema, input, None),
    )?;

    measure_result(
        timing
            .as_mut()
            .map(|timing| &mut (**timing).validate_vector_ms),
        || validate_vector_dimensions(schema, input),
    )?;

    let (tenant_id, key) =
        measure_result(timing.as_mut().map(|timing| &mut (**timing).key_ms), || {
            let tenant_id = if input.tenant_id.is_empty() {
                return Err(TraceDbError::InvalidRecord(
                    "tenant id cannot be empty".to_string(),
                ));
            } else {
                input.tenant_id.clone()
            };
            let key = record_key(&input.table, &tenant_id, &input.id);
            Ok((tenant_id, key))
        })?;

    let fields = measure_value(
        timing.as_mut().map(|timing| &mut (**timing).fields_ms),
        || {
            let mut fields = input.fields.clone();
            fields.insert(
                schema.primary_id_column.clone(),
                Value::String(input.id.clone()),
            );
            fields.insert(
                schema.tenant_id_column.clone(),
                Value::String(tenant_id.clone()),
            );
            fields
        },
    );

    measure_result(
        timing
            .as_mut()
            .map(|timing| &mut (**timing).finalize_identity_ms),
        || validate_record_identity(schema, input, Some(&fields)),
    )?;

    let record = measure_value(
        timing.as_mut().map(|timing| &mut (**timing).features_ms),
        || {
            let features = build_features(schema, input, &fields, None, epoch);
            StoredRecord {
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
            }
        },
    );

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

    #[test]
    fn timed_and_untimed_replacement_paths_remain_equivalent() {
        let schema = schema();
        let mut untimed = RecordStore::default();
        let mut timed = RecordStore::default();
        let original = record("a", "tenant-a");
        untimed
            .apply_replacement(&schema, &original, Epoch::new(1))
            .expect("untimed insert original");
        timed
            .apply_replacement(&schema, &original, Epoch::new(1))
            .expect("timed insert original");

        let mut replacement = record("a", "tenant-a");
        replacement.fields.insert(
            "body".to_string(),
            Value::String("tenant-a timed equivalent replacement".to_string()),
        );
        untimed
            .apply_replacement_without_return(&schema, &replacement, Epoch::new(2))
            .expect("untimed replacement");
        let timing = timed
            .apply_replacement_without_return_with_timing(&schema, &replacement, Epoch::new(2))
            .expect("timed replacement");

        let subphase_ms = timing.validate_identity_ms
            + timing.validate_vector_ms
            + timing.key_ms
            + timing.fields_ms
            + timing.finalize_identity_ms
            + timing.features_ms
            + timing.install_ms;
        assert!(subphase_ms > 0.0);
        assert_eq!(
            untimed.visible_records_at("docs", "tenant-a", Epoch::new(1)),
            timed.visible_records_at("docs", "tenant-a", Epoch::new(1))
        );
        assert_eq!(
            untimed.visible_records_at("docs", "tenant-a", Epoch::new(2)),
            timed.visible_records_at("docs", "tenant-a", Epoch::new(2))
        );
        assert_eq!(
            untimed.checkpoint_records(Epoch::new(2)),
            timed.checkpoint_records(Epoch::new(2))
        );
    }

    #[test]
    fn record_store_delta_plan_does_not_mutate_until_applied() {
        let schema = schema();
        let mut store = RecordStore::default();
        store
            .apply_replacement(&schema, &record("a", "tenant-a"), Epoch::new(1))
            .expect("seed original");

        let mut replacement = record("a", "tenant-a");
        replacement.fields.insert(
            "body".to_string(),
            Value::String("planned body".to_string()),
        );
        let delta = store
            .plan_replacement(&schema, &replacement, Epoch::new(2))
            .expect("plan replacement");

        let before_apply = store.visible_records_at("docs", "tenant-a", Epoch::new(2));
        assert_eq!(before_apply.len(), 1);
        assert_eq!(
            before_apply[0].fields.get("body"),
            Some(&Value::String("tenant-a a".to_string()))
        );

        store.apply_delta(delta);

        let after_apply = store.visible_records_at("docs", "tenant-a", Epoch::new(2));
        assert_eq!(after_apply.len(), 1);
        assert_eq!(
            after_apply[0].fields.get("body"),
            Some(&Value::String("planned body".to_string()))
        );
    }

    #[test]
    fn record_store_delta_apply_matches_existing_helpers_for_writes_and_feature_status() {
        let schema = schema();
        let mut helper = RecordStore::default();
        let mut delta = RecordStore::default();
        let original = record("a", "tenant-a");
        helper
            .apply_replacement(&schema, &original, Epoch::new(1))
            .expect("helper seed");
        delta.apply_delta(
            delta
                .plan_replacement(&schema, &original, Epoch::new(1))
                .expect("delta seed"),
        );

        let mut patch = RecordInput {
            table: "docs".to_string(),
            id: "a".to_string(),
            tenant_id: "tenant-a".to_string(),
            fields: Map::new(),
        };
        patch.fields.insert(
            "body".to_string(),
            Value::String("patched body".to_string()),
        );
        helper
            .apply_mutation(&schema, &patch, Epoch::new(2))
            .expect("helper patch");
        delta.apply_delta(
            delta
                .plan_mutation(&schema, &patch, Epoch::new(2))
                .expect("delta patch"),
        );

        let invalidation = FeatureInvalidation {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            record_id: "a".to_string(),
            feature: "embedding".to_string(),
            status: FeatureStatus::Pending,
        };
        helper
            .apply_feature_invalidation(&invalidation, Epoch::new(3))
            .expect("helper feature status");
        delta.apply_delta(
            delta
                .plan_feature_invalidation(&invalidation, Epoch::new(3))
                .expect("delta feature status"),
        );

        let deletion = RecordDeletion {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            id: "a".to_string(),
            tombstone: "delete".to_string(),
        };
        helper
            .apply_delete(&schema, &deletion, Epoch::new(4))
            .expect("helper delete");
        delta.apply_delta(
            delta
                .plan_delete(&schema, &deletion, Epoch::new(4))
                .expect("delta delete"),
        );

        assert_eq!(
            helper.visible_records_at("docs", "tenant-a", Epoch::new(3)),
            delta.visible_records_at("docs", "tenant-a", Epoch::new(3))
        );
        assert_eq!(
            helper.feature_state("docs", "tenant-a", "a", "embedding", Epoch::new(3)),
            delta.feature_state("docs", "tenant-a", "a", "embedding", Epoch::new(3))
        );
        assert_eq!(
            helper.visible_records_at("docs", "tenant-a", Epoch::new(4)),
            delta.visible_records_at("docs", "tenant-a", Epoch::new(4))
        );
        assert_eq!(
            helper.checkpoint_records(Epoch::new(4)),
            delta.checkpoint_records(Epoch::new(4))
        );
    }
}
