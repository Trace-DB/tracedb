#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BranchState {
    Active,
    Idle,
    Warming,
    BackgroundOnly,
    Suspended,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseCatalogEntry {
    pub org_id: String,
    pub project_id: String,
    pub database_id: String,
    pub name: String,
    pub region: String,
    pub endpoint: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchCatalogEntry {
    pub database_id: String,
    pub branch_id: String,
    pub parent_branch_id: Option<String>,
    pub state: BranchState,
    pub endpoint: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Catalog {
    databases: BTreeMap<String, DatabaseCatalogEntry>,
    branches: BTreeMap<String, BranchCatalogEntry>,
}

impl Catalog {
    pub fn load(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::default());
        }
        let body = fs::read(path)?;
        serde_json::from_slice(&body).map_err(std::io::Error::other)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = tmp_path(path);
        let body = serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?;
        fs::write(&tmp_path, body)?;
        fs::rename(&tmp_path, path)?;
        if let Some(parent) = path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    pub fn create_database(
        &mut self,
        org_id: impl Into<String>,
        project_id: impl Into<String>,
        name: impl Into<String>,
        region: impl Into<String>,
    ) -> Result<DatabaseCatalogEntry, String> {
        let name = name.into();
        let database_id = format!("db_{name}");
        let entry = DatabaseCatalogEntry {
            org_id: org_id.into(),
            project_id: project_id.into(),
            database_id: database_id.clone(),
            name,
            region: region.into(),
            endpoint: format!("https://{database_id}.tracedb.local"),
        };
        self.databases.insert(database_id, entry.clone());
        Ok(entry)
    }

    pub fn create_branch(
        &mut self,
        database_id: &str,
        branch_name: impl Into<String>,
        parent_branch_id: Option<String>,
    ) -> Result<BranchCatalogEntry, String> {
        if !self.databases.contains_key(database_id) {
            return Err(format!("unknown database {database_id}"));
        }
        let branch_name = branch_name.into();
        let branch_id = format!("{database_id}:{branch_name}");
        let entry = BranchCatalogEntry {
            database_id: database_id.to_string(),
            branch_id: branch_id.clone(),
            parent_branch_id,
            state: BranchState::Active,
            endpoint: format!("https://{database_id}.tracedb.local/{branch_name}"),
        };
        self.branches.insert(branch_id, entry.clone());
        Ok(entry)
    }

    pub fn branch(&self, branch_id: &str) -> Option<&BranchCatalogEntry> {
        self.branches.get(branch_id)
    }

    pub fn databases(&self) -> impl Iterator<Item = &DatabaseCatalogEntry> {
        self.databases.values()
    }

    pub fn branches(&self) -> impl Iterator<Item = &BranchCatalogEntry> {
        self.branches.values()
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| format!("{value}.tmp"))
        .unwrap_or_else(|| "tmp".to_string());
    tmp.set_extension(extension);
    tmp
}
