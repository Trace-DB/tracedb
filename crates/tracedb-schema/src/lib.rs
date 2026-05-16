#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LogicalType {
    Bool,
    Int64,
    Float64,
    Text,
    TextIndexed,
    Json,
    Id,
    Timestamp,
    BlobRef,
    Vector {
        element: String,
        dimensions: usize,
        metric: String,
    },
    SparseVector,
    MultiVector,
    Edge {
        target_table: String,
    },
    TemporalRange,
    Policy,
    Provenance,
    ActivationState,
    SuppressionState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub name: String,
    pub logical_type: LogicalType,
    pub primary: bool,
    pub tenant: bool,
    pub nullable: bool,
}

impl ColumnDescriptor {
    pub fn new(name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self {
            name: name.into(),
            logical_type,
            primary: false,
            tenant: false,
            nullable: true,
        }
    }

    pub fn primary(name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self {
            primary: true,
            nullable: false,
            ..Self::new(name, logical_type)
        }
    }

    pub fn tenant(name: impl Into<String>) -> Self {
        Self {
            tenant: true,
            nullable: false,
            ..Self::new(name, LogicalType::Id)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexDescriptor {
    pub name: String,
    pub kind: String,
    pub columns: Vec<String>,
    pub policy_aware: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FeatureDescriptor {
    pub name: String,
    pub target_column: String,
    pub source_columns: Vec<String>,
    pub model_id: String,
}

impl FeatureDescriptor {
    pub fn embedding(
        target_column: impl Into<String>,
        source_columns: Vec<String>,
        model_id: impl Into<String>,
    ) -> Self {
        let target_column = target_column.into();
        Self {
            name: format!("{target_column}_feature"),
            target_column,
            source_columns,
            model_id: model_id.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyDescriptor {
    pub name: String,
    pub required_tenant_column: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EdgeTableDescriptor {
    pub name: String,
    pub target_table: String,
}

impl EdgeTableDescriptor {
    pub fn new(name: impl Into<String>, target_table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            target_table: target_table.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModuleRequirement {
    pub module_id: String,
    pub min_version: String,
}

impl ModuleRequirement {
    pub fn new(module_id: impl Into<String>, min_version: impl Into<String>) -> Self {
        Self {
            module_id: module_id.into(),
            min_version: min_version.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableDescriptor {
    pub table_id: String,
    pub name: String,
    pub schema_version: u64,
    pub columns: Vec<ColumnDescriptor>,
    pub indexes: Vec<IndexDescriptor>,
    pub features: Vec<FeatureDescriptor>,
    pub policies: Vec<PolicyDescriptor>,
    pub edge_tables: Vec<EdgeTableDescriptor>,
    pub module_requirements: Vec<ModuleRequirement>,
}

impl TableDescriptor {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            table_id: format!("table:{name}"),
            name,
            schema_version: 1,
            columns: Vec::new(),
            indexes: Vec::new(),
            features: Vec::new(),
            policies: Vec::new(),
            edge_tables: Vec::new(),
            module_requirements: Vec::new(),
        }
    }

    pub fn column(mut self, column: ColumnDescriptor) -> Self {
        self.columns.push(column);
        self
    }

    pub fn feature(mut self, feature: FeatureDescriptor) -> Self {
        self.features.push(feature);
        self
    }

    pub fn edge_table(mut self, edge_table: EdgeTableDescriptor) -> Self {
        self.edge_tables.push(edge_table);
        self
    }

    pub fn module_requirement(mut self, requirement: ModuleRequirement) -> Self {
        self.module_requirements.push(requirement);
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("table name cannot be empty".to_string());
        }
        if !self.columns.iter().any(|column| column.primary) {
            return Err(format!("table {} has no primary id column", self.name));
        }
        if !self.columns.iter().any(|column| column.tenant) {
            return Err(format!("table {} has no tenant column", self.name));
        }
        for column in &self.columns {
            if let LogicalType::Vector { dimensions, .. } = column.logical_type {
                if dimensions == 0 {
                    return Err(format!("vector column {} has zero dimensions", column.name));
                }
            }
        }
        for feature in &self.features {
            if !self
                .columns
                .iter()
                .any(|column| column.name == feature.target_column)
            {
                return Err(format!(
                    "feature {} targets missing column {}",
                    feature.name, feature.target_column
                ));
            }
            for source in &feature.source_columns {
                if !self.columns.iter().any(|column| column.name == *source) {
                    return Err(format!(
                        "feature {} source {source} is missing",
                        feature.name
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn requires_module(&self, module_id: &str) -> bool {
        self.module_requirements
            .iter()
            .any(|requirement| requirement.module_id == module_id)
    }
}
