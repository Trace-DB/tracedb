#![forbid(unsafe_code)]

use tracedb_core::ModuleManifest;

pub fn standard_module_manifest_ids() -> Vec<String> {
    standard_module_manifests()
        .into_iter()
        .map(|manifest| manifest.module_id)
        .collect()
}

pub fn standard_module_manifests() -> Vec<ModuleManifest> {
    [
        "tracedb-text",
        "tracedb-vector",
        "tracedb-graph",
        "tracedb-temporal",
        "tracedb-policy",
        "tracedb-provenance",
        "tracedb-features",
        "tracedb-retrieval-core",
    ]
    .into_iter()
    .map(|module_id| ModuleManifest {
        module_id: module_id.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        trust_level: "FIRST_PARTY_SIGNED".to_string(),
    })
    .collect()
}
