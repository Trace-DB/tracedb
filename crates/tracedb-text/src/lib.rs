#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tracedb_core::Result;
use tracedb_modules::{
    AccessPathDescriptor, ExplainHookDescriptor, SegmentCodecDescriptor, TraceDbModule,
    TypeDescriptor, WalDecoderDescriptor,
};

pub struct TextModule;

impl TraceDbModule for TextModule {
    fn module_id(&self) -> &str {
        "tracedb-text"
    }

    fn types(&self) -> Vec<TypeDescriptor> {
        vec![TypeDescriptor {
            type_id: "TEXT_INDEXED".to_string(),
        }]
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        vec![AccessPathDescriptor {
            access_path_id: "LexicalPath".to_string(),
            policy_aware: true,
        }]
    }

    fn explain_hooks(&self) -> Vec<ExplainHookDescriptor> {
        vec![ExplainHookDescriptor {
            hook_id: "lexical".to_string(),
        }]
    }

    fn segment_codecs(&self) -> Vec<SegmentCodecDescriptor> {
        vec![SegmentCodecDescriptor {
            codec_id: "text-postings-v1".to_string(),
        }]
    }

    fn wal_decoders(&self) -> Vec<WalDecoderDescriptor> {
        vec![WalDecoderDescriptor {
            decoder_id: "text-wal-v1".to_string(),
        }]
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TextPosting {
    pub record_id: String,
    pub positions: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TextPostingsBlock {
    pub term: String,
    pub postings: Vec<TextPosting>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TextWalEvent {
    pub table: String,
    pub record_id: String,
    pub terms: Vec<String>,
}

pub fn encode_text_postings(block: &TextPostingsBlock) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(block)?)
}

pub fn decode_text_postings(bytes: &[u8]) -> Result<TextPostingsBlock> {
    Ok(serde_json::from_slice(bytes)?)
}

pub fn encode_text_wal_event(event: &TextWalEvent) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(event)?)
}

pub fn decode_text_wal_event(bytes: &[u8]) -> Result<TextWalEvent> {
    Ok(serde_json::from_slice(bytes)?)
}

pub fn tokenize(input: &str) -> Vec<String> {
    input
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

pub fn score_corpus(query: &str, docs: &[(String, String)]) -> Vec<(String, f32)> {
    let query_terms = tokenize(query);
    if query_terms.is_empty() || docs.is_empty() {
        return Vec::new();
    }

    let tokenized_docs: Vec<(String, Vec<String>)> = docs
        .iter()
        .map(|(id, body)| (id.clone(), tokenize(body)))
        .collect();
    let avg_len = tokenized_docs
        .iter()
        .map(|(_, tokens)| tokens.len() as f32)
        .sum::<f32>()
        / tokenized_docs.len() as f32;
    let mut df = BTreeMap::<String, usize>::new();
    for (_, tokens) in &tokenized_docs {
        let unique = tokens.iter().cloned().collect::<BTreeSet<_>>();
        for token in unique {
            *df.entry(token).or_default() += 1;
        }
    }

    tokenized_docs
        .iter()
        .filter_map(|(id, tokens)| {
            let score = bm25(&query_terms, tokens, tokenized_docs.len(), avg_len, &df);
            (score > 0.0).then(|| (id.clone(), score))
        })
        .collect()
}

fn bm25(
    query_terms: &[String],
    tokens: &[String],
    doc_count: usize,
    avg_len: f32,
    df: &BTreeMap<String, usize>,
) -> f32 {
    let mut tf = BTreeMap::<&str, usize>::new();
    for token in tokens {
        *tf.entry(token.as_str()).or_default() += 1;
    }

    let k1 = 1.5;
    let b = 0.75;
    let doc_len = tokens.len() as f32;
    let mut score = 0.0;
    for term in query_terms {
        let freq = *tf.get(term.as_str()).unwrap_or(&0) as f32;
        if freq == 0.0 {
            continue;
        }
        let doc_freq = *df.get(term).unwrap_or(&0) as f32;
        let idf = (((doc_count as f32 - doc_freq + 0.5) / (doc_freq + 0.5)) + 1.0).ln();
        let denom = freq + k1 * (1.0 - b + b * doc_len / avg_len.max(1.0));
        score += idf * (freq * (k1 + 1.0)) / denom;
    }
    score
}
