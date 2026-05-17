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
    score_corpus_with_stats(query, docs).scores
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TextScoreReport {
    pub scores: Vec<(String, f32)>,
    pub tokenized_documents: usize,
    pub scored_documents: usize,
}

pub fn score_corpus_with_stats(query: &str, docs: &[(String, String)]) -> TextScoreReport {
    let query_terms = tokenize(query);
    if query_terms.is_empty() || docs.is_empty() {
        return TextScoreReport::default();
    }

    let query_term_set = query_terms.iter().cloned().collect::<BTreeSet<_>>();
    let mut total_len = 0usize;
    let mut df = BTreeMap::<String, usize>::new();
    let mut matching_docs = Vec::<QueryScopedDoc>::new();
    for (id, body) in docs {
        let tokens = tokenize(body);
        total_len += tokens.len();
        let mut tf = BTreeMap::<String, usize>::new();
        for token in &tokens {
            if query_term_set.contains(token) {
                *tf.entry(token.clone()).or_default() += 1;
            }
        }
        if !tf.is_empty() {
            for term in tf.keys() {
                *df.entry(term.clone()).or_default() += 1;
            }
            matching_docs.push(QueryScopedDoc {
                id: id.clone(),
                doc_len: tokens.len(),
                tf,
            });
        }
    }
    let avg_len = total_len as f32 / docs.len() as f32;
    let scores = matching_docs
        .iter()
        .filter_map(|doc| {
            let score = bm25_from_term_frequency(
                &query_terms,
                &doc.tf,
                doc.doc_len,
                docs.len(),
                avg_len,
                &df,
            );
            (score > 0.0).then(|| (doc.id.clone(), score))
        })
        .collect::<Vec<_>>();

    TextScoreReport {
        scored_documents: scores.len(),
        tokenized_documents: docs.len(),
        scores,
    }
}

#[derive(Clone, Debug, PartialEq)]
struct QueryScopedDoc {
    id: String,
    doc_len: usize,
    tf: BTreeMap<String, usize>,
}

fn bm25_from_term_frequency(
    query_terms: &[String],
    tf: &BTreeMap<String, usize>,
    doc_len: usize,
    doc_count: usize,
    avg_len: f32,
    df: &BTreeMap<String, usize>,
) -> f32 {
    let k1 = 1.5;
    let b = 0.75;
    let doc_len = doc_len as f32;
    let mut score = 0.0;
    for term in query_terms {
        let freq = *tf.get(term).unwrap_or(&0) as f32;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_corpus_with_stats_reports_only_matching_documents_scored() {
        let docs = vec![
            ("target".to_string(), "alpha beta raretoken".to_string()),
            ("distractor-1".to_string(), "alpha beta gamma".to_string()),
            ("distractor-2".to_string(), "delta epsilon zeta".to_string()),
        ];

        let report = score_corpus_with_stats("raretoken", &docs);

        assert_eq!(report.tokenized_documents, 3);
        assert_eq!(report.scored_documents, 1);
        assert_eq!(
            report
                .scores
                .first()
                .map(|(record_id, _)| record_id.as_str()),
            Some("target")
        );
    }
}
