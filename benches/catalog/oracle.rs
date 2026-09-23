//! Derives exact expected read answers from the catalog fixture and checks results against them.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::BTreeSet;

use craqle::QueryResults;

const INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

/// Shape of the seeded catalog fixture: `rows` subjects spread across `graphs` graphs.
#[derive(Clone, Copy, Debug)]
pub struct Fixture {
    pub seed: usize,
    pub rows: usize,
    pub graphs: usize,
}

impl Fixture {
    pub fn subject(&self, index: usize) -> String {
        format!("urn:catalog:s:{}:{index}", self.seed)
    }

    pub fn object(&self, index: usize) -> String {
        format!("\"catalog token {} {index}\"", self.seed)
    }

    pub fn graph(&self, index: usize) -> String {
        format!("urn:bench:catalog:{}", index % self.graphs.max(1))
    }
}

/// The complete answer a correct engine must return for one read.
#[derive(Clone, Debug)]
pub enum Answer {
    /// Every row, compared as a multiset.
    Exact(Vec<String>),
    /// Every row in this order.
    Ordered(Vec<String>),
    /// Exactly `size` distinct rows chosen from `pool`, in any order.
    Choice { pool: BTreeSet<String>, size: usize },
    /// Exactly `size` distinct `(graph, subject)` text hits chosen from `pool`.
    Hits {
        pool: BTreeSet<(String, String)>,
        size: usize,
    },
}

/// A read's SPARQL text, its projected variables, and its independently derived answer.
#[derive(Clone, Debug)]
pub struct Expected {
    pub text: String,
    pub variables: &'static [&'static str],
    pub answer: Answer,
}

impl Expected {
    pub fn rows(&self) -> usize {
        match &self.answer {
            Answer::Exact(rows) | Answer::Ordered(rows) => rows.len(),
            Answer::Choice { size, .. } | Answer::Hits { size, .. } => *size,
        }
    }
}

/// Builds each named read over the fixture, with its answer derived from the generator.
pub fn expected(query: &str, fixture: &Fixture) -> Result<Expected, String> {
    let seed = fixture.seed;
    let all = 0..fixture.rows;
    let page = fixture.rows.min(100);
    let subjects = |indexes: &mut dyn Iterator<Item = usize>| {
        indexes
            .map(|index| format!("s=<{}>", fixture.subject(index)))
            .collect::<Vec<_>>()
    };
    let (text, variables, answer): (String, &'static [&'static str], Answer) = match query {
        "fts" => (
            "fts:catalog".to_owned(),
            &[],
            Answer::Hits {
                pool: all
                    .map(|index| (fixture.graph(index), fixture.subject(index)))
                    .collect(),
                size: page,
            },
        ),
        "values" => (
            format!(
                "SELECT ?s WHERE {{ VALUES ?s {{ <{}> }} ?s ?p ?o }}",
                fixture.subject(0)
            ),
            &["s"],
            Answer::Exact(subjects(&mut std::iter::once(0))),
        ),
        "join" | "string" => (
            if query == "join" {
                "SELECT ?s WHERE { ?s <urn:catalog:p> ?o . ?s <urn:catalog:p> ?x }".to_owned()
            } else {
                "SELECT ?s WHERE { ?s <urn:catalog:p> ?o FILTER(CONTAINS(STR(?o), \"token\")) }"
                    .to_owned()
            },
            &["s"],
            Answer::Exact(subjects(&mut all.clone())),
        ),
        "sort" => {
            let mut order: Vec<usize> = all.collect();
            order.sort_by_key(|index| fixture.object(*index));
            (
                "SELECT ?s WHERE { ?s <urn:catalog:p> ?o } ORDER BY ?o LIMIT 100".to_owned(),
                &["s"],
                Answer::Ordered(subjects(&mut order.into_iter().take(page))),
            )
        }
        "distinct" => (
            "SELECT DISTINCT ?o WHERE { ?s <urn:catalog:p> ?o }".to_owned(),
            &["o"],
            Answer::Exact(
                all.map(|index| format!("o={}", fixture.object(index)))
                    .collect(),
            ),
        ),
        "group" => (
            "SELECT (COUNT(?s) AS ?count) WHERE { ?s <urn:catalog:p> ?o }".to_owned(),
            &["count"],
            Answer::Exact(vec![format!("count=\"{}\"^^<{INTEGER}>", fixture.rows)]),
        ),
        "high" => (
            format!("SELECT ?s WHERE {{ ?s <urn:catalog:p> \"catalog token {seed} 0\" }}"),
            &["s"],
            Answer::Exact(subjects(&mut std::iter::once(0))),
        ),
        // A lookup that must find nothing, kept apart from the positive cases.
        "absent" => (
            "SELECT ?s WHERE { ?s <urn:catalog:p> \"catalog token absent\" }".to_owned(),
            &["s"],
            Answer::Exact(Vec::new()),
        ),
        "path" | "default" | "low" => (
            if query == "path" {
                "SELECT ?s WHERE { ?s <urn:catalog:p>+ ?o } LIMIT 100".to_owned()
            } else {
                "SELECT ?s WHERE { ?s <urn:catalog:p> ?o } LIMIT 100".to_owned()
            },
            &["s"],
            Answer::Choice {
                pool: subjects(&mut all.clone()).into_iter().collect(),
                size: page,
            },
        ),
        other => return Err(format!("query={other} has no expected answer")),
    };
    Ok(Expected {
        text,
        variables,
        answer,
    })
}

/// Every solution as `name=term` cells in projection order, keeping unbound variables,
/// datatypes, language tags, and duplicate rows.
pub fn canonical_rows(results: &QueryResults, variables: &[&str]) -> Result<Vec<String>, String> {
    let QueryResults::Solutions(solutions) = results else {
        return Err(format!("expected solutions, got {results:?}"));
    };
    solutions
        .iter()
        .map(|row| {
            if let Some(extra) = row.keys().find(|name| !variables.contains(&name.as_str())) {
                return Err(format!("unexpected variable {extra}"));
            }
            Ok(variables
                .iter()
                .map(|name| match row.get(*name) {
                    Some(term) => format!("{name}={}", term.0),
                    None => format!("{name}=UNBOUND"),
                })
                .collect::<Vec<_>>()
                .join(" "))
        })
        .collect()
}

/// Checks rows against the expected answer and returns a digest of the accepted answer.
pub fn check_rows(expected: &Expected, rows: Vec<String>) -> Result<String, String> {
    let mut rows = rows;
    match &expected.answer {
        Answer::Exact(answer) => {
            let mut answer = answer.clone();
            answer.sort();
            rows.sort();
            if rows != answer {
                return Err(difference(&answer, &rows));
            }
        }
        Answer::Ordered(answer) => {
            if &rows != answer {
                return Err(difference(answer, &rows));
            }
        }
        Answer::Choice { pool, size } => {
            let distinct: BTreeSet<&String> = rows.iter().collect();
            if rows.len() != *size || distinct.len() != rows.len() {
                return Err(format!(
                    "expected {size} distinct rows, got {} rows with {} distinct",
                    rows.len(),
                    distinct.len()
                ));
            }
            if let Some(foreign) = rows.iter().find(|row| !pool.contains(*row)) {
                return Err(format!("{foreign} is not an allowed row"));
            }
            // Different valid selections are equal answers.
            return Ok(format!("choice:{size}"));
        }
        Answer::Hits { .. } => return Err("text hits are checked with check_hits".to_owned()),
    }
    Ok(blake3::hash(rows.join("\n").as_bytes())
        .to_hex()
        .to_string())
}

/// Checks text hits by identity, graph and subject pairing, distinctness, and count.
pub fn check_hits(expected: &Expected, hits: &[(String, String)]) -> Result<String, String> {
    let Answer::Hits { pool, size } = &expected.answer else {
        return Err("rows are checked with check_rows".to_owned());
    };
    let distinct: BTreeSet<&(String, String)> = hits.iter().collect();
    if hits.len() != *size || distinct.len() != hits.len() {
        return Err(format!(
            "expected {size} distinct hits, got {} hits with {} distinct",
            hits.len(),
            distinct.len()
        ));
    }
    if let Some(foreign) = hits.iter().find(|hit| !pool.contains(*hit)) {
        return Err(format!(
            "{foreign:?} is not a fixture resource in its graph"
        ));
    }
    let lines: Vec<String> = hits
        .iter()
        .map(|(graph, subject)| format!("{graph}\u{1f}{subject}"))
        .collect();
    Ok(blake3::hash(lines.join("\n").as_bytes())
        .to_hex()
        .to_string())
}

fn difference(expected: &[String], actual: &[String]) -> String {
    let position = expected
        .iter()
        .zip(actual)
        .position(|(left, right)| left != right)
        .unwrap_or(expected.len().min(actual.len()));
    format!(
        "{} rows expected, {} returned; first difference at {position}: expected {:?}, got {:?}",
        expected.len(),
        actual.len(),
        expected.get(position),
        actual.get(position)
    )
}
