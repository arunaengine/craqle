use std::cmp::Ordering;
use std::str::FromStr;

use oxrdf::Term;
use oxsdatatypes::{Date, DateTime, Decimal, Double, Float, Integer, Time};

use crate::EncodedTerm;
use crate::store::{TermId, hash_term};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TermKind {
    Iri,
    BlankNode,
    Literal,
}

#[derive(Clone, Debug)]
pub(crate) struct TermMeta {
    pub(crate) kind: TermKind,
    pub(crate) datatype: Option<TermId>,
    pub(crate) language: Option<String>,
    pub(crate) lexical: Option<String>,
    pub(crate) lexical_length: usize,
    comparable: Option<ComparableValue>,
}

#[derive(Clone, Debug)]
enum ComparableValue {
    String(String, Option<String>),
    Integer(Integer),
    Decimal(Decimal),
    Float(Float),
    Double(Double),
    DateTime(DateTime),
    Date(Date),
    Time(Time),
}

impl TermMeta {
    pub(crate) fn from_encoded(term: &EncodedTerm) -> Option<Self> {
        match term.to_term()? {
            Term::NamedNode(node) => {
                let lexical = node.as_str().to_owned();
                Some(Self {
                    kind: TermKind::Iri,
                    datatype: None,
                    language: None,
                    lexical_length: lexical.chars().count(),
                    lexical: Some(lexical.clone()),
                    comparable: Some(ComparableValue::String(lexical, None)),
                })
            }
            Term::BlankNode(_) => Some(Self {
                kind: TermKind::BlankNode,
                datatype: None,
                language: None,
                lexical: None,
                lexical_length: 0,
                comparable: None,
            }),
            Term::Literal(literal) => {
                let lexical = literal.value().to_owned();
                let datatype = EncodedTerm::from_named_node(&literal.datatype().into_owned());
                let datatype_iri = literal.datatype().as_str();
                let language = literal.language().map(|value| value.to_ascii_lowercase());
                Some(Self {
                    kind: TermKind::Literal,
                    datatype: Some(hash_term(&datatype)),
                    language: language.clone(),
                    lexical_length: lexical.chars().count(),
                    comparable: comparable(datatype_iri, &lexical, language.clone()),
                    lexical: Some(lexical),
                })
            }
            Term::Triple(_) => None,
        }
    }

    pub(crate) fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        comparable_cmp(self.comparable.as_ref()?, other.comparable.as_ref()?)
    }
}

fn comparable(datatype: &str, lexical: &str, language: Option<String>) -> Option<ComparableValue> {
    const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
    match datatype.strip_prefix(XSD) {
        Some(
            "integer" | "long" | "int" | "short" | "byte" | "nonPositiveInteger"
            | "negativeInteger" | "nonNegativeInteger" | "positiveInteger" | "unsignedLong"
            | "unsignedInt" | "unsignedShort" | "unsignedByte",
        ) => Integer::from_str(lexical)
            .ok()
            .map(ComparableValue::Integer),
        Some("decimal") => Decimal::from_str(lexical)
            .ok()
            .map(ComparableValue::Decimal),
        Some("float") => Float::from_str(lexical).ok().map(ComparableValue::Float),
        Some("double") => Double::from_str(lexical).ok().map(ComparableValue::Double),
        Some("dateTime" | "dateTimeStamp") => DateTime::from_str(lexical)
            .ok()
            .map(ComparableValue::DateTime),
        Some("date") => Date::from_str(lexical).ok().map(ComparableValue::Date),
        Some("time") => Time::from_str(lexical).ok().map(ComparableValue::Time),
        Some("string") => Some(ComparableValue::String(lexical.to_owned(), None)),
        None if language.is_some() => Some(ComparableValue::String(lexical.to_owned(), language)),
        _ => None,
    }
}

fn comparable_cmp(left: &ComparableValue, right: &ComparableValue) -> Option<Ordering> {
    match (left, right) {
        (
            ComparableValue::String(left, left_language),
            ComparableValue::String(right, right_language),
        ) if left_language == right_language => Some(left.cmp(right)),
        (ComparableValue::Integer(left), ComparableValue::Integer(right)) => {
            left.partial_cmp(right)
        }
        (ComparableValue::Integer(left), ComparableValue::Decimal(right)) => {
            Decimal::from(*left).partial_cmp(right)
        }
        (ComparableValue::Decimal(left), ComparableValue::Integer(right)) => {
            left.partial_cmp(&Decimal::from(*right))
        }
        (ComparableValue::Decimal(left), ComparableValue::Decimal(right)) => {
            left.partial_cmp(right)
        }
        (ComparableValue::Float(left), ComparableValue::Float(right)) => left.partial_cmp(right),
        (ComparableValue::Float(left), ComparableValue::Double(right)) => {
            Double::from(*left).partial_cmp(right)
        }
        (ComparableValue::Float(left), ComparableValue::Integer(right)) => {
            left.partial_cmp(&Float::from(*right))
        }
        (ComparableValue::Float(left), ComparableValue::Decimal(right)) => {
            left.partial_cmp(&Float::from(*right))
        }
        (ComparableValue::Double(left), ComparableValue::Float(right)) => {
            left.partial_cmp(&Double::from(*right))
        }
        (ComparableValue::Double(left), ComparableValue::Double(right)) => left.partial_cmp(right),
        (ComparableValue::Double(left), ComparableValue::Integer(right)) => {
            left.partial_cmp(&Double::from(*right))
        }
        (ComparableValue::Double(left), ComparableValue::Decimal(right)) => {
            left.partial_cmp(&Double::from(*right))
        }
        (ComparableValue::Integer(left), ComparableValue::Float(right)) => {
            Float::from(*left).partial_cmp(right)
        }
        (ComparableValue::Integer(left), ComparableValue::Double(right)) => {
            Double::from(*left).partial_cmp(right)
        }
        (ComparableValue::Decimal(left), ComparableValue::Float(right)) => {
            Float::from(*left).partial_cmp(right)
        }
        (ComparableValue::Decimal(left), ComparableValue::Double(right)) => {
            Double::from(*left).partial_cmp(right)
        }
        (ComparableValue::DateTime(left), ComparableValue::DateTime(right)) => {
            left.partial_cmp(right)
        }
        (ComparableValue::Date(left), ComparableValue::Date(right)) => left.partial_cmp(right),
        (ComparableValue::Time(left), ComparableValue::Time(right)) => left.partial_cmp(right),
        _ => None,
    }
}
