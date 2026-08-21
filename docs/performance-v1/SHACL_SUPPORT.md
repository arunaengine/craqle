# CraqleFastV1 support boundary

`CraqleFastV1` is Craqle's bounded native SHACL profile. It is available with
the optional `shacl-core` feature and is not a claim of unrestricted SHACL
Core conformance.

## Execution contract

Rudof 0.3.8 parses shapes with its SPARQL service features disabled. Craqle
lowers the parsed graph to an immutable local plan and evaluates data through
its indexed RDF read views. Production validation does not export or copy the
complete data graph. The bounded root and enabled local-import closure is
materialized only for compilation; `owl:imports` discovery uses a
predicate-bound cursor.

Public validation returns one complete deterministic report or one error. It
does not expose row-by-row SPARQL or SHACL results. The boolean conformance API
may stop at the first violation without exposing a partial report.

Compiled plans are keyed by the canonical shapes digest, compiler model
version, RO-Crate version, Rudof version, and enabled extensions. A persisted
report is current only while its data version, every recorded root/import
version, and compiler model version still match.

## Supported RO-Crate rules

RO-Crate 1.1, 1.2, and 1.3 currently use the same six native rule bodies:

1. root data entity;
2. metadata descriptor;
3. required `schema:name`, `schema:description`, and `schema:datePublished`;
4. exact `datePublished` cardinality;
5. entity `rdf:type`; and
6. reachability.

This is an implementation fact, not a claim that the specifications are
interchangeable. Candidate metadata and root `dcterms:conformsTo` markers
select the supported version.

## Supported SHACL forms

Targets:

- `sh:targetNode`;
- `sh:targetClass`;
- `sh:targetSubjectsOf`;
- `sh:targetObjectsOf`; and
- Rudof's implicit class target for a node shape declared as an RDF class.

Property paths:

- direct predicate;
- inverse;
- sequence;
- alternative;
- zero-or-one;
- zero-or-more; and
- one-or-more.

Constraints:

- `sh:minCount` and `sh:maxCount`;
- `sh:datatype`, `sh:nodeKind`, and exact `sh:class` membership;
- `sh:hasValue` and `sh:in`;
- numeric bounds, string length, pattern, and flags;
- `sh:languageIn` and `sh:uniqueLang`;
- `sh:closed` and `sh:ignoredProperties`;
- equals, disjoint, less-than, and less-than-or-equals pairs;
- `sh:node`, `sh:and`, `sh:or`, `sh:not`, and `sh:xone`; and
- qualified value shape, count, and disjointness constraints.

Shape deactivation, severity, and messages are retained. Reports identify the
focus node, optional value and result path, source shape and constraint
component, severity, and messages, then normalize results deterministically.

## Limits and explicit failures

- `sh:class` does not perform RDFS subclass inference.
- A property shape must have exactly one well-formed `sh:path`.
- Only direct-predicate property paths extend a closed shape's allow-list.
- Logical and nested cycles are errors.
- Defaults are 10,000 results, 1,000,000 path edges, and depth 128.
- Budget exhaustion, cancellation, invalid regular expressions, stale schema
  versions, and unavailable forced-delta work return errors instead of
  truncating or claiming conformance.
- `Auto` chooses delta or full work from conservative estimates. `ForceDelta`
  and `ForceFull` execute the requested path or return an explicit error.

Local imports are opt-in, must name existing Craqle graphs, and participate in
version fences. Missing imports, import cycles, disabled imports, and remote or
network-loaded imports are errors.

## Unsupported components

Compilation rejects, rather than ignores:

- SHACL-SPARQL (`sh:sparql`);
- SHACL-JS;
- SHACL-AF rules and expressions;
- custom constraint components and custom targets;
- reifier shapes;
- RDF-star shape terms; and
- remote or network-loaded `owl:imports`.

The profile should continue to be identified as `CraqleFastV1` until a
separately recorded complete SHACL Core conformance run exists.
