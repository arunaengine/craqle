"""Portable RO-Crate query cases with answers derived from the fixture model, not an engine."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import re

from fixture import (FORMATS, LICENSES, SCHEMA, WORDS, XSD, crate_triples, iri, literal,
                     org_name, person_name)

S = f"<{SCHEMA}"
INTEGER = f"{XSD}integer"
TEXT_PREDICATES = ("name", "description", "keywords", "identifier")


def count(value):
    return literal(str(value), INTEGER)


def rows(*columns):
    """Rows of `name=term` cells in projection order, the form both adapters report."""
    return [" ".join(f"{name}={term}" for name, term in row) for row in columns]


def sparql(case_id, text, variables, expected, **extra):
    case = {"id": case_id, "kind": "sparql", "sparql": " ".join(text.split()),
            "variables": variables, "expected": expected,
            "compare": "ordered" if "ORDER BY" in text else "exact", "graphs": None}
    case.update(extra)
    return case


def tokens(text):
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def text_documents(model):
    """Searchable text per (graph, subject): name, description, keywords, and identifier."""
    documents = {}
    for crate in model.crates:
        for subject, predicate, obj in crate_triples(model, crate, iri(crate.base)):
            name = predicate[len(S):-1] if predicate.startswith(S) else None
            if name in TEXT_PREDICATES and obj.startswith('"'):
                value = obj[1:obj.rindex('"')]
                documents.setdefault((crate.base, subject[1:-1]), set()).update(tokens(value))
    return documents


def matches(documents, terms, readable=None):
    wanted = tokens(terms)
    return sorted(key for key, words in documents.items()
                  if words & wanted and (readable is None or key[0] in readable))


def pick(model, count_of, rare):
    """The most or least frequent value, with ties broken by value."""
    ranked = sorted(count_of.items(), key=lambda item: (item[1], item[0]))
    return (ranked[0] if rare else ranked[-1])[0]


def build(model):
    """Every case of the catalog for one fixture model."""
    crates = model.crates
    target = crates[min(7, len(crates) - 1)]
    unlicensed = next(crate for crate in crates if crate.license is None)
    cases = []
    graph = target.base

    for crate in (target, unlicensed):
        cases.append(sparql(
            f"RC01-open-{crate.index}",
            f"""SELECT ?name ?date ?license WHERE {{ GRAPH <{crate.base}> {{
                ?d {S}about> <{crate.base}> . <{crate.base}> a {S}Dataset> ;
                {S}name> ?name ; {S}datePublished> ?date .
                OPTIONAL {{ <{crate.base}> {S}license> ?license }} }} }}""",
            ["name", "date", "license"],
            rows([("name", literal(crate.name)), ("date", literal(crate.date)),
                  ("license", iri(crate.license) if crate.license else "UNBOUND")])))

    direct = sorted(target.files[: len(target.files) // 2], key=lambda file: file.iri)
    page = direct[2:7]
    cases.append(sparql(
        "RC02-files-page",
        f"""SELECT ?file ?name ?format ?size WHERE {{ GRAPH <{graph}> {{
            <{graph}> {S}hasPart> ?file . ?file a {S}MediaObject> ; {S}name> ?name ;
            {S}encodingFormat> ?format ; {S}contentSize> ?size }} }}
            ORDER BY ?file LIMIT 5 OFFSET 2""",
        ["file", "name", "format", "size"],
        rows(*[[("file", iri(file.iri)), ("name", literal(file.name)),
                ("format", literal(file.format)),
                ("size", literal(str(file.size), INTEGER))] for file in page])))

    cases.append(sparql(
        "RC03-keywords",
        f"""SELECT ?keyword WHERE {{ GRAPH <{graph}> {{ <{graph}> {S}keywords> ?keyword }} }}
            ORDER BY ?keyword""",
        ["keyword"], rows(*[[("keyword", literal(word))] for word in target.keywords])))
    authors = sorted(model.people[person] for person in target.authors)
    for label, limit in (("complete", ""), ("first", "LIMIT 1")):
        chosen = authors[:1] if limit else authors
        cases.append(sparql(
            f"RC03-authors-{label}",
            f"""SELECT ?author ?name WHERE {{ GRAPH <{graph}> {{ <{graph}> {S}author> ?author .
                ?author {S}name> ?name }} }} ORDER BY ?author {limit}""",
            ["author", "name"],
            rows(*[[("author", iri(author)), ("name", literal(person_name(model, author)))]
                   for author in chosen])))

    authored = {}
    for crate in crates:
        for person in crate.authors:
            authored.setdefault(model.people[person], set()).add(crate.base)
    for label, rare in (("common", False), ("rare", True)):
        person = pick(model, {key: len(value) for key, value in authored.items()}, rare)
        cases.append(sparql(
            f"RC04-person-{label}",
            f"""SELECT DISTINCT ?g WHERE {{ GRAPH ?g {{ ?d {S}about> ?g .
                ?g {S}author> <{person}> }} }} ORDER BY ?g""",
            ["g"], rows(*[[("g", iri(base))] for base in sorted(authored[person])])))
    by_org = {}
    for crate in crates:
        for person in crate.authors:
            by_org.setdefault(model.orgs[model.affiliation[person]], set()).add(crate.base)
    for label, rare in (("common", False), ("rare", True)):
        org = pick(model, {key: len(value) for key, value in by_org.items()}, rare)
        cases.append(sparql(
            f"RC04-org-{label}",
            f"""SELECT DISTINCT ?g WHERE {{ GRAPH ?g {{ ?d {S}about> ?g . ?g {S}author> ?p .
                ?p {S}affiliation> <{org}> }} }} ORDER BY ?g""",
            ["g"], rows(*[[("g", iri(base))] for base in sorted(by_org[org])])))

    keyword, license, media = WORDS[0], LICENSES[0], FORMATS[0]
    found = sorted(crate.base for crate in crates
                   if keyword in crate.keywords and crate.license == license
                   and any(file.format == media for file in crate.files[: len(crate.files) // 2]))
    cases.append(sparql(
        "RC05-match",
        f"""SELECT DISTINCT ?g WHERE {{ GRAPH ?g {{ ?d {S}about> ?g . ?g {S}keywords> "{keyword}" ;
            {S}license> <{license}> ; {S}hasPart> ?f . ?f {S}encodingFormat> "{media}" }} }}
            ORDER BY ?g""",
        ["g"], rows(*[[("g", iri(base))] for base in found])))

    recent = sorted((crate for crate in crates if crate.date >= "2024-01-01"),
                    key=lambda crate: (crate.date, crate.base))
    recent = sorted(recent, key=lambda crate: crate.date, reverse=True)[:10]
    cases.append(sparql(
        "RC06-recent",
        f"""SELECT ?g ?date WHERE {{ GRAPH ?g {{ ?d {S}about> ?g . ?g {S}datePublished> ?date }}
            FILTER(?date >= "2024-01-01") }} ORDER BY DESC(?date) ?g LIMIT 10""",
        ["g", "date"],
        rows(*[[("g", iri(crate.base)), ("date", literal(crate.date))] for crate in recent])))
    # The same range over the lexical form, for engines that type date-shaped strings.
    cases.append(sparql(
        "RC06-recent-str",
        f"""SELECT ?g ?date WHERE {{ GRAPH ?g {{ ?d {S}about> ?g . ?g {S}datePublished> ?date }}
            FILTER(STR(?date) >= "2024-01-01") }} ORDER BY DESC(?date) ?g LIMIT 10""",
        ["g", "date"],
        rows(*[[("g", iri(crate.base)), ("date", literal(crate.date))] for crate in recent])))

    facets = {"license": {}, "format": {}, "keyword": {}}
    for crate in crates:
        if crate.license:
            facets["license"].setdefault(iri(crate.license), set()).add(crate.base)
        for file in crate.files[: len(crate.files) // 2]:
            facets["format"].setdefault(literal(file.format), set()).add(crate.base)
        for word in crate.keywords:
            facets["keyword"].setdefault(literal(word), set()).add(crate.base)
    patterns = {
        "license": f"?g {S}license> ?value",
        "format": f"?g {S}hasPart> ?f . ?f {S}encodingFormat> ?value",
        "keyword": f"?g {S}keywords> ?value",
    }
    for facet, pattern in patterns.items():
        cases.append(sparql(
            f"RC07-facet-{facet}",
            f"""SELECT ?value (COUNT(DISTINCT ?g) AS ?crates) WHERE {{ GRAPH ?g {{
                ?d {S}about> ?g . {pattern} }} }} GROUP BY ?value ORDER BY ?value""",
            ["value", "crates"],
            rows(*[[("value", value), ("crates", count(len(members)))]
                   for value, members in sorted(facets[facet].items())])))

    files = sorted(target.files[: len(target.files) // 2], key=lambda file: file.iri)
    cases.append(sparql(
        "RC08-optional",
        f"""SELECT ?file ?description WHERE {{ GRAPH <{graph}> {{ <{graph}> {S}hasPart> ?file .
            ?file a {S}MediaObject> . OPTIONAL {{ ?file {S}description> ?description }} }} }}
            ORDER BY ?file""",
        ["file", "description"],
        rows(*[[("file", iri(file.iri)), ("description", literal(file.description)
                                          if file.description else "UNBOUND")]
               for file in files])))
    cases.append(sparql(
        "RC08-absent",
        f"""SELECT ?file WHERE {{ GRAPH <{graph}> {{ <{graph}> {S}hasPart> ?file .
            ?file a {S}MediaObject> . FILTER NOT EXISTS {{ ?file {S}description> ?any }} }} }}
            ORDER BY ?file""",
        ["file"], rows(*[[("file", iri(file.iri))] for file in files if not file.description])))

    for label, members in (("one", crates[:1]), ("few", crates[:3]), ("more", crates[:10]),
                           ("duplicate", [crates[0], crates[0]])):
        people = sorted({model.people[person] for crate in members for person in crate.authors})
        text = f"""SELECT DISTINCT ?person ?name WHERE {{ ?person a {S}Person> ;
            {S}name> ?name }} ORDER BY ?person"""
        cases.append(sparql(
            f"RC09-set-{label}", text, ["person", "name"],
            rows(*[[("person", iri(person)), ("name", literal(person_name(model, person)))]
                   for person in people]),
            graphs=[crate.base for crate in members]))
    # Without DISTINCT, a person in several selected graphs is one default-graph triple.
    people = sorted({model.people[person] for crate in crates[:10] for person in crate.authors})
    cases.append(sparql(
        "RC09-set-union",
        f"SELECT ?person WHERE {{ ?person a {S}Person> }} ORDER BY ?person",
        ["person"], rows(*[[("person", iri(person))] for person in people]),
        graphs=[crate.base for crate in crates[:10]], semantics="default-union"))

    action = target.action
    cases.append(sparql(
        "RC10-provenance",
        f"""SELECT ?action ?agent ?name ?result WHERE {{ GRAPH <{graph}> {{
            ?action a {S}CreateAction> ; {S}agent> ?agent ; {S}result> ?result .
            ?agent {S}name> ?name }} }}""",
        ["action", "agent", "name", "result"],
        rows([("action", iri(action["iri"])), ("agent", iri(action["agent"])),
              ("name", literal(person_name(model, action["agent"]))),
              ("result", iri(action["result"]))])))
    reachable = sorted([file.iri for file in target.files] + [target.nested])
    cases.append(sparql(
        "RC10-path",
        f"""SELECT ?part WHERE {{ GRAPH <{graph}> {{ <{graph}> {S}hasPart>+ ?part }} }}
            ORDER BY ?part""",
        ["part"], rows(*[[("part", iri(part))] for part in reachable]), subset="property-path"))
    cases.append(sparql(
        "RC10-path-dataset",
        f"SELECT ?part WHERE {{ <{graph}> {S}hasPart>+ ?part }} ORDER BY ?part",
        ["part"], rows(*[[("part", iri(part))] for part in reachable]), subset="property-path",
        graphs=[graph]))

    cases.extend(text_cases(model))
    cases.append(export_case(model, target))
    cases.extend(scope_cases(model))
    return cases


def text_cases(model):
    """Controlled matching (exact resource sets) and native ranked top-k with eligibility."""
    documents = text_documents(model)
    frequency = {}
    for words in documents.values():
        for word in words & set(WORDS):
            frequency[word] = frequency.get(word, 0) + 1
    common = pick(model, frequency, False)
    rare = next((word for word in ("rare3", "rare1", "rare0")
                 if any(word in words for words in documents.values())), common)
    cases = []
    for label, terms in (("common", common), ("rare", rare), ("absent", "zzzabsent"),
                         ("either", f"{WORDS[3]} {WORDS[20]}")):
        cases.append({"id": f"RC11-match-{label}", "kind": "text", "mode": "controlled",
                      "terms": terms, "expected": [list(key) for key in matches(documents, terms)]})
    for limit in (10, 20, 100):
        pool = matches(documents, common)
        cases.append({"id": f"RC11-ranked-{limit}", "kind": "text", "mode": "ranked",
                      "terms": common, "limit": limit, "pool": [list(key) for key in pool],
                      "size": min(limit, len(pool))})
    # Text and structure together: root datasets whose own text matches and that a person wrote.
    person = model.people[model.crates[0].authors[0]]
    wanted = sorted(crate.base for crate in model.crates
                    if (crate.base, crate.base) in documents
                    and common in documents[(crate.base, crate.base)]
                    and person in [model.people[author] for author in crate.authors])
    cases.append({"id": "RC12-text-author", "kind": "structured", "terms": common,
                  "person": person, "variables": ["g"],
                  "expected": rows(*[[("g", iri(base))] for base in wanted]),
                  "craqle_sparql": " ".join(f"""SELECT DISTINCT ?g WHERE {{
                      SERVICE <urn:craqle:fts> {{ ?s <urn:craqle:fts:query> "{common}" .
                      ?s <urn:craqle:fts:graph> ?g . ?s <urn:craqle:fts:limit> 10000 }}
                      GRAPH ?g {{ ?d {S}about> ?s . ?s {S}author> <{person}> }} }}
                      ORDER BY ?g""".split()),
                  "virtuoso_sparql": " ".join(f"""SELECT DISTINCT ?g WHERE {{ GRAPH ?g {{
                      ?d {S}about> ?g . ?g {S}author> <{person}> . ?g ?p ?o .
                      FILTER(?p IN ({", ".join(f"{S}{name}>" for name in TEXT_PREDICATES)}))
                      ?o bif:contains "'{common}'" }} }} ORDER BY ?g""".split()),
                  "compare": "ordered"})
    return cases


def export_case(model, crate):
    """Every user-visible triple of one crate graph; the metadata file name is normalized."""
    triples = crate_triples(model, crate, "<DESCRIPTOR>")
    return sparql(
        "RC13-export",
        f"SELECT ?s ?p ?o WHERE {{ GRAPH <{crate.base}> {{ ?s ?p ?o }} }}",
        ["s", "p", "o"], rows(*[[("s", s), ("p", p), ("o", o)] for s, p, o in triples]),
        descriptor=crate.base)


def scope_cases(model):
    """The same reads over 100, 10, 1, and 0 percent readable crates."""
    cases = []
    crates = model.crates
    person = model.people[crates[0].authors[0]]
    for percent in (100, 10, 1, 0):
        readable = [crate.base for crate in crates if crate.index * percent % 100 < percent]
        if percent == 1:
            readable = [crate.base for crate in crates if crate.index % 100 == 0]
        allowed = set(readable)
        by_person = sorted(crate.base for crate in crates if crate.base in allowed
                           and person in [model.people[author] for author in crate.authors])
        cases.append(sparql(
            f"RC04-scope-{percent}",
            f"""SELECT DISTINCT ?g WHERE {{ GRAPH ?g {{ ?d {S}about> ?g .
                ?g {S}author> <{person}> }} }} ORDER BY ?g""",
            ["g"], rows(*[[("g", iri(base))] for base in by_person]),
            readable=readable, scope="application-enforced"))
    return cases
