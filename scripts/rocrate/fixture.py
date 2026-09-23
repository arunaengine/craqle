"""Generate deterministic RO-Crate 1.2 fixtures, their canonical N-Quads, and the model."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import random

CONTEXT = "https://w3id.org/ro/crate/1.2/context"
PROFILE = "https://w3id.org/ro/crate/1.2"
SCHEMA = "http://schema.org/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
DCT_CONFORMS = "http://purl.org/dc/terms/conformsTo"
XSD = "http://www.w3.org/2001/XMLSchema#"
DESCRIPTOR = "ro-crate-metadata.json"

# Deterministic profiles; sizes are fixture targets, not production distributions.
PROFILES = {
    "smoke": {"crates": 50, "entities": 20, "people": 24, "orgs": 6},
    "development": {"crates": 1000, "entities": 100, "people": 400, "orgs": 40},
    "scale": {"crates": 10000, "entities": 100, "people": 4000, "orgs": 200},
    "sweep-20": {"crates": 20, "entities": 1000, "people": 200, "orgs": 20},
    "sweep-200": {"crates": 200, "entities": 100, "people": 200, "orgs": 20},
    "sweep-2000": {"crates": 2000, "entities": 10, "people": 200, "orgs": 20},
}

# Lowercase ASCII words keep tokenization identical in both engines' text indexes.
WORDS = [
    "soil", "drought", "river", "sediment", "genome", "protein", "climate", "forest",
    "glacier", "coastal", "microbe", "pollen", "isotope", "plankton", "aquifer", "wetland",
    "lichen", "fungal", "tundra", "estuary", "biofilm", "moraine", "canopy", "peatland",
    "delta", "basalt", "loess", "karst", "fjord", "savanna", "mangrove", "steppe",
    "alpine", "reef", "lagoon", "dune", "marsh", "prairie", "taiga", "atoll",
]
FORMATS = ["text/csv", "application/json", "image/png", "text/plain", "application/pdf",
           "image/tiff", "application/x-netcdf", "text/tab-separated-values"]
LICENSES = ["https://spdx.org/licenses/CC-BY-4.0", "https://spdx.org/licenses/CC0-1.0",
            "https://spdx.org/licenses/MIT", "https://spdx.org/licenses/CC-BY-SA-4.0"]


@dataclass
class File:
    iri: str
    name: str
    format: str
    size: int
    modified: str | None
    description: str | None


@dataclass
class Crate:
    index: int
    base: str
    name: str
    description: str
    language: str | None
    date: str
    license: str | None
    authors: list
    keywords: list
    identifier: str
    files: list = field(default_factory=list)
    nested: str | None = None
    nested_parts: list = field(default_factory=list)
    action: dict | None = None


@dataclass
class Model:
    profile: str
    seed: int
    people: list
    orgs: list
    affiliation: list
    crates: list


def zipf_index(rng, size, skew=1.1):
    """Pick an index where low indexes are much more frequent than high ones."""
    weights = [1.0 / (rank + 1) ** skew for rank in range(size)]
    return rng.choices(range(size), weights=weights)[0]


def generate(profile, seed=20260921):
    """Build the logical model for one profile from a fixed seed."""
    shape = PROFILES[profile]
    rng = random.Random(seed)
    orgs = [f"https://ror.org/0bench{index:04d}" for index in range(shape["orgs"])]
    people = [f"https://orcid.org/0000-0002-{index // 10000:04d}-{index % 10000:04d}"
              for index in range(shape["people"])]
    affiliation = [zipf_index(rng, len(orgs)) for _ in people]
    crates = []
    for index in range(shape["crates"]):
        base = f"https://bench.example/crate/{index:05d}/"
        words = [WORDS[zipf_index(rng, len(WORDS))] for _ in range(rng.randint(6, 20))]
        # Rare words appear in only a few crates, so rare and common terms both exist.
        if index % 17 == 3:
            words.append(f"rare{index % 5}")
        authors = sorted({zipf_index(rng, len(people)) for _ in range(rng.randint(1, 3))})
        keywords = sorted({WORDS[zipf_index(rng, len(WORDS))] for _ in range(rng.randint(1, 5))})
        crate = Crate(
            index=index,
            base=base,
            name=f"Crate {index} {words[0]} {words[1]}",
            description=" ".join(words),
            language="de" if index % 11 == 0 else None,
            date=f"{2015 + index % 12}-{1 + index % 12:02d}-{1 + index % 28:02d}",
            license=None if index % 9 == 0 else LICENSES[zipf_index(rng, len(LICENSES))],
            authors=authors,
            keywords=keywords,
            identifier=f"doi:10.5555/bench.{index}",
        )
        fixed = 1 + 1 + len(authors) + len({affiliation[a] for a in authors})
        fixed += int(crate.license is not None) + 1 + 1
        count = max(2, shape["entities"] - fixed)
        for part in range(count):
            crate.files.append(File(
                iri=f"{base}data/file{part:04d}.dat",
                name=f"File {part} of crate {index}",
                format=FORMATS[zipf_index(rng, len(FORMATS))],
                size=rng.randint(1, 10_000_000),
                modified=None if part % 3 == 0 else crate.date,
                description=None if part % 4 == 0
                else " ".join(WORDS[zipf_index(rng, len(WORDS))] for _ in range(3)),
            ))
        crate.nested = f"{base}data/"
        crate.nested_parts = [file.iri for file in crate.files[len(crate.files) // 2:]]
        crate.action = {
            "iri": f"{base}#create",
            "agent": people[authors[0]],
            "result": crate.files[0].iri,
        }
        crates.append(crate)
    return Model(profile, seed, people, orgs, affiliation, crates)


def person_name(model, person):
    number = model.people.index(person)
    return f"Person {number}" if number % 13 else f"Zoë Müller {number}"


def org_name(model, org):
    number = model.orgs.index(org)
    return f"Organization {number}" if number % 7 else f"Université {number}"


def crate_document(model, crate):
    """The RO-Crate 1.2 metadata document for one crate."""
    root = {
        "@id": "./",
        "@type": "Dataset",
        "name": crate.name,
        "description": ({"@value": crate.description, "@language": crate.language}
                        if crate.language else crate.description),
        "datePublished": crate.date,
        "identifier": crate.identifier,
        "keywords": crate.keywords,
        "author": [{"@id": model.people[person]} for person in crate.authors],
        "hasPart": [{"@id": file.iri} for file in crate.files[: len(crate.files) // 2]]
        + [{"@id": crate.nested}],
    }
    if crate.license:
        root["license"] = {"@id": crate.license}
    graph = [
        {"@id": DESCRIPTOR, "@type": "CreativeWork", "conformsTo": {"@id": PROFILE},
         "about": {"@id": "./"}},
        root,
        {"@id": crate.nested, "@type": "Dataset", "name": f"Data of crate {crate.index}",
         "hasPart": [{"@id": part} for part in crate.nested_parts]},
    ]
    for file in crate.files:
        entity = {"@id": file.iri, "@type": "File", "name": file.name,
                  "encodingFormat": file.format,
                  "contentSize": {"@value": str(file.size), "@type": f"{XSD}integer"}}
        if file.modified:
            entity["dateModified"] = {"@value": file.modified, "@type": f"{XSD}date"}
        if file.description:
            entity["description"] = file.description
        graph.append(entity)
    orgs = set()
    for person in crate.authors:
        org = model.affiliation[person]
        orgs.add(org)
        graph.append({"@id": model.people[person], "@type": "Person",
                      "name": person_name(model, model.people[person]),
                      "affiliation": {"@id": model.orgs[org]}})
    for org in sorted(orgs):
        graph.append({"@id": model.orgs[org], "@type": "Organization",
                      "name": org_name(model, model.orgs[org])})
    if crate.license:
        graph.append({"@id": crate.license, "@type": "CreativeWork",
                      "name": crate.license.rsplit("/", 1)[1]})
    action = crate.action
    graph.append({"@id": action["iri"], "@type": "CreateAction",
                  "agent": {"@id": action["agent"]}, "result": {"@id": action["result"]}})
    return {"@context": CONTEXT, "@graph": graph}


def iri(value):
    return f"<{value}>"


def literal(value, datatype=None, language=None):
    text = json.dumps(value, ensure_ascii=False)
    if language:
        return f"{text}@{language}"
    if datatype:
        return f"{text}^^<{datatype}>"
    return text


def crate_triples(model, crate, descriptor):
    """The user-visible triples of one crate, with `descriptor` naming the metadata file."""
    triples = [(descriptor, RDF_TYPE, iri(f"{SCHEMA}CreativeWork")),
               (descriptor, DCT_CONFORMS, iri(PROFILE)),
               (descriptor, f"{SCHEMA}about", iri(crate.base))]
    root = crate.base

    def add(subject, predicate, value):
        triples.append((iri(subject), predicate, value))

    add(root, RDF_TYPE, iri(f"{SCHEMA}Dataset"))
    add(root, f"{SCHEMA}name", literal(crate.name))
    add(root, f"{SCHEMA}description", literal(crate.description, language=crate.language))
    add(root, f"{SCHEMA}datePublished", literal(crate.date))
    add(root, f"{SCHEMA}identifier", literal(crate.identifier))
    for keyword in crate.keywords:
        add(root, f"{SCHEMA}keywords", literal(keyword))
    for person in crate.authors:
        add(root, f"{SCHEMA}author", iri(model.people[person]))
    for file in crate.files[: len(crate.files) // 2]:
        add(root, f"{SCHEMA}hasPart", iri(file.iri))
    add(root, f"{SCHEMA}hasPart", iri(crate.nested))
    if crate.license:
        add(root, f"{SCHEMA}license", iri(crate.license))
        add(crate.license, RDF_TYPE, iri(f"{SCHEMA}CreativeWork"))
        add(crate.license, f"{SCHEMA}name", literal(crate.license.rsplit("/", 1)[1]))
    add(crate.nested, RDF_TYPE, iri(f"{SCHEMA}Dataset"))
    add(crate.nested, f"{SCHEMA}name", literal(f"Data of crate {crate.index}"))
    for part in crate.nested_parts:
        add(crate.nested, f"{SCHEMA}hasPart", iri(part))
    for file in crate.files:
        add(file.iri, RDF_TYPE, iri(f"{SCHEMA}MediaObject"))
        add(file.iri, f"{SCHEMA}name", literal(file.name))
        add(file.iri, f"{SCHEMA}encodingFormat", literal(file.format))
        add(file.iri, f"{SCHEMA}contentSize", literal(str(file.size), f"{XSD}integer"))
        if file.modified:
            add(file.iri, f"{SCHEMA}dateModified", literal(file.modified, f"{XSD}date"))
        if file.description:
            add(file.iri, f"{SCHEMA}description", literal(file.description))
    orgs = set()
    for person in crate.authors:
        person_iri = model.people[person]
        org = model.orgs[model.affiliation[person]]
        orgs.add(org)
        add(person_iri, RDF_TYPE, iri(f"{SCHEMA}Person"))
        add(person_iri, f"{SCHEMA}name", literal(person_name(model, person_iri)))
        add(person_iri, f"{SCHEMA}affiliation", iri(org))
    for org in orgs:
        add(org, RDF_TYPE, iri(f"{SCHEMA}Organization"))
        add(org, f"{SCHEMA}name", literal(org_name(model, org)))
    action = crate.action
    add(action["iri"], RDF_TYPE, iri(f"{SCHEMA}CreateAction"))
    add(action["iri"], f"{SCHEMA}agent", iri(action["agent"]))
    add(action["iri"], f"{SCHEMA}result", iri(action["result"]))
    return sorted(set((subject, f"<{predicate}>", obj) for subject, predicate, obj in triples))


def write_fixture(model, directory):
    """Write documents, canonical N-Quads, and a manifest with hashes and effective sizes."""
    directory = Path(directory)
    (directory / "crates").mkdir(parents=True, exist_ok=False)
    quads = 0
    literals = 0
    with (directory / "dataset.nq").open("w", encoding="utf-8") as dataset:
        for crate in model.crates:
            document = json.dumps(crate_document(model, crate), ensure_ascii=False, indent=1)
            (directory / "crates" / f"{crate.index:05d}.json").write_text(document)
            descriptor = iri(crate.base + DESCRIPTOR)
            for subject, predicate, obj in crate_triples(model, crate, descriptor):
                dataset.write(f"{subject} {predicate} {obj} <{crate.base}> .\n")
                quads += 1
                literals += obj.startswith('"')
    manifest = {
        "profile": model.profile,
        "seed": model.seed,
        "crates": len(model.crates),
        "quads": quads,
        "literals": literals,
        "entities": sum(len(crate_document(model, crate)["@graph"]) for crate in model.crates),
        "context": CONTEXT,
        "dataset_sha256": hashlib.sha256((directory / "dataset.nq").read_bytes()).hexdigest(),
        "dataset_bytes": (directory / "dataset.nq").stat().st_size,
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=1))
    return manifest
