"""Regression tests for the canonical repository style gate."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import json
from pathlib import Path
import tempfile
import unittest

import checker
from imports import rust_wildcards
from tokens import rust_names, rust_tokens, terms


class StyleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def rules(self, **changes):
        rules = {
            "maintained_extensions": [".json", ".py", ".rs", ".sh"],
            "source_extensions": [".py", ".rs", ".sh"],
            "header_extensions": [".py", ".rs", ".sh"],
            "max_comment_lines": 2,
            "required_roots": [".", "src", "scripts", "tests"],
            "small_domains": [],
            "generic_prefixes": ["get", "run", "test"],
            "skip_paths": [],
            "exceptions": [],
            "macro_inputs": {"id_type": {"mode": "single_type"}},
            "test_paths": ["src/private_tests.rs"],
        }
        rules.update(changes)
        return rules

    def write(self, path, source=""):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source)
        return target

    def issue_rules(self, **changes):
        return {issue.rule for issue in checker.check(self.root, self.rules(**changes))}

    def test_acronym_terms(self):
        self.assertEqual(terms("HttpBodyLimit"), ["Http", "Body", "Limit"])
        self.assertEqual(terms("API_TOKEN_ID"), ["API", "TOKEN", "ID"])
        self.assertEqual(len(terms("max_http_body_size")), 4)

    def test_default_root(self):
        script = self.root / "repo" / "scripts" / "style" / "checker.py"
        self.assertEqual(checker.default_root(script), self.root / "repo")

    def test_prefix_threshold(self):
        self.write("src/compute_config.rs")
        self.write("src/compute_backend.rs")
        self.assertNotIn("shared_prefix", self.issue_rules())
        self.write("src/compute_tests.rs")
        self.assertIn("shared_prefix", self.issue_rules())

    def test_numeric_prefix(self):
        for name in ("1_1.jsonld", "1_2.jsonld", "1_3.jsonld"):
            self.write(f"src/rocrate/{name}", "{}")
        self.assertNotIn("shared_prefix", self.issue_rules())
        for name in ("asset-one.json", "asset-two.json", "asset-three.json"):
            self.write(f"tests/{name}", "{}")
        self.assertIn("shared_prefix", self.issue_rules())

    def test_small_domain(self):
        for name in ("config.rs", "backend.rs", "tests.rs"):
            self.write(f"src/compute/{name}")
        domains = [{"path": "src/compute", "reason": "Three-file family.",
                    "source": "fixture policy", "members": ["config.rs", "backend.rs", "tests.rs"]}]
        self.assertNotIn("folder_size", self.issue_rules(small_domains=domains))
        (self.root / "src/compute/tests.rs").unlink()
        rules = self.issue_rules(small_domains=domains)
        self.assertIn("domain_members", rules)
        self.assertIn("folder_size", rules)

    def test_test_companion(self):
        self.write("src/owner/tests.rs")
        self.assertIn("folder_size", self.issue_rules())
        self.temporary.cleanup()
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.write("src/owner.rs")
        self.write("src/owner_tests.rs")
        self.assertNotIn("folder_size", self.issue_rules())

    def test_empty_wrapper(self):
        self.write("src/wrapper/mod.rs", "mod domain;\n")
        for name in ("first.rs", "second.rs", "third.rs", "fourth.rs", "fifth.rs"):
            self.write(f"src/wrapper/domain/{name}", "fn entry() {}\n")
        issues = checker.check(self.root, self.rules())
        self.assertTrue(any(issue.path == "src/wrapper" and issue.rule == "folder_size"
                            for issue in issues))

    def test_generic_prefix(self):
        for name in ("get_alpha.rs", "get_beta.rs", "get_gamma.rs"):
            self.write(f"src/{name}")
        self.assertNotIn("shared_prefix", self.issue_rules())

    def test_asset_names(self):
        for name in ("schema_alpha.json", "schema_beta.json", "schema_gamma.json"):
            self.write(f"tests/{name}", "{}")
        self.assertIn("shared_prefix", self.issue_rules())
        self.write("tests/four_term_asset_name.json", "{}")
        self.assertIn("file_terms", self.issue_rules())

    def test_unknown_asset(self):
        self.write("src/opaque.asset", "fixed")
        self.assertIn("unsupported_asset", self.issue_rules())

    def test_generated_skip(self):
        self.write("src/generated/four_term_asset_name.rs", "fn four_term_name_here() {}\n")
        skip = [{"prefix": "src/generated", "rules": ["content", "layout"],
                 "reason": "Generated fixture.", "source": "fixture generator"}]
        self.assertEqual(checker.check(self.root, self.rules(skip_paths=skip)), [])

    def test_rust_declarations(self):
        source = """
            use external::FourTermImportName;
            const FOUR_TERM_LIMIT_NAME: usize = 1;
            type Alias<LongGenericParameterName> = Option<LongGenericParameterName>;
            enum LongTypeNameHere {
                #[serde(rename = "kept")]
                LongVariantNameHere,
                Item { long_field_name_here: usize }
            }
            fn takes_value<LongFunctionGenericName: Send + Sync + Clone + Copy + Sized + Unpin>(
                long_parameter_name_here: usize,
            ) {
                let long_local_name_here = long_parameter_name_here;
                let closure = |long_closure_parameter_name: usize| long_closure_parameter_name;
                match Some(closure(1)) {
                    Some(long_match_binding_name) => long_match_binding_name,
                    None => 0,
                };
            }
            id_type!(GeneratedTypeNameHere);
        """
        tokens, _, errors = rust_tokens(source)
        names, name_errors = rust_names(tokens, {"id_type": {"mode": "single_type"}})
        self.assertEqual(errors + name_errors, [])
        found = {name.text for name in names}
        expected = {"FourTermImportName", "FOUR_TERM_LIMIT_NAME", "LongGenericParameterName",
                    "LongFunctionGenericName", "LongTypeNameHere", "LongVariantNameHere",
                    "long_field_name_here", "long_parameter_name_here",
                    "long_local_name_here", "long_closure_parameter_name",
                    "long_match_binding_name", "GeneratedTypeNameHere"}
        self.assertTrue(expected <= found)

    def test_macro_functions(self):
        source = "forward!(long_generated_method_name(long_argument_name_here: usize) -> usize;);"
        tokens, _, errors = rust_tokens(source)
        rules = {"forward": {"mode": "function_list"}}
        names, name_errors = rust_names(tokens, rules)
        self.assertEqual(errors + name_errors, [])
        self.assertTrue({"long_generated_method_name", "long_argument_name_here"}
                        <= {name.text for name in names})

    def test_macro_statics(self):
        source = "vocab_terms!(SHORT_NAME => vocab::name, FOUR_TERM_STATIC_NAME => vocab::kind);"
        tokens, _, errors = rust_tokens(source)
        names, name_errors = rust_names(tokens, {"vocab_terms": {"mode": "static_list"}})
        self.assertEqual(errors + name_errors, [])
        self.assertIn("FOUR_TERM_STATIC_NAME", {name.text for name in names})

    def test_anon_const(self):
        tokens, _, errors = rust_tokens("const _: () = assert!(true);")
        names, name_errors = rust_names(tokens)
        self.assertEqual(errors + name_errors, [])
        self.assertNotIn("_", {name.text for name in names})

    def test_unknown_macro(self):
        tokens, _, errors = rust_tokens("macro_rules! owned { () => {}; } owned!();")
        names, name_errors = rust_names(tokens, {})
        self.assertTrue(names)
        self.assertEqual(errors, [])
        self.assertTrue(name_errors)

    def test_literal_ignored(self):
        source = 'fn good() { let okay = "fn four_term_name_here() {}"; }\n'
        tokens, _, errors = rust_tokens(source)
        names, name_errors = rust_names(tokens)
        self.assertEqual(errors + name_errors, [])
        self.assertNotIn("four_term_name_here", {name.text for name in names})

    def test_nested_fields(self):
        source = "struct Record { good: Vec<Vec<u8>>, long_field_name_here: usize }"
        tokens, _, errors = rust_tokens(source)
        names, name_errors = rust_names(tokens)
        self.assertEqual(errors + name_errors, [])
        self.assertIn("long_field_name_here", {name.text for name in names})

    def test_external_exception(self):
        self.write("src/lib.rs", "impl External for Local { fn required_external_trait_name() {} }\n")
        entry = {"path": "src/lib.rs", "rule": "name_terms",
                 "name": "required_external_trait_name", "kind": "function",
                 "reason": "Required by an external trait.", "source": "fixture trait"}
        self.assertNotIn("name_terms", self.issue_rules(exceptions=[entry]))
        self.write("src/lib.rs", "fn short_name() {}\n")
        self.assertIn("stale_exception", self.issue_rules(exceptions=[entry]))

    def test_wrong_kind(self):
        self.write("src/lib.rs", "fn four_term_name_here() {}\n")
        for kind in ("", "fun"):
            entry = {"path": "src/lib.rs", "rule": "name_terms", "name": "four_term_name_here",
                     "kind": kind, "reason": "Wrong kind.", "source": "fixture"}
            rules = self.issue_rules(exceptions=[entry])
            self.assertIn("name_terms", rules)
            self.assertIn("stale_exception", rules)

    def test_comment_fragments(self):
        self.write("src/lib.rs", "// one\n// two\n// three\n// four\nfn good() {}\n")
        self.assertIn("comment_lines", self.issue_rules())
        self.write("src/lib.rs", "// one\n// two\nfn good() {}\n")
        self.assertNotIn("comment_lines", self.issue_rules())

    def test_doc_attributes(self):
        self.write("src/lib.rs", '\n'.join('#![doc = "reason"]' for _ in range(4)))
        self.assertIn("comment_lines", self.issue_rules())
        self.write("src/lib.rs", '\n'.join('#![doc = "reason"]' for _ in range(2)))
        self.assertNotIn("comment_lines", self.issue_rules())

    def test_mandatory_notice(self):
        notice = "// SPDX-License-Identifier: MIT OR Apache-2.0"
        self.write("src/lib.rs", notice + "\n//! first\n//! second\n")
        self.assertNotIn("comment_lines", self.issue_rules(mandatory_notices=[notice]))
        self.write("src/lib.rs", notice + "\n//! first\n//! second\n//! third\n")
        self.assertIn("comment_lines", self.issue_rules(mandatory_notices=[notice]))
        self.write("src/lib.rs", "// one\n\n// two\n\n// three\n\n// four\nfn good() {}\n")
        self.assertIn("comment_lines", self.issue_rules())

    def test_python_syntax(self):
        source = '''"fn four_term_name_here()"\nclass FourTermTypeName:\n    long_field_name_here = 1\ndef long_function_name_here(long_parameter_name_here):\n    long_local_name_here = 1\n'''
        scan = checker.python_scan(source)
        self.assertEqual((len(scan.comments), scan.wildcards, scan.errors), (1, [], []))
        found = {name.text for name in scan.names}
        self.assertNotIn("four_term_name_here", found)
        self.assertIn("long_parameter_name_here", found)
        self.assertIn("long_local_name_here", found)

    def test_lambda_parameters(self):
        source = "callback = lambda long_lambda_parameter_here: long_lambda_parameter_here\n"
        scan = checker.python_scan(source)
        self.assertEqual(scan.errors, [])
        self.assertIn("long_lambda_parameter_here", {name.text for name in scan.names})

    def test_shell_syntax(self):
        source = "LONG_ENV_NAME_HERE=value tool\nexport first_long_name_here=x second_long_name_here=y\n"
        scan = checker.shell_scan(source)
        self.assertEqual(scan.errors, [])
        self.assertTrue({"LONG_ENV_NAME_HERE", "first_long_name_here", "second_long_name_here"}
                        <= {name.text for name in scan.names})
        self.assertTrue(checker.shell_scan("tool <<EOF\nvalue\nEOF\n").errors)
        self.assertTrue(checker.shell_scan("case x in y) ;; esac\n").errors)
        self.assertTrue(checker.shell_scan("value=$(long_name_here=value; echo x)\n").errors)
        self.assertEqual(checker.shell_scan("value='case x in y) ;; esac'\n").errors, [])

    def test_wildcard_scope(self):
        source = "use super::*;\n#[cfg(test)] mod tests { use super::*; }\n"
        self.write("src/lib.rs", source)
        issues = checker.check(self.root, self.rules())
        self.assertEqual(sum(issue.rule == "wildcard_import" for issue in issues), 1)
        self.write("tests/private.rs", "use super::*;\n")
        issues = checker.check(self.root, self.rules())
        self.assertEqual(sum(issue.rule == "wildcard_import" for issue in issues), 2)
        self.write("tests/private.rs", "")
        self.write("src/private_tests.rs", "use super::*;\n")
        issues = checker.check(self.root, self.rules())
        self.assertEqual(sum(issue.rule == "wildcard_import" for issue in issues), 1)

    def test_shell_functions(self):
        for declaration in ("four_term_name_here() { :; }",
                            "  function four-term-name-here { :; }"):
            scan = checker.shell_scan(declaration)
            self.assertEqual(scan.errors, [])
            self.assertTrue(any(name.kind == "function" and len(terms(name.text)) == 4
                                for name in scan.names))
        for source in ("  case x in y) ;; esac", "  eval 'function value() {}'"):
            self.assertTrue(checker.shell_scan(source).errors)

    def test_cfg_imports(self):
        cases = {
            "#[cfg(test)] use super::*;": [],
            "#[cfg(all(test, feature = \"x\"))] mod tests { use super::*; }": [],
            "#[cfg(any(test, all(test, unix)))] mod tests { use super::*; }": [],
            "#[cfg(any(test, feature = \"x\"))] mod tests { use super::*; }": [1],
            "#[cfg(not(test))] mod tests { use super::*; }": [1],
            "#[cfg(test)] fn test() {} use super::*;": [1],
            "#[cfg(test)] mod tests {} use super::*;": [1],
            "#[cfg(test)] #[allow(unused)] pub(crate) use super::*;": [],
        }
        for source, expected in cases.items():
            with self.subTest(source=source):
                tokens, _, errors = rust_tokens(source)
                self.assertEqual(errors, [])
                self.assertEqual(rust_wildcards(tokens), expected)

    def test_python_shebang(self):
        self.write("scripts/main.py", '#!/usr/bin/env python3\n"""One.\nTwo."""\n')
        self.assertNotIn("comment_lines", self.issue_rules())
        self.write("scripts/main.py", '# Normal comment.\n"""One.\nTwo."""\n')
        self.assertIn("comment_lines", self.issue_rules())

    def test_precise_capture(self):
        source = "fn values() -> impl Iterator<Item = usize> + use<> { [1].into_iter() }"
        tokens, _, errors = rust_tokens(source)
        self.assertEqual(errors, [])
        self.assertEqual(rust_wildcards(tokens), [])

    def test_rule_validation(self):
        path = self.write("rules.json", json.dumps(self.rules(exceptions=[{
            "path": "src/lib.rs", "rule": "name_terms", "name": "external_name",
            "kind": "function", "reason": ""
        }])))
        with self.assertRaises(ValueError):
            checker.load_rules(path)

    def test_header_order(self):
        header = ("//! Describes this fixture.\n"
                  "// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen\n"
                  "// SPDX-License-Identifier: MIT\n")
        self.write("src/lib.rs", header + "fn good() {}\n")
        self.assertNotIn("header", self.issue_rules())
        self.write("src/lib.rs", "// SPDX-License-Identifier: MIT\n" + header)
        self.assertIn("header", self.issue_rules())

    def test_header_notice(self):
        source = ("//! Describes this fixture.\n"
                  "// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen\n"
                  "// SPDX-License-Identifier: MIT OR Apache-2.0\n")
        self.write("src/lib.rs", source)
        self.assertIn("header", self.issue_rules())

    def test_header_comment(self):
        source = ("/// Describes this fixture.\n"
                  "// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen\n"
                  "// SPDX-License-Identifier: MIT\n")
        self.write("src/lib.rs", source)
        self.assertIn("header", self.issue_rules())

    def test_header_length(self):
        notices = ("// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen\n"
                   "// SPDX-License-Identifier: MIT\n")
        self.write("src/lib.rs", "//! One.\n//! Two.\n" + notices)
        self.assertNotIn("header", self.issue_rules())
        self.write("src/lib.rs", "//! One.\n//! Two.\n//! Three.\n" + notices)
        self.assertIn("header", self.issue_rules())


if __name__ == "__main__":
    unittest.main()
