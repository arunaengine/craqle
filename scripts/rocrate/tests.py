"""Checks eligibility validation of capped search answers."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import unittest

from main import SEARCH_LIMIT, validate


class CappedHits(unittest.TestCase):
    def test_capped_membership(self):
        expected = [["urn:graph", f"urn:subject:{index}"] for index in range(SEARCH_LIMIT + 1)]
        case = {"kind": "text", "mode": "controlled", "expected": expected}
        rows = expected[:SEARCH_LIMIT]
        record = {"status": "measured", "engine": "craqle", "rows": rows}
        self.assertTrue(validate(case, record).startswith("limit:"))
        rows[-1] = ["urn:hidden", "urn:wrong"]
        self.assertEqual(validate(case, record), "ineligible hit")


if __name__ == "__main__":
    unittest.main()
