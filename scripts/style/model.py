"""Defines immutable style findings shared by checker modules."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class Issue:
    path: str
    line: int
    rule: str
    name: str
    kind: str
    detail: str
