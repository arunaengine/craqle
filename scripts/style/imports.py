"""Inspect Rust cfg scopes and wildcard imports for the style gate."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

from tokens import pairs, segments, skip_attrs


def requires_test(tokens, start, end):
    """Recognize cfg predicates that cannot enable a production import."""
    if end == start + 1:
        return tokens[start].text == "test"
    if end < start + 3 or tokens[start + 1].text != "(" or tokens[end - 1].text != ")":
        return False
    children = [requires_test(tokens, begin, finish)
                for begin, finish in segments(tokens, start + 2, end - 1)
                if begin < finish]
    if tokens[start].text == "all":
        return any(children)
    if tokens[start].text == "any":
        return bool(children) and all(children)
    return False


def test_ranges(tokens):
    """Locate modules and imports with a directly attached test-only cfg."""
    linked, _ = pairs(tokens)
    ranges = []
    for index, token in enumerate(tokens):
        if token.text != "#" or index + 4 >= len(tokens):
            continue
        if [item.text for item in tokens[index + 1:index + 4]] != ["[", "cfg", "("]:
            continue
        closing = linked.get(index + 3)
        if closing is None or not requires_test(tokens, index + 4, closing):
            continue
        target = skip_attrs(tokens, linked[index + 1] + 1, len(tokens), linked)
        if target < len(tokens) and tokens[target].text == "pub":
            target += 1
            if target < len(tokens) and tokens[target].text == "(":
                target = linked[target] + 1
        if target >= len(tokens) or tokens[target].text not in ("mod", "use"):
            continue
        opening = next((pos for pos in range(target + 1, len(tokens))
                        if tokens[pos].text in ("{", ";")), None)
        if opening is not None:
            end = linked[opening] if tokens[opening].text == "{" else opening
            ranges.append((target - 1, end))
    return ranges


def rust_wildcards(tokens):
    """Return production wildcard-import lines."""
    ranges = test_ranges(tokens)
    result = []
    for index, token in enumerate(tokens):
        if token.text != "use" or index + 1 >= len(tokens) or tokens[index + 1].text == "<":
            continue
        finish = index + 1
        while finish < len(tokens) and tokens[finish].text != ";":
            finish += 1
        wildcard = any(tokens[pos].text == "*" for pos in range(index + 1, finish))
        guarded = any(start < index < end for start, end in ranges)
        if wildcard and not guarded:
            result.append(token.line)
    return result
