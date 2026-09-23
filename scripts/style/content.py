"""Check logical comments and ordered file headers for the style gate."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

from pathlib import Path

from model import Issue


def comment_issues(path, comments, source, maximum):
    """Reject logical comment blocks over the configured source-line limit."""
    if not comments:
        return []
    ordered = sorted(set(comments), key=lambda item: (item.line, item.end))
    blocks = []
    lines = source.splitlines()
    start, end, count = ordered[0].line, ordered[0].end, ordered[0].end - ordered[0].line + 1
    for comment in ordered[1:]:
        gap = lines[end:comment.line - 1]
        if comment.line <= end + 1 or all(not line.strip() for line in gap):
            count += max(0, comment.end - max(end, comment.line) + 1)
            end = max(end, comment.end)
        else:
            blocks.append((start, end, count))
            start, end = comment.line, comment.end
            count = comment.end - comment.line + 1
    blocks.append((start, end, count))
    return [Issue(path, start, "comment_lines", "comment", "comment",
                  f"logical comment uses {count} source lines")
            for start, _, count in blocks if count > maximum]


def header_issues(path, source, suffix, rules):
    """Check the exact description, copyright, and MIT notice order."""
    if suffix not in rules["header_extensions"]:
        return []
    lines = source.splitlines()
    offset = int(bool(lines and lines[0].startswith("#!")))
    prefixes = {
        ".rs": ("//! ", "// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen",
                "// SPDX-License-Identifier: MIT"),
        ".py": ('"""', "# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen",
                "# SPDX-License-Identifier: MIT"),
        ".sh": ("# ", "# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen",
                "# SPDX-License-Identifier: MIT"),
        ".toml": ("# ", "# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen",
                  "# SPDX-License-Identifier: MIT"),
        ".yaml": ("# ", "# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen",
                  "# SPDX-License-Identifier: MIT"),
        ".yml": ("# ", "# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen",
                 "# SPDX-License-Identifier: MIT"),
        ".md": ("<!-- ", "<!-- Copyright (c) 2026 ArunaStorage Team @ JLU Giessen -->",
                "<!-- SPDX-License-Identifier: MIT -->"),
        ".gitignore": ("# ", "# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen",
                       "# SPDX-License-Identifier: MIT"),
    }
    description, copyright_line, license_line = prefixes[suffix]
    count = 1
    if suffix == ".rs" and len(lines) > offset + 1 and lines[offset + 1].startswith(description):
        count = 2
    actual = lines[offset:offset + count + 2]
    valid_description = bool(actual and all(line.startswith(description) for line in actual[:count]))
    if suffix == ".py":
        valid_description = valid_description and actual[0].endswith('"""')
    if suffix == ".md":
        valid_description = valid_description and actual[0].endswith(" -->")
    valid = (valid_description and len(actual) == count + 2
             and actual[count] == copyright_line and actual[count + 1] == license_line)
    if valid:
        return []
    return [Issue(path, offset + 1, "header", Path(path).name, "file",
                  "expected description, Craqle copyright, and MIT notice")]
