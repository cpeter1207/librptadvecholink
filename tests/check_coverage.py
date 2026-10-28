#!/usr/bin/env python3
"""Require complete line and branch coverage for production Rust code."""

from __future__ import annotations

import json
import re
import sys
from itertools import pairwise
from pathlib import Path


def test_ranges(filename: str) -> list[tuple[int, int]]:
    """Return source ranges compiled only when Rust's test configuration is set."""
    try:
        lines = Path(filename).read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    ranges = []
    index = 0
    while index < len(lines):
        if not re.fullmatch(r"\s*#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*", lines[index]):
            index += 1
            continue
        start = index + 1
        item = start
        while item < len(lines) and (
            not lines[item].strip() or lines[item].lstrip().startswith("#[")
        ):
            item += 1
        if item == len(lines):
            ranges.append((start, start))
            index += 1
            continue
        end = item + cfg_item_end("\n".join(lines[item:]))
        ranges.append((start, end))
        index = end
    return ranges


def cfg_item_end(source: str) -> int:
    """Return the last line of a cfg(test) Rust item, ignoring strings and comments."""
    state, block_depth, raw_end = "code", 0, ""
    braces, body, parentheses, brackets, line, index = 0, False, 0, 0, 1, 0
    while index < len(source):
        char, following = source[index], source[index : index + 2]
        if char == "\n":
            line += 1
        if state == "line_comment":
            if char == "\n":
                state = "code"
        elif state == "block_comment":
            if following == "/*":
                block_depth += 1
                index += 1
            elif following == "*/":
                block_depth -= 1
                index += 1
                if block_depth == 0:
                    state = "code"
        elif state == "string":
            if char == "\\":
                index += 1
            elif char == '"':
                state = "code"
        elif state == "raw_string":
            if source.startswith(raw_end, index):
                index += len(raw_end) - 1
                state = "code"
        else:
            raw = re.match(r'(?:b|c)?r(#+)?"', source[index:])
            if raw:
                raw_end = '"' + (raw.group(1) or "")
                index += len(raw.group(0)) - 1
                state = "raw_string"
            elif following == "//":
                state = "line_comment"
                index += 1
            elif following == "/*":
                state, block_depth = "block_comment", 1
                index += 1
            elif char == '"':
                state = "string"
            elif char == "(":
                parentheses += 1
            elif char == ")":
                parentheses -= 1
            elif char == "[":
                brackets += 1
            elif char == "]":
                brackets -= 1
            elif char == "{":
                body, braces = True, braces + 1
            elif char == "}" and body:
                braces -= 1
                if braces == 0:
                    return line
            elif not body and parentheses == 0 and brackets == 0 and char in ";,":
                return line
        index += 1
    return line


def is_test_source(filename: str) -> bool:
    """Identify separate test modules excluded from production coverage."""
    path = filename.replace("\\", "/")
    name = path.rsplit("/", 1)[-1]
    return "/tests/" in path or name == "tests.rs" or name.endswith("_tests.rs")


def check(report: Path) -> tuple[int, int]:
    """Reject uncovered executable production lines or branch arms."""
    data = json.loads(report.read_text(encoding="utf-8"))
    lines: dict[tuple[str, int], bool] = {}
    branches: dict[tuple[object, ...], list[bool]] = {}
    for unit in data["data"]:
        for source in unit["files"]:
            filename = source["filename"]
            if is_test_source(filename):
                continue
            excluded = test_ranges(filename)
            segments = source.get("segments", [])
            for segment, following in pairwise(segments):
                if not segment[3] or segment[5] or segment[:2] == following[:2]:
                    continue
                end = following[0] - (following[1] == 1)
                for current_line in range(segment[0], end + 1):
                    if any(start <= current_line <= stop for start, stop in excluded):
                        continue
                    key = filename, current_line
                    lines[key] = lines.get(key, False) or segment[2] > 0
            for branch in source.get("branches", []):
                if any(start <= branch[0] <= stop for start, stop in excluded):
                    continue
                arms = branches.setdefault((filename, *branch[:4]), [False, False])
                for index, count in enumerate(branch[4:6]):
                    arms[index] |= count > 0
    missing_lines = [key for key, hit in lines.items() if not hit]
    missing_branches = [key for key, arms in branches.items() if not all(arms)]
    if missing_lines or missing_branches:
        raise ValueError(
            f"uncovered production lines={missing_lines}; branches={missing_branches}"
        )
    if not lines or not branches:
        raise ValueError("coverage report contains no production lines or branches")
    return len(lines), len(branches)


if __name__ == "__main__":
    try:
        line_count, branch_count = check(Path(sys.argv[1]))
    except (IndexError, OSError, ValueError, json.JSONDecodeError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
    print(f"100% production coverage: {line_count} lines, {branch_count} branches")
