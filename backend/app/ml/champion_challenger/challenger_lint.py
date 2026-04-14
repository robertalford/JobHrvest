"""AST-based guardrails for auto-generated challenger extractors."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_DOMAIN_LITERAL = re.compile(r"\w+\.(?:com|io|co|net|org|au|nz)\b", re.IGNORECASE)


@dataclass
class LintViolation:
    rule_id: str
    message: str
    line: int | None = None


@dataclass
class LintReport:
    path: str
    ok: bool
    loc: int
    violations: list[LintViolation] = field(default_factory=list)


def lint_challenger(path: str | Path, memory: dict[str, Any] | None) -> LintReport:
    file_path = Path(path)
    source = file_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(file_path))
    loc = _source_loc(source)
    violations: list[LintViolation] = []

    cls = next((node for node in tree.body if isinstance(node, ast.ClassDef)), None)
    if cls is None:
        violations.append(LintViolation("R0", "No class definition found"))
        return LintReport(str(file_path), False, loc, violations)

    base_name = _base_name(cls)
    if base_name.startswith("TieredExtractorV") and base_name != "TieredExtractorV16":
        violations.append(LintViolation("R1", "Inheritance depth >1 from TieredExtractorV16", cls.lineno))

    method_defs = [node for node in cls.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    if len(method_defs) > 5:
        violations.append(LintViolation("R3", "Method budget exceeded (>5 methods)", cls.lineno))

    extract_def = next((node for node in method_defs if node.name == "extract"), None)
    if isinstance(extract_def, ast.AsyncFunctionDef) and not _extract_calls_allowed_path(extract_def):
        violations.append(
            LintViolation(
                "R2",
                "extract() override must await super().extract(...) or self._finalize_with_enrichment(...)",
                extract_def.lineno,
            )
        )

    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and any(isinstance(op, ast.Eq) for op in node.ops):
            values = [node.left, *node.comparators]
            for value in values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str) and _DOMAIN_LITERAL.search(value.value):
                    violations.append(LintViolation("R4", "Domain literal equality hack detected", node.lineno))
                    break

    banned = (memory or {}).get("banned_approaches") or []
    lowered = source.lower()
    for ban in banned:
        tokens = ban.get("salient_tokens") or _salient_tokens(ban.get("summary") or "")
        if tokens and all(token in lowered for token in tokens):
            violations.append(LintViolation("R5", f"Banned approach repeated: {ban.get('summary') or ', '.join(tokens)}"))
            break

    if loc > 200:
        violations.append(LintViolation("R6", f"LOC budget exceeded ({loc} > 200)"))

    deduped = _dedupe_violations(violations)
    return LintReport(str(file_path), not deduped, loc, deduped)


def _source_loc(source: str) -> int:
    return sum(1 for line in source.splitlines() if line.strip() and not line.strip().startswith("#"))


def _base_name(cls: ast.ClassDef) -> str:
    if not cls.bases:
        return ""
    base = cls.bases[0]
    if isinstance(base, ast.Name):
        return base.id
    if isinstance(base, ast.Attribute):
        return base.attr
    return ""


def _extract_calls_allowed_path(node: ast.AsyncFunctionDef) -> bool:
    for child in ast.walk(node):
        if not isinstance(child, ast.Await) or not isinstance(child.value, ast.Call):
            continue
        if _is_super_extract_call(child.value) or _is_finalize_call(child.value):
            return True
    return False


def _is_super_extract_call(call: ast.Call) -> bool:
    func = call.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "extract"
        and isinstance(func.value, ast.Call)
        and isinstance(func.value.func, ast.Name)
        and func.value.func.id == "super"
    )


def _is_finalize_call(call: ast.Call) -> bool:
    func = call.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "_finalize_with_enrichment"
        and isinstance(func.value, ast.Name)
        and func.value.id == "self"
    )


def _salient_tokens(text: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9_]+", text.lower()) if len(token) >= 4][:4]


def _dedupe_violations(items: list[LintViolation]) -> list[LintViolation]:
    seen: set[tuple[str, int | None]] = set()
    out: list[LintViolation] = []
    for item in items:
        key = (item.rule_id, item.line)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


__all__ = ["LintReport", "LintViolation", "lint_challenger"]
