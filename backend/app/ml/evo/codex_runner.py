"""Async wrapper around `codex exec --json` for evo cycles."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class CodexResult:
    candidate_id: str
    returncode: int
    events: list[dict] = field(default_factory=list)
    stdout: str = ""
    stderr: str = ""


async def spawn_codex(prompt_path: str | Path, working_dir: str | Path, candidate_id: str, timeout_sec: int, log_file: str | Path) -> CodexResult:
    proc = await asyncio.create_subprocess_exec(
        "codex",
        "exec",
        "--full-auto",
        "--json",
        str(prompt_path),
        cwd=str(working_dir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        proc.kill()
        stdout, stderr = await proc.communicate()
        return CodexResult(candidate_id=candidate_id, returncode=124, stdout=stdout.decode(), stderr=stderr.decode())

    stdout_text = stdout.decode()
    events = []
    for line in stdout_text.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    Path(log_file).write_text(stdout_text)
    return CodexResult(candidate_id=candidate_id, returncode=proc.returncode or 0, events=events, stdout=stdout_text, stderr=stderr.decode())


__all__ = ["CodexResult", "spawn_codex"]
