from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any


def normalize_whitespace(value: str) -> str:
    return " ".join(value.replace("\u00a0", " ").split())


def cleaned_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = normalize_whitespace(raw_line.replace("\uf0b7", "•"))
        if line:
            lines.append(line)
    return lines


def strip_bullet_prefix(value: str) -> str:
    return re.sub(r"^[•*\-]+\s*", "", value).strip()


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    ensure_parent_dir(path)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        if value and value not in seen:
            unique.append(value)
            seen.add(value)
    return unique


def playwright_environment_hint(root_dir: Path) -> str:
    venv_python = root_dir / ".venv" / "bin" / "python3"
    return (
        "Playwright is not installed in the active Python environment. "
        f"Current interpreter: {sys.executable}. "
        f"Use '{venv_python} -m app.main ...' or run 'source .venv/bin/activate' first. "
        f"If needed, install with '{venv_python} -m pip install -r requirements.txt' "
        f"and '{venv_python} -m playwright install chromium'."
    )
