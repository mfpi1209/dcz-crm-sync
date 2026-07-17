"""Carrega variáveis do .env na raiz do projeto de forma robusta."""
from __future__ import annotations

import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_ENV_FILE = _PROJECT_ROOT / ".env"


def get_project_root() -> Path:
    return _PROJECT_ROOT


def get_env_file() -> Path:
    return _ENV_FILE


def _parse_env_file(path: Path) -> None:
    try:
        raw = path.read_text(encoding="utf-8-sig")
    except OSError:
        return

    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ[key] = value


def load_project_env() -> bool:
    """Carrega o .env da raiz. Retorna True se SUPABASE_URL e SUPABASE_KEY existem."""
    if not _ENV_FILE.is_file():
        return False

    try:
        from dotenv import load_dotenv

        load_dotenv(_ENV_FILE, override=True)
    except (PermissionError, OSError):
        _parse_env_file(_ENV_FILE)

    url = (os.environ.get("SUPABASE_URL", "") or "").strip()
    key = (os.environ.get("SUPABASE_KEY", "") or "").strip()
    return bool(url and key)
