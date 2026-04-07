"""
OlymHaus — meme launcher + social aggregator for GoHoLaunch.
Runs with zero config (local SQLite + mock ingest); optionally indexes chain events if RPC + contract env vars are set.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import datetime as _dt
import hashlib
import hmac
import html
import json
import os
import random
import re
import secrets
import sqlite3
import string
import time
import typing as t
import urllib.parse

try:
    from fastapi import FastAPI, Request, Response, HTTPException
    from fastapi.responses import HTMLResponse, RedirectResponse
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependencies. Install with: pip install fastapi uvicorn aiosqlite pydantic\n"
        f"Import error: {e}"
    )

try:
    import aiosqlite
except Exception as e:  # pragma: no cover
    raise SystemExit("Missing dependency: aiosqlite. Install with: pip install aiosqlite\n" f"Import error: {e}")

try:
    from pydantic import BaseModel, Field
except Exception as e:  # pragma: no cover
    raise SystemExit("Missing dependency: pydantic. Install with: pip install pydantic\n" f"Import error: {e}")

# web3.py is optional (demo mode works without it)
try:
    from web3 import Web3  # type: ignore
    from web3.exceptions import BadFunctionCallOutput  # type: ignore
except Exception:
    Web3 = None
    BadFunctionCallOutput = Exception


# ---------------------------- CONFIG ----------------------------
def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return default if v is None or v == "" else v


APP_NAME = "OlymHaus"
APP_VERSION = "0.9.3"

DB_PATH = os.path.join(os.path.dirname(__file__), "olymhaus.sqlite3")

BIND_HOST = _env("OLYMHAUS_BIND_HOST", "127.0.0.1")
BIND_PORT = int(_env("OLYMHAUS_BIND_PORT", "8787"))

RPC_URL = os.environ.get("OLYMHAUS_RPC_URL", "").strip() or None
CONTRACT_ADDRESS = os.environ.get("OLYMHAUS_CONTRACT_ADDRESS", "").strip() or None

# If web3 is missing or env not provided, we run in demo mode.
DEMO_MODE = (Web3 is None) or (RPC_URL is None) or (CONTRACT_ADDRESS is None)


# --------------------------- UTILITIES ---------------------------
def now_ts() -> int:
    return int(time.time())


def iso(ts: int) -> str:
    return _dt.datetime.utcfromtimestamp(ts).isoformat() + "Z"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def keccak_like_hex(data: bytes) -> str:
    # Not real keccak; used only as a local stable hash for UI.
    return hashlib.sha3_256(data).hexdigest()


def clamp(n: int, lo: int, hi: int) -> int:
    return lo if n < lo else hi if n > hi else n


def sbool(v: t.Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in {"1", "true", "t", "yes", "y", "on"}


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _unb64url(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode(s + pad)


def safe_text(s: str, max_len: int = 4000) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.strip()
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s


def normalize_handle(h: str) -> str:
    h = safe_text(h, 80)
    h = h.strip()
    h = re.sub(r"\s+", "", h)
    h = h.lower()
    h = re.sub(r"[^a-z0-9_\\.\\-]", "", h)
    return h


def rand_slug(n: int = 12) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))


def guess_lane_from_text(text: str) -> str:
    t0 = text.lower()
    if any(k in t0 for k in ["alpha", "chart", "breakout", "rsi", "macd", "support", "resistance"]):
        return "alpha"
    if any(k in t0 for k in ["meme", "gm", "lol", "lmao", "ngmi", "wagmi", "based", "cringe"]):
        return "memes"
