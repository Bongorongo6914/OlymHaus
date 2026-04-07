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
    if any(k in t0 for k in ["security", "audit", "cve", "exploit", "hack", "reentrancy"]):
        return "sec"
    if any(k in t0 for k in ["irl", "life", "walk", "coffee", "gym"]):
        return "irl"
    return "general"


def html_page(title: str, body: str, *, head_extra: str = "") -> str:
    css = """
    :root {
      --bg: #0b0d13;
      --panel: #101525;
      --muted: #8aa0b8;
      --text: #e8f0ff;
      --brand: #b6ff3b;
      --brand2: #3be0ff;
      --danger: #ff4d67;
      --warn: #ffcc66;
      --line: rgba(255,255,255,0.10);
      --chip: rgba(182,255,59,0.12);
      --shadow: 0 20px 70px rgba(0,0,0,0.55);
    }
    body { background: radial-gradient(1000px 700px at 15% 10%, rgba(59,224,255,0.12), transparent 40%),
                    radial-gradient(900px 700px at 70% 0%, rgba(182,255,59,0.10), transparent 45%),
                    var(--bg);
           color: var(--text); font-family: ui-sans-serif, system-ui, Segoe UI, Arial;
           margin: 0; padding: 0; }
    a { color: var(--brand2); text-decoration: none; }
    a:hover { text-decoration: underline; }
    .wrap { max-width: 1120px; margin: 0 auto; padding: 28px 18px 80px; }
    .topbar { display:flex; gap:12px; align-items:center; justify-content:space-between; padding: 12px 16px;
              background: rgba(16,21,37,0.75); border: 1px solid var(--line); border-radius: 16px;
              box-shadow: var(--shadow); backdrop-filter: blur(10px); position: sticky; top: 10px; z-index: 20; }
    .brand { display:flex; gap:10px; align-items:center; }
    .logo { width: 28px; height: 28px; border-radius: 9px;
            background: linear-gradient(135deg, var(--brand2), var(--brand)); box-shadow: 0 12px 40px rgba(59,224,255,0.18); }
    .title { font-weight: 820; letter-spacing: 0.2px; }
    .pill { font-size: 12px; color: var(--muted); padding: 6px 10px; border: 1px solid var(--line);
            border-radius: 999px; background: rgba(255,255,255,0.03); }
    .grid { display:grid; grid-template-columns: 1.25fr 0.85fr; gap: 16px; margin-top: 16px; }
    @media (max-width: 940px) { .grid { grid-template-columns: 1fr; } }
    .card { background: rgba(16,21,37,0.70); border: 1px solid var(--line); border-radius: 18px;
            box-shadow: var(--shadow); overflow:hidden; }
    .card .hd { padding: 14px 16px; border-bottom: 1px solid var(--line); display:flex; align-items:center; justify-content:space-between; }
    .card .hd h2 { margin:0; font-size: 14px; letter-spacing: 0.35px; text-transform: uppercase; color: var(--muted); }
    .card .bd { padding: 14px 16px; }
    .btn { display:inline-block; cursor:pointer; padding: 10px 12px; border-radius: 12px;
           border: 1px solid var(--line); background: rgba(255,255,255,0.03); color: var(--text); font-weight: 700; }
    .btn:hover { background: rgba(255,255,255,0.06); }
    .btn.primary { border-color: rgba(182,255,59,0.35); background: rgba(182,255,59,0.12); }
    .btn.danger { border-color: rgba(255,77,103,0.35); background: rgba(255,77,103,0.10); }
    .row { display:flex; gap: 10px; flex-wrap: wrap; }
    input, textarea, select { width: 100%; box-sizing: border-box; padding: 10px 12px;
            background: rgba(0,0,0,0.20); border: 1px solid var(--line); border-radius: 12px; color: var(--text); }
    textarea { min-height: 110px; resize: vertical; }
    .muted { color: var(--muted); }
    .hr { height: 1px; background: var(--line); margin: 14px 0; }
    .chip { display:inline-block; padding: 4px 10px; border-radius: 999px; border: 1px solid rgba(182,255,59,0.28);
            background: var(--chip); color: var(--brand); font-size: 12px; font-weight: 750; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12.5px; }
    .post { border: 1px solid var(--line); border-radius: 16px; padding: 12px; margin-bottom: 10px;
            background: rgba(0,0,0,0.16); }
    .post .meta { display:flex; gap: 10px; align-items:center; justify-content:space-between; }
    .post .who { font-weight: 800; }
    .post .txt { margin-top: 8px; white-space: pre-wrap; line-height: 1.35; }
    .k { color: var(--muted); font-weight: 700; }
    .v { color: var(--text); font-weight: 820; }
    .kv { display:grid; grid-template-columns: 120px 1fr; gap: 6px 12px; }
    .small { font-size: 12px; }
    """

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{html.escape(title)} — {APP_NAME}</title>
  <style>{css}</style>
  {head_extra}
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="brand">
        <div class="logo"></div>
        <div>
          <div class="title">{APP_NAME}</div>
          <div class="muted small">meme launcher + social aggregator</div>
        </div>
      </div>
      <div class="row">
        <a class="pill" href="/">feed</a>
        <a class="pill" href="/launches">launches</a>
        <a class="pill" href="/admin">admin</a>
        <span class="pill mono">v{APP_VERSION}</span>
      </div>
    </div>
    {body}
  </div>
</body>
</html>"""


# --------------------------- DATABASE ---------------------------
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS local_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  handle TEXT NOT NULL UNIQUE,
  created_at INTEGER NOT NULL,
  api_key_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  lane TEXT NOT NULL,
  author TEXT NOT NULL,
  body TEXT NOT NULL,
  body_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  parent_id INTEGER,
  tags_json TEXT NOT NULL,
  attachments_json TEXT NOT NULL,
  chain_post_id INTEGER,
  chain_tx TEXT,
  score REAL NOT NULL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_lane ON posts(lane, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author, created_at DESC);

CREATE TABLE IF NOT EXISTS launches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chain_launch_id INTEGER,
  token_address TEXT,
  creator TEXT,
  ticker_hash TEXT,
  minted_supply TEXT,
  start_at INTEGER,
  end_at INTEGER,
  mode INTEGER,
  fee_bps INTEGER,
  finalized INTEGER,
  eth_reserve TEXT,
  token_reserve TEXT,
  final_price_e18 TEXT,
  last_seen_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_launches_chain_id ON launches(chain_launch_id);

CREATE TABLE IF NOT EXISTS ingest_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  lane TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  last_pull_at INTEGER
);

CREATE TABLE IF NOT EXISTS ingest_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  ext_id TEXT NOT NULL,
  title TEXT,
  url TEXT,
  author TEXT,
  body TEXT,
  body_hash TEXT NOT NULL,
  published_at INTEGER,
  lane TEXT NOT NULL,
  imported INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  UNIQUE(source_id, ext_id)
);

CREATE INDEX IF NOT EXISTS idx_ingest_items_published ON ingest_items(published_at DESC);

CREATE TABLE IF NOT EXISTS chain_cursor (
  id INTEGER PRIMARY KEY CHECK (id=1),
  last_block INTEGER NOT NULL,
  last_poll_at INTEGER NOT NULL
);
"""


async def db_init(db: aiosqlite.Connection) -> None:
    await db.executescript(SCHEMA)
    await db.commit()

    # Ensure chain_cursor exists (even in demo mode)
    cur = await db.execute("SELECT COUNT(*) FROM chain_cursor WHERE id=1")
    n = (await cur.fetchone())[0]
    if n == 0:
        await db.execute("INSERT INTO chain_cursor (id, last_block, last_poll_at) VALUES (1, 0, 0)")
        await db.commit()

    # Ensure a secret exists for cookie signing
    secret = await meta_get(db, "cookie_secret")
    if not secret:
        secret = secrets.token_hex(32)
        await meta_set(db, "cookie_secret", secret)
