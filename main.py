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

    # Seed demo sources (RSS-like placeholders) without requiring network
    seeded = await meta_get(db, "seeded_sources")
    if not seeded:
        await seed_sources(db)
        await meta_set(db, "seeded_sources", "1")


async def meta_get(db: aiosqlite.Connection, k: str) -> str | None:
    cur = await db.execute("SELECT v FROM meta WHERE k=?", (k,))
    row = await cur.fetchone()
    return None if row is None else row[0]


async def meta_set(db: aiosqlite.Connection, k: str, v: str) -> None:
    await db.execute("INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))
    await db.commit()


async def seed_sources(db: aiosqlite.Connection) -> None:
    now = now_ts()
    rows = [
        ("mock", "MemeWire", "mock://memewire", "memes", 1, now),
        ("mock", "AlphaGeyser", "mock://alphageyser", "alpha", 1, now),
        ("mock", "SecScream", "mock://secscream", "sec", 1, now),
        ("mock", "IRLDrip", "mock://irldrip", "irl", 1, now),
    ]
    await db.executemany(
        "INSERT INTO ingest_sources(kind, name, url, lane, enabled, created_at) VALUES(?,?,?,?,?,?)",
        rows,
    )
    await db.commit()


# ----------------------------- AUTH -----------------------------
def api_key() -> str:
    raw = secrets.token_bytes(24)
    return "oh_" + _b64url(raw)


def api_key_hash(key: str) -> str:
    return sha256_hex(key.encode("utf-8"))


def sign_cookie(secret_hex: str, payload: dict) -> str:
    msg = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    secret = bytes.fromhex(secret_hex)
    sig = hmac.new(secret, msg, hashlib.sha256).digest()
    return _b64url(msg) + "." + _b64url(sig)


def verify_cookie(secret_hex: str, token: str) -> dict | None:
    try:
        msg_b64, sig_b64 = token.split(".", 1)
        msg = _unb64url(msg_b64)
        sig = _unb64url(sig_b64)
        secret = bytes.fromhex(secret_hex)
        exp = hmac.new(secret, msg, hashlib.sha256).digest()
        if not hmac.compare_digest(exp, sig):
            return None
        payload = json.loads(msg.decode("utf-8"))
        if not isinstance(payload, dict):
            return None
        return payload
    except Exception:
        return None


async def create_local_user(db: aiosqlite.Connection, handle: str) -> tuple[int, str]:
    handle = normalize_handle(handle)
    if len(handle) < 3:
        raise ValueError("handle too short")
    key = api_key()
    h = api_key_hash(key)
    now = now_ts()
    cur = await db.execute(
        "INSERT INTO local_users(handle, created_at, api_key_hash) VALUES(?,?,?)",
        (handle, now, h),
    )
    await db.commit()
    return int(cur.lastrowid), key


async def auth_user_by_api_key(db: aiosqlite.Connection, key: str) -> dict | None:
    if not key or not key.startswith("oh_"):
        return None
    h = api_key_hash(key)
    cur = await db.execute("SELECT id, handle, created_at FROM local_users WHERE api_key_hash=?", (h,))
    row = await cur.fetchone()
    if row is None:
        return None
    return {"id": row[0], "handle": row[1], "created_at": row[2]}


# -------------------------- MODELS --------------------------
class PostIn(BaseModel):
    author: str = Field(..., min_length=2, max_length=64)
    body: str = Field(..., min_length=1, max_length=4000)
    lane: str | None = Field(None, max_length=32)
    parent_id: int | None = None
    tags: list[str] = Field(default_factory=list, max_length=20)
    attachments: list[dict] = Field(default_factory=list, max_length=10)
    push_chain: bool = False


class SourceIn(BaseModel):
    kind: str = Field(..., max_length=16)
    name: str = Field(..., max_length=80)
    url: str = Field(..., max_length=400)
    lane: str = Field(..., max_length=32)
    enabled: bool = True


class LaunchCreateIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    symbol: str = Field(..., min_length=1, max_length=16)
    supply: int = Field(..., ge=100_000, le=10_000_000_000)
    ticker: str = Field(..., min_length=1, max_length=32)
    minutes: int = Field(120, ge=10, le=60 * 24 * 7)
    mode: str = Field("fixed")
    start_price: float = Field(0.000001, gt=0.0, le=3000000.0)
    fee_bps: int = Field(137, ge=0, le=999)


# ------------------------ MOCK INGEST ------------------------
def _mock_items(seed: str, lane: str, n: int = 8) -> list[dict]:
    rng = random.Random(seed + "|" + lane + "|" + str(int(time.time() // 3600)))
    verbs = ["apes", "deploys", "cooks", "dunks", "speedruns", "summons", "front-runs", "resurrects", "mints", "ships"]
    nouns = ["frogs", "cats", "whales", "charts", "memes", "audits", "threads", "bags", "lanes", "portals"]
    moods = ["based", "cursed", "radiant", "sus", "glorious", "unhinged", "liquid", "spicy", "chill", "volatile"]
    items: list[dict] = []
    for i in range(n):
        who = rng.choice(["pilto", "anon", "capy", "owl", "haus", "goho", "glitch", "relay", "snek", "catdad"])
        title = f"{who} {rng.choice(verbs)} {rng.choice(nouns)} ({rng.choice(moods)})"
        body = (
            f"{title}\n\n"
            f"lane={lane} | pulse={rng.randint(1, 999)} | spice={rng.randint(11, 9999)}\n"
            f"signal: {rng.choice(['WAGMI', 'NGMI', 'DYOR', 'NFA', 'GM', 'GNGMI', 'fr fr'])}\n"
        )
        ext_id = sha256_hex((seed + str(i) + title).encode("utf-8"))[:24]
        url = f"https://example.invalid/{lane}/{ext_id}"
        items.append(
            {
                "ext_id": ext_id,
                "title": title,
                "url": url,
                "author": who,
                "body": body,
                "published_at": now_ts() - rng.randint(10, 3600 * 10),
                "lane": lane,
            }
        )
    return items


async def ingest_pull_once(db: aiosqlite.Connection) -> int:
    # Pull enabled sources and insert items if missing.
    cur = await db.execute(
        "SELECT id, kind, name, url, lane FROM ingest_sources WHERE enabled=1 ORDER BY id ASC"
    )
    rows = await cur.fetchall()
    inserted = 0
    for (sid, kind, name, url, lane) in rows:
        if kind != "mock":
            continue
        items = _mock_items(url + "|" + name, lane, n=8)
        for it in items:
            body = safe_text(it["body"], 4000)
            bh = sha256_hex(body.encode("utf-8"))
            try:
                await db.execute(
                    """
                    INSERT INTO ingest_items(source_id, ext_id, title, url, author, body, body_hash, published_at, lane, imported, created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        sid,
                        it["ext_id"],
                        it["title"],
                        it["url"],
                        it["author"],
                        body,
                        bh,
                        it["published_at"],
                        lane,
                        0,
                        now_ts(),
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass
        await db.execute("UPDATE ingest_sources SET last_pull_at=? WHERE id=?", (now_ts(), sid))
        await db.commit()
    return inserted


async def import_ingest_items(db: aiosqlite.Connection, limit: int = 20) -> int:
    # Take top items and turn into posts (local feed).
    cur = await db.execute(
        """
        SELECT id, title, url, author, body, body_hash, published_at, lane
        FROM ingest_items
        WHERE imported=0
        ORDER BY COALESCE(published_at, created_at) DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = await cur.fetchall()
    n = 0
    for row in rows:
        iid, title, url, author, body, body_hash, published_at, lane = row
        lane2 = lane or guess_lane_from_text(body or "")
        body2 = safe_text(body or "", 4000)
        tags = [lane2, "ingest", "wire"]
        att = [{"kind": "url", "data": url or ""}]
        await add_post(
            db,
            source="ingest",
            lane=lane2,
            author=str(author or "wire"),
            body=body2,
            parent_id=None,
            tags=tags,
            attachments=att,
            chain_post_id=None,
            chain_tx=None,
            created_at=int(published_at or now_ts()),
        )
        await db.execute("UPDATE ingest_items SET imported=1 WHERE id=?", (iid,))
        n += 1
    await db.commit()
    return n


# ----------------------- POSTS / RANK -----------------------
async def add_post(
    db: aiosqlite.Connection,
    *,
    source: str,
    lane: str,
    author: str,
    body: str,
    parent_id: int | None,
    tags: list[str],
    attachments: list[dict],
    chain_post_id: int | None,
    chain_tx: str | None,
    created_at: int | None = None,
) -> int:
    body = safe_text(body, 4000)
    lane = safe_text(lane, 32) or "general"
    author = safe_text(author, 64) or "anon"
    created = now_ts() if created_at is None else int(created_at)

    tags2 = []
    for t0 in tags[:20]:
        t1 = safe_text(str(t0), 32).lower()
        t1 = re.sub(r"[^a-z0-9_\\-\\.]", "", t1)
        if t1:
            tags2.append(t1)
    if not tags2:
        tags2 = [lane]

    at2: list[dict] = []
    for a in attachments[:10]:
        if not isinstance(a, dict):
            continue
        kind = safe_text(str(a.get("kind", "")), 16).lower()
        data = safe_text(str(a.get("data", "")), 600)
        if not kind:
            continue
        at2.append({"kind": kind, "data": data, "hash": keccak_like_hex(data.encode("utf-8"))})

    bh = sha256_hex(body.encode("utf-8"))

    # Heuristic score: recency + lane spice
    lane_spice = (int(sha256_hex(lane.encode("utf-8"))[:6], 16) % 97) / 100.0
    age = max(0, now_ts() - created)
    rec = max(0.0, 2.0 - (age / 3600.0))  # fades over ~2h
    score = float(rec + lane_spice)

    cur = await db.execute(
        """
        INSERT INTO posts(source, lane, author, body, body_hash, created_at, parent_id, tags_json, attachments_json, chain_post_id, chain_tx, score)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            source,
            lane,
            author,
            body,
            bh,
            created,
            parent_id,
            json.dumps(tags2),
            json.dumps(at2),
            chain_post_id,
            chain_tx,
            score,
        ),
    )
    await db.commit()
    return int(cur.lastrowid)


async def list_posts(
    db: aiosqlite.Connection,
    *,
    lane: str | None,
    q: str | None,
    limit: int = 40,
) -> list[dict]:
    limit = clamp(limit, 1, 120)
    where = []
    params: list[t.Any] = []

    if lane:
        where.append("lane=?")
        params.append(lane)
    if q:
        qs = "%" + q.strip() + "%"
        where.append("(body LIKE ? OR author LIKE ? OR tags_json LIKE ?)")
        params.extend([qs, qs, qs])
    wsql = "WHERE " + " AND ".join(where) if where else ""
    cur = await db.execute(
        f"""
        SELECT id, source, lane, author, body, body_hash, created_at, parent_id, tags_json, attachments_json, chain_post_id, chain_tx, score
        FROM posts
        {wsql}
        ORDER BY score DESC, created_at DESC
        LIMIT ?
        """,
        (*params, limit),
    )
    rows = await cur.fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "source": r[1],
                "lane": r[2],
                "author": r[3],
                "body": r[4],
                "body_hash": r[5],
                "created_at": r[6],
                "parent_id": r[7],
                "tags": json.loads(r[8] or "[]"),
                "attachments": json.loads(r[9] or "[]"),
                "chain_post_id": r[10],
                "chain_tx": r[11],
                "score": r[12],
            }
        )
    return out


async def list_lanes(db: aiosqlite.Connection) -> list[dict]:
    cur = await db.execute(
        "SELECT lane, COUNT(*) AS c FROM posts GROUP BY lane ORDER BY c DESC, lane ASC LIMIT 30"
    )
    rows = await cur.fetchall()
    return [{"lane": r[0], "count": r[1]} for r in rows]


# ------------------------ CHAIN (OPT) ------------------------
GOHO_ABI_MIN = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "postId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "author", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "parentId", "type": "uint256"},
            {"indexed": False, "internalType": "bytes32", "name": "lane", "type": "bytes32"},
            {"indexed": False, "internalType": "bytes32", "name": "contentHash", "type": "bytes32"},
            {"indexed": False, "internalType": "uint64", "name": "flags", "type": "uint64"},
            {"indexed": False, "internalType": "uint64", "name": "createdAt", "type": "uint64"},
        ],
        "name": "GH_Post",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "launchId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "token", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "creator", "type": "address"},
            {"indexed": False, "internalType": "bytes32", "name": "tickerHash", "type": "bytes32"},
            {"indexed": False, "internalType": "uint256", "name": "mintedSupply", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "startAt", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "endAt", "type": "uint256"},
        ],
        "name": "GH_LaunchCreated",
        "type": "event",
    },
]


@dataclasses.dataclass
class ChainCtx:
    w3: t.Any
    contract: t.Any
    ok: bool
    why: str


def chain_ctx() -> ChainCtx:
    if DEMO_MODE:
        return ChainCtx(None, None, False, "demo mode (no web3 / rpc / contract address)")
    assert Web3 is not None
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    if not w3.is_connected():
        return ChainCtx(None, None, False, "rpc not reachable")
    try:
        ca = Web3.to_checksum_address(CONTRACT_ADDRESS)  # type: ignore
    except Exception:
        return ChainCtx(None, None, False, "bad contract address")
    c = w3.eth.contract(address=ca, abi=GOHO_ABI_MIN)
    return ChainCtx(w3, c, True, "ok")


async def chain_poll_loop(app: FastAPI) -> None:
    while True:
        try:
