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
