import base64
import asyncio
import hashlib
import hmac
import inspect
import json
import mimetypes
import os
import queue
import re
import secrets
import threading
import time
from concurrent.futures import wait
from pathlib import Path
from urllib.parse import quote

import requests
from flask import Flask, Response, jsonify, redirect, request, stream_with_context

try:
    import gevent
except Exception:
    gevent = None

try:
    from pyrogram import Client as PyrogramClient
except Exception:
    PyrogramClient = None

HTTP = requests.Session()


def load_local_env():
    for env_path in (Path(".env"), Path(__file__).with_name(".env")):
        if not env_path.exists():
            continue
        for line in env_path.read_text("utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_local_env()
app = Flask(__name__)


def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def csv_env(*names):
    for name in names:
        raw = os.getenv(name, "")
        if raw.strip():
            return [part.strip() for part in raw.split(",") if part.strip()]
    return []


CONFIG = {
    "port": int(os.getenv("PORT", "7860")),
    "public_engine_url": os.getenv("PUBLIC_ENGINE_URL", "https://atx-direct-atx-direct-download.hf.space").rstrip("/"),
    "public_blogger_url": os.getenv("PUBLIC_BLOGGER_URL", "https://atx-direct-downloads.blogspot.com").rstrip("/"),
    "storage_channel_id": os.getenv("STORAGE_CHANNEL_ID", "-1003716257573"),
    "log_channel_id": os.getenv("LOG_CHANNEL_ID", "-1003938522225"),
    "admin_ids": {value.strip() for value in os.getenv("ADMIN_IDS", "7784446308").split(",") if value.strip()},
    "signing_secret": os.getenv("LINK_SIGNING_SECRET", "CHANGE_THIS_LONG_RANDOM_SECRET"),
    "admin_secret": os.getenv("ADMIN_SECRET", "CHANGE_THIS_PRIVATE_ADMIN_SECRET"),
    "enable_polling": env_bool("ENABLE_POLLING", True),
    "enable_webhooks": env_bool("ENABLE_WEBHOOKS", False),
    "index_file": os.getenv("INDEX_FILE", "/data/file_index.json" if Path("/data").exists() else "./data/file_index.json"),
    "offsets_file": os.getenv("OFFSETS_FILE", "/data/update_offsets.json" if Path("/data").exists() else "./data/update_offsets.json"),
    "max_codes": int(os.getenv("MAX_CODES_PER_LINK", "500")),
    "max_range": int(os.getenv("MAX_RANGE_SIZE", "500")),
    "download_mode": os.getenv("DOWNLOAD_MODE", "redirect").strip().lower(),
    "bot_api_base_url": os.getenv("BOT_API_BASE_URL", "https://api.telegram.org").rstrip("/"),
    "bot_file_base_url": os.getenv("BOT_FILE_BASE_URL", "https://api.telegram.org/file").rstrip("/"),
    "large_file_base_url": os.getenv("LARGE_FILE_BASE_URL", "").rstrip("/"),
    "enable_large_stream": env_bool("ENABLE_LARGE_FILE_STREAM", True),
    "max_stream_file_size": int(os.getenv("MAX_STREAM_FILE_SIZE", str(2 * 1024 * 1024 * 1024))),
    "bot_api_direct_limit": int(os.getenv("BOT_API_DIRECT_LIMIT", str(20 * 1024 * 1024))),
    "max_streams_per_bot": int(os.getenv("MAX_STREAMS_PER_BOT", "25")),
    "import_timeout": int(os.getenv("IMPORT_TIMEOUT_SECONDS", "20")),
    "max_import_bots": int(os.getenv("MAX_IMPORT_BOTS_PER_CODE", "25")),
    "cors_origin": os.getenv("CORS_ORIGIN", "*"),
    "max_users_per_bot": int(os.getenv("MAX_USERS_PER_BOT", "400")),
}


DIRECT_TOKENS = csv_env("DIRECT_BOT_TOKENS", "DOWNLOAD_BOT_TOKENS", "BOT_TOKENS", "BOT_TOKEN")
MAINTENANCE_TOKENS = csv_env("MAINTENANCE_BOT_TOKENS", "ADMIN_BOT_TOKENS")
DIRECT_SLOTS = [{"slot": index, "name": f"Direct Bot {index + 1}", "token": token} for index, token in enumerate(DIRECT_TOKENS)]
MAINTENANCE_SLOTS = [{"slot": index, "name": f"Maintenance Bot {index + 1}", "token": token} for index, token in enumerate(MAINTENANCE_TOKENS)]
ALL_SLOTS = DIRECT_SLOTS + MAINTENANCE_SLOTS


class PostCodeError(ValueError):
    pass


class IndexStore:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.data = {"version": 1, "files": {}, "links": {}, "downloads": {}}
        self.load()

    def load(self):
        with self.lock:
            if self.path.exists():
                self.data = json.loads(self.path.read_text("utf-8"))
            self.data.setdefault("version", 1)
            self.data.setdefault("files", {})
            self.data.setdefault("links", {})
            self.data.setdefault("downloads", {})

    def save(self):
        with self.lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.path.with_suffix(self.path.suffix + ".tmp")
            temp_path.write_text(json.dumps(self.data, indent=2, sort_keys=True), "utf-8")
            temp_path.replace(self.path)

    def upsert_message(self, slot, message):
        media = extract_media(message)
        if not media:
            return None
        code = find_explicit_code(message.get("caption", "")) or str(message.get("message_id"))
        now = int(time.time())
        file_name = media.get("file_name") or caption_name(message.get("caption", "")) or f"telegram-{code}.mp4"

        with self.lock:
            item = self.data.setdefault("files", {}).setdefault(code, {
                "code": code,
                "file_name": file_name,
                "mime_type": media.get("mime_type") or "application/octet-stream",
                "file_size": int(media.get("file_size") or 0),
                "duration": int(media.get("duration") or 0),
                "source_chat_id": str(message.get("chat", {}).get("id", "")),
                "source_message_id": int(message.get("message_id") or 0),
                "updated_at": now,
                "bots": {},
            })
            item.update({
                "file_name": file_name,
                "mime_type": media.get("mime_type") or item.get("mime_type") or "application/octet-stream",
                "file_size": int(media.get("file_size") or item.get("file_size") or 0),
                "duration": int(media.get("duration") or item.get("duration") or 0),
                "source_chat_id": str(message.get("chat", {}).get("id", "")),
                "source_message_id": int(message.get("message_id") or 0),
                "updated_at": now,
            })
            item.setdefault("bots", {})[str(slot["slot"])] = {
                "file_id": media["file_id"],
                "file_unique_id": media.get("file_unique_id", ""),
                "bot_name": slot["name"],
                "updated_at": now,
            }
            self.save()
            return item

    def upsert_pyrogram_message(self, slot, message):
        media = pyrogram_media_from(message)
        if not media:
            return None
        caption = getattr(message, "caption", "") or ""
        code = find_explicit_code(caption) or str(getattr(message, "id", ""))
        now = int(time.time())
        file_name = getattr(media, "file_name", "") or caption_name(caption) or f"telegram-{code}.mp4"
        mime_type = getattr(media, "mime_type", "") or guess_mime(file_name)
        file_size = int(getattr(media, "file_size", 0) or 0)
        duration = int(getattr(media, "duration", 0) or 0)
        file_id = getattr(media, "file_id", "")
        file_unique_id = getattr(media, "file_unique_id", "")

        with self.lock:
            item = self.data.setdefault("files", {}).setdefault(code, {
                "code": code,
                "file_name": file_name,
                "mime_type": mime_type,
                "file_size": file_size,
                "duration": duration,
                "source_chat_id": str(CONFIG["storage_channel_id"]),
                "source_message_id": int(getattr(message, "id", 0) or 0),
                "updated_at": now,
                "bots": {},
            })
            item.update({
                "file_name": file_name,
                "mime_type": mime_type,
                "file_size": file_size,
                "duration": duration,
                "source_chat_id": str(CONFIG["storage_channel_id"]),
                "source_message_id": int(getattr(message, "id", 0) or 0),
                "updated_at": now,
            })
            if file_id:
                item.setdefault("bots", {})[str(slot["slot"])] = {
                    "file_id": file_id,
                    "file_unique_id": file_unique_id,
                    "bot_name": slot["name"],
                    "updated_at": now,
                }
            self.save()
            return item

    def update_bot_file_path(self, code, slot, file_path):
        now = int(time.time())
        with self.lock:
            item = self.data.get("files", {}).get(str(code))
            if not item:
                return
            bot_file = item.setdefault("bots", {}).setdefault(str(slot["slot"]), {"bot_name": slot["name"]})
            bot_file["file_path"] = file_path
            bot_file["file_path_cached_at"] = now
            item["updated_at"] = now
            self.save()

    def get(self, code):
        with self.lock:
            return self.data.get("files", {}).get(str(code))

    def all_codes(self):
        with self.lock:
            return list(self.data.get("files", {}).keys())

    def create_page_token(self, expression, ttl_seconds):
        now = int(time.time())
        token = secrets.token_urlsafe(32)
        with self.lock:
            self.cleanup_tokens(now)
            self.data.setdefault("links", {})[token] = {
                "expression": expression,
                "expires_at": now + int(ttl_seconds),
                "created_at": now,
            }
            self.save()
        return token

    def get_page_expression(self, token):
        now = int(time.time())
        with self.lock:
            self.cleanup_tokens(now)
            item = self.data.get("links", {}).get(str(token))
            if not item or int(item.get("expires_at") or 0) < now:
                return ""
            return item.get("expression", "")

    def create_download_token(self, code, ttl_seconds):
        now = int(time.time())
        token = secrets.token_urlsafe(32)
        with self.lock:
            self.cleanup_tokens(now)
            self.data.setdefault("downloads", {})[token] = {
                "code": str(code),
                "expires_at": now + int(ttl_seconds),
                "created_at": now,
            }
            self.save()
        return token

    def get_download_code(self, token):
        now = int(time.time())
        with self.lock:
            self.cleanup_tokens(now)
            item = self.data.get("downloads", {}).get(str(token))
            if not item or int(item.get("expires_at") or 0) < now:
                return ""
            return item.get("code", "")

    def cleanup_tokens(self, now=None):
        now = int(now or time.time())
        for key in ("links", "downloads"):
            values = self.data.setdefault(key, {})
            for token in list(values.keys()):
                if int(values[token].get("expires_at") or 0) < now:
                    values.pop(token, None)

    def stats(self):
        with self.lock:
            files = self.data.get("files", {})
            return {
                "files": len(files),
                "directBots": len(DIRECT_SLOTS),
                "maintenanceBots": len(MAINTENANCE_SLOTS),
                "indexedBotReferences": sum(len(item.get("bots", {})) for item in files.values()),
                "cachedFilePaths": sum(1 for item in files.values() for bot in item.get("bots", {}).values() if bot.get("file_path")),
                "largeFileStreamReady": bool(DIRECT_SLOTS) and len(PYRO_READY_SLOTS) == len(DIRECT_SLOTS),
                "largeFileStreamBots": len(PYRO_READY_SLOTS),
                "maxStreamsPerBot": CONFIG["max_streams_per_bot"],
                "largeStreamCapacity": len(PYRO_READY_SLOTS) * CONFIG["max_streams_per_bot"],
                "botApiDirectLimit": CONFIG["bot_api_direct_limit"],
                "activePageTokens": len(self.data.get("links", {})),
                "activeDownloadTokens": len(self.data.get("downloads", {})),
                "downloadMode": CONFIG["download_mode"],
            }


INDEX = IndexStore(CONFIG["index_file"])
OFFSET_LOCK = threading.RLock()
ROUND_ROBIN_LOCK = threading.RLock()
ROUND_ROBIN_CURSOR = 0
BOT_LOAD_LOCK = threading.RLock()
BOT_LOAD = {}
BOT_LEASE_SECONDS = int(os.getenv("BOT_LEASE_SECONDS", "1800"))
PYRO_LOOP = None
PYRO_CLIENTS = {}
PYRO_READY_SLOTS = set()
PYRO_ERRORS = {}
PYRO_LOCK = threading.RLock()
PYRO_ACTIVE_STREAMS = {}


@app.after_request
def add_headers(response):
    response.headers["Access-Control-Allow-Origin"] = CONFIG["cors_origin"]
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,X-Admin-Secret"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Cache-Control"] = "no-store"
    return response


@app.route("/")
def home():
    return jsonify({
        "ok": True,
        "service": "hf-direct-engine",
        "message": "ATX Direct Engine is running. Use /health or /api/create-link.",
        "stats": INDEX.stats(),
    })


@app.route("/health")
def health():
    return jsonify({"ok": True, "service": "hf-direct-engine", "stats": INDEX.stats()})


@app.route("/api/files")
def api_files():
    page_token = request.args.get("token") or request.args.get("link") or ""
    if page_token:
        expression = INDEX.get_page_expression(page_token)
        if not expression:
            return error("LINK_EXPIRED", "This download page link is invalid or expired.", 403)
        parsed = parse_post_codes(expression)
    else:
        expression = request.args.get("start") or request.args.get("codes") or ""
        parsed = parse_post_codes(expression)
        verify_signature("list", parsed["expression"], request.args)

    files = []
    missing = []
    for code in parsed["codes"]:
        item = INDEX.get(code)
        if not item:
            item = import_code_from_storage(code)
        if not item:
            missing.append(code)
            continue
        files.append(public_file(item))

    return jsonify({"ok": True, "files": files, "missingCount": len(missing)})


@app.route("/api/download/<code>")
def api_download(code):
    verify_signature("download", code, request.args)
    return serve_download(code)


@app.route("/api/download-token/<token>")
def api_download_token(token):
    code = INDEX.get_download_code(token)
    if not code:
        return error("DOWNLOAD_LINK_EXPIRED", "This download button link is invalid or expired.", 403)
    return serve_download(code)


def serve_download(code):
    item = INDEX.get(code)
    if not item:
        return error("FILE_NOT_FOUND", "No indexed file exists for this post code.", 404)

    client_key = request.args.get("client") or request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()
    attempts = choose_bot_attempts(item, client_key)
    if not attempts:
        return error("NO_BOT_FILE_ID", "No direct bot has indexed this file yet.", 404)

    if should_stream_large_file(item):
        stream_slot = best_large_stream_slot(attempts)
        if not stream_slot:
            return error("LARGE_FILE_STREAM_NOT_READY", "No large-file bot is ready right now.", 503, {"streamers": large_stream_status()})
        fallback = stream_large_file_response(item, stream_slot)
        if fallback:
            return fallback
        return error("LARGE_FILE_STREAM_NOT_READY", "Large-file streaming is not ready right now.", 503, {"streamers": large_stream_status()})

    last_error = None
    for slot, bot_file in attempts:
        try:
            file_path = bot_file.get("file_path")
            if not file_path:
                telegram_file = telegram_api(slot, "getFile", {"file_id": bot_file["file_id"]}, timeout=8)
                file_path = telegram_file["file_path"]
                INDEX.update_bot_file_path(code, slot, file_path)
            file_url = telegram_file_url(slot["token"], file_path)
            log_event("info", f"{slot['name']} generated download link", {"code": code, "file": item.get("file_name", ""), "mode": CONFIG["download_mode"], "load": active_load_count(slot["slot"])})
            if CONFIG["download_mode"] == "proxy":
                return proxy_download(file_url, item)
            response = redirect(file_url, code=302)
            response.headers["Content-Disposition"] = f"attachment; filename=\"{safe_filename(item.get('file_name'))}\""
            return response
        except Exception as exc:
            last_error = str(exc)
            if is_file_too_big_error(last_error):
                stream_slot = best_large_stream_slot(attempts)
                if not stream_slot:
                    return error("LARGE_FILE_STREAM_BUSY", "All large-file bots are busy right now.", 429, {"streamers": large_stream_status()})
                fallback = stream_large_file_response(item, stream_slot)
                if fallback:
                    log_event("info", "Using in-engine large-file stream", {"code": code, "file": item.get("file_name", ""), "bot": stream_slot["name"]})
                    return fallback
                fallback = large_file_fallback(code)
                if fallback:
                    log_event("info", "Using external large-file stream", {"code": code, "file": item.get("file_name", ""), "bot": slot["name"]})
                    return fallback
                return error(
                    "LARGE_FILE_STREAM_NOT_READY",
                    "Large-file streaming is not ready right now.",
                    413,
                    {"reason": last_error},
                )
            log_event("error", f"{slot['name']} failed for code {code}", {"reason": last_error})

    return error("TELEGRAM_LINK_FAILED", "No direct bot could generate this Telegram link right now.", 502, {"reason": last_error})


@app.route("/api/create-link")
def api_create_link():
    admin_secret = request.headers.get("X-Admin-Secret") or request.args.get("adminSecret") or ""
    if not CONFIG["admin_secret"] or admin_secret != CONFIG["admin_secret"]:
        return error("ADMIN_SECRET_REQUIRED", "A valid admin secret is required.", 403)
    expression = request.args.get("start") or request.args.get("codes") or ""
    parsed = parse_post_codes(expression)
    ttl = int(request.args.get("ttl") or 6 * 60 * 60)
    return jsonify({"ok": True, "url": build_blogger_link(parsed["expression"], ttl), "expression": parsed["expression"]})


@app.route("/api/admin/stats")
def api_admin_stats():
    admin_secret = request.headers.get("X-Admin-Secret") or request.args.get("adminSecret") or ""
    if not CONFIG["admin_secret"] or admin_secret != CONFIG["admin_secret"]:
        return error("ADMIN_SECRET_REQUIRED", "A valid admin secret is required.", 403)
    return jsonify({"ok": True, "stats": INDEX.stats()})


@app.route("/api/admin/warm-cache")
def api_admin_warm_cache():
    admin_secret = request.headers.get("X-Admin-Secret") or request.args.get("adminSecret") or ""
    if not CONFIG["admin_secret"] or admin_secret != CONFIG["admin_secret"]:
        return error("ADMIN_SECRET_REQUIRED", "A valid admin secret is required.", 403)
    started = warm_cache()
    return jsonify({"ok": True, "started": started})


@app.route("/api/admin/setup-webhooks")
def api_admin_setup_webhooks():
    admin_secret = request.headers.get("X-Admin-Secret") or request.args.get("adminSecret") or ""
    if not CONFIG["admin_secret"] or admin_secret != CONFIG["admin_secret"]:
        return error("ADMIN_SECRET_REQUIRED", "A valid admin secret is required.", 403)
    return jsonify({"ok": True, "webhooks": setup_webhooks()})


@app.route("/api/admin/telegram-status")
def api_admin_telegram_status():
    admin_secret = request.headers.get("X-Admin-Secret") or request.args.get("adminSecret") or ""
    if not CONFIG["admin_secret"] or admin_secret != CONFIG["admin_secret"]:
        return error("ADMIN_SECRET_REQUIRED", "A valid admin secret is required.", 403)
    return jsonify({"ok": True, "status": telegram_status()})


@app.route("/api/admin/test-log")
def api_admin_test_log():
    admin_secret = request.headers.get("X-Admin-Secret") or request.args.get("adminSecret") or ""
    if not CONFIG["admin_secret"] or admin_secret != CONFIG["admin_secret"]:
        return error("ADMIN_SECRET_REQUIRED", "A valid admin secret is required.", 403)
    log_event("info", "Manual test log from HF Direct Engine", {"files": INDEX.stats().get("files")})
    return jsonify({"ok": True, "message": "Test log sent if the maintenance bot and log channel are configured correctly."})


@app.route("/api/admin/stream-status")
def api_admin_stream_status():
    admin_secret = request.headers.get("X-Admin-Secret") or request.args.get("adminSecret") or ""
    if not CONFIG["admin_secret"] or admin_secret != CONFIG["admin_secret"]:
        return error("ADMIN_SECRET_REQUIRED", "A valid admin secret is required.", 403)
    return jsonify({"ok": True, "stats": INDEX.stats(), "streamers": large_stream_status()})


@app.route("/api/admin/import-codes")
def api_admin_import_codes():
    admin_secret = request.headers.get("X-Admin-Secret") or request.args.get("adminSecret") or ""
    if not CONFIG["admin_secret"] or admin_secret != CONFIG["admin_secret"]:
        return error("ADMIN_SECRET_REQUIRED", "A valid admin secret is required.", 403)
    expression = request.args.get("start") or request.args.get("codes") or ""
    parsed = parse_post_codes(expression)
    imported = []
    missing = []
    errors = {}
    for code in parsed["codes"]:
        item, code_errors = import_code_from_storage(code, include_errors=True)
        if item:
            imported.append({"code": code, "fileName": item.get("file_name"), "botRefs": len(item.get("bots", {}))})
        else:
            missing.append(code)
        if code_errors:
            errors[code] = code_errors
    return jsonify({"ok": True, "imported": imported, "missing": missing, "errors": errors, "stats": INDEX.stats()})


@app.route("/api/admin/check-code")
def api_admin_check_code():
    admin_secret = request.headers.get("X-Admin-Secret") or request.args.get("adminSecret") or ""
    if not CONFIG["admin_secret"] or admin_secret != CONFIG["admin_secret"]:
        return error("ADMIN_SECRET_REQUIRED", "A valid admin secret is required.", 403)
    code = request.args.get("code") or request.args.get("msg") or ""
    if not re.match(r"^\d+$", code):
        return error("BAD_CODE", "Use /api/admin/check-code?code=25", 400)
    item, errors = import_code_from_storage(code, include_errors=True)
    return jsonify({
        "ok": True,
        "code": code,
        "indexed": bool(item),
        "file": public_file(item) if item else None,
        "errors": errors,
        "stats": INDEX.stats(),
        "streamers": large_stream_status(),
    })


@app.route("/telegram/webhook/<int:slot_id>", methods=["POST"])
def telegram_webhook(slot_id):
    if slot_id < 0 or slot_id >= len(DIRECT_SLOTS):
        return error("BAD_SLOT", "Unknown direct bot slot.", 404)
    update = request.get_json(force=True, silent=True) or {}
    process_storage_update(DIRECT_SLOTS[slot_id], update)
    if not MAINTENANCE_SLOTS:
        process_admin_update(DIRECT_SLOTS[slot_id], update)
    return jsonify({"ok": True})


@app.route("/telegram/admin-webhook/<int:slot_id>", methods=["POST"])
def telegram_admin_webhook(slot_id):
    if slot_id < 0 or slot_id >= len(MAINTENANCE_SLOTS):
        return error("BAD_SLOT", "Unknown maintenance bot slot.", 404)
    process_admin_update(MAINTENANCE_SLOTS[slot_id], request.get_json(force=True, silent=True) or {})
    return jsonify({"ok": True})


@app.errorhandler(PostCodeError)
def handle_postcode_error(exc):
    return error("INVALID_POST_CODES", str(exc), 400)


@app.errorhandler(ValueError)
def handle_value_error(exc):
    return error("BAD_REQUEST", str(exc), 400)


def parse_post_codes(value):
    expression = re.sub(r"\s+", "", str(value or ""))
    if not expression:
        raise PostCodeError("No post code expression was provided.")
    if not re.match(r"^\d+(?:-\d+)?(?:_\d+(?:-\d+)?)*$", expression):
        raise PostCodeError("Use 1234, 1234-1267, 1245_1223_3421, or mixed underscore/range patterns.")
    codes = []
    seen = set()
    for segment in expression.split("_"):
        if "-" in segment:
            start_raw, end_raw = segment.split("-", 1)
            start, end = int(start_raw), int(end_raw)
            if start > end:
                raise PostCodeError(f"Invalid range {segment}. Range start must be lower than range end.")
            if end - start + 1 > CONFIG["max_range"]:
                raise PostCodeError(f"Range {segment} is too large. Max range size is {CONFIG['max_range']}.")
            for code in range(start, end + 1):
                add_code(str(code), codes, seen)
        else:
            add_code(segment, codes, seen)
    return {"expression": expression, "codes": codes}


def add_code(code, codes, seen):
    if code in seen:
        return
    if len(codes) >= CONFIG["max_codes"]:
        raise PostCodeError(f"Too many post codes. Max is {CONFIG['max_codes']}.")
    seen.add(code)
    codes.append(code)


def build_blogger_link(expression, ttl):
    token = INDEX.create_page_token(expression, ttl)
    separator = "&" if "?" in CONFIG["public_blogger_url"] else "?"
    return f"{CONFIG['public_blogger_url']}{separator}token={quote(token)}"


def signed_query(action, subject, ttl_seconds):
    exp = int(time.time()) + int(ttl_seconds)
    sig = sign(action, subject, exp)
    return f"exp={exp}&sig={quote(sig)}"


def verify_signature(action, subject, args):
    exp = int(args.get("exp") or "0")
    sig = args.get("sig") or ""
    if exp < int(time.time()):
        raise ValueError("This link has expired.")
    expected = sign(action, subject, exp)
    if not hmac.compare_digest(sig, expected):
        raise ValueError("This signed link is not valid.")


def sign(action, subject, exp):
    payload = f"{action}|{subject}|{exp}".encode("utf-8")
    digest = hmac.new(CONFIG["signing_secret"].encode("utf-8"), payload, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def public_file(item):
    code = item["code"]
    download_token = INDEX.create_download_token(code, 15 * 60)
    return {
        "fileName": item.get("file_name", f"File {code}"),
        "mimeType": item.get("mime_type", ""),
        "fileSize": item.get("file_size", 0),
        "sizeLabel": format_bytes(item.get("file_size", 0)),
        "duration": item.get("duration", 0),
        "downloadPath": f"/api/download-token/{quote(download_token)}",
    }


def choose_bot_attempts(item, client_key):
    bots = item.get("bots", {})
    available = []
    for slot in DIRECT_SLOTS:
        bot_file = bots.get(str(slot["slot"]))
        if bot_file:
            available.append((slot, bot_file))
    if not available:
        return []

    sticky = sticky_slot_for_client(client_key, available)
    if sticky is not None:
        preferred_slot_id = sticky
    else:
        preferred_slot_id = least_loaded_slot(available)
        assign_client_to_slot(client_key, preferred_slot_id)

    preferred = []
    fallback = []
    for pair in available:
        if pair[0]["slot"] == preferred_slot_id:
            preferred.append(pair)
        else:
            fallback.append(pair)
    return preferred + fallback


def sticky_slot_for_client(client_key, available):
    cleanup_bot_load()
    allowed = {slot["slot"] for slot, _ in available}
    with BOT_LOAD_LOCK:
        for slot_id, leases in BOT_LOAD.items():
            for lease in leases:
                if lease["client"] == client_key and slot_id in allowed:
                    lease["expires"] = time.time() + BOT_LEASE_SECONDS
                    return slot_id
    return None


def least_loaded_slot(available):
    cleanup_bot_load()
    best_slot = available[0][0]["slot"]
    best_count = 10**9
    with BOT_LOAD_LOCK:
        for slot, _ in available:
            count = len(BOT_LOAD.get(slot["slot"], []))
            if count < best_count and count < CONFIG["max_users_per_bot"]:
                best_slot = slot["slot"]
                best_count = count
        if best_count == 10**9:
            best_slot = min(available, key=lambda pair: len(BOT_LOAD.get(pair[0]["slot"], [])))[0]["slot"]
    return best_slot


def assign_client_to_slot(client_key, slot_id):
    with BOT_LOAD_LOCK:
        leases = BOT_LOAD.setdefault(slot_id, [])
        leases[:] = [lease for lease in leases if lease["client"] != client_key]
        leases.append({"client": client_key, "expires": time.time() + BOT_LEASE_SECONDS})


def active_load_count(slot_id):
    cleanup_bot_load()
    with BOT_LOAD_LOCK:
        return len(BOT_LOAD.get(slot_id, []))


def cleanup_bot_load():
    now = time.time()
    with BOT_LOAD_LOCK:
        for slot_id in list(BOT_LOAD.keys()):
            BOT_LOAD[slot_id] = [lease for lease in BOT_LOAD[slot_id] if lease["expires"] > now]
            if not BOT_LOAD[slot_id]:
                BOT_LOAD.pop(slot_id, None)


def telegram_api(slot, method, payload, timeout=15):
    response = HTTP.post(f"{CONFIG['bot_api_base_url']}/bot{slot['token']}/{method}", json=payload, timeout=timeout)
    data = response.json()
    if not response.ok or not data.get("ok"):
        raise RuntimeError(data.get("description") or f"Telegram {method} failed")
    return data["result"]


def telegram_file_url(token, file_path):
    return f"{CONFIG['bot_file_base_url']}/bot{token}/{quote(file_path, safe='/')}"


def proxy_download(file_url, item):
    upstream = HTTP.get(file_url, stream=True, timeout=30)
    if not upstream.ok:
        return error("TELEGRAM_DOWNLOAD_FAILED", "Telegram file download failed.", 502, {"status": upstream.status_code})
    headers = {
        "Content-Type": item.get("mime_type") or upstream.headers.get("content-type", "application/octet-stream"),
        "Content-Disposition": f"attachment; filename=\"{safe_filename(item.get('file_name'))}\"",
    }
    if upstream.headers.get("content-length"):
        headers["Content-Length"] = upstream.headers["content-length"]
    return Response(stream_with_context(upstream.iter_content(chunk_size=1024 * 256)), headers=headers)


def should_stream_large_file(item):
    file_size = int(item.get("file_size") or 0)
    return CONFIG["enable_large_stream"] and file_size > CONFIG["bot_api_direct_limit"]


def stream_large_file_response(item, slot):
    if not CONFIG["enable_large_stream"]:
        return None
    file_size = int(item.get("file_size") or 0)
    if file_size > CONFIG["max_stream_file_size"]:
        return error(
            "FILE_ABOVE_2GB_LIMIT",
            "This file is above the configured 2 GB limit and is not streamable.",
            413,
            {"fileSize": file_size, "maxSize": CONFIG["max_stream_file_size"]},
        )
    with PYRO_LOCK:
        if PYRO_ACTIVE_STREAMS.get(slot["slot"], 0) >= CONFIG["max_streams_per_bot"]:
            return error("LARGE_FILE_STREAM_BUSY", f"{slot['name']} is at stream capacity.", 429, {"streamers": large_stream_status()})
    if not ensure_pyrogram_ready(slot):
        return None

    headers = {
        "Content-Type": item.get("mime_type") or guess_mime(item.get("file_name")),
        "Content-Disposition": f"attachment; filename=\"{safe_filename(item.get('file_name'))}\"",
        "Accept-Ranges": "none",
        "Cache-Control": "no-store",
        "X-Accel-Buffering": "no",
    }
    if file_size:
        headers["Content-Length"] = str(file_size)

    log_event("info", f"{slot['name']} starting large-file stream", {"code": item.get("code"), "file": item.get("file_name"), "size": file_size})
    return Response(stream_with_context(pyrogram_stream_generator(item, slot)), headers=headers, direct_passthrough=True)


def pyrogram_stream_generator(item, slot):
    out = queue.Queue(maxsize=8)
    increment_pyro_active(slot)
    future = asyncio.run_coroutine_threadsafe(produce_pyrogram_stream(item, slot, out), PYRO_LOOP)
    try:
        while True:
            try:
                chunk = out.get_nowait()
            except queue.Empty:
                cooperative_sleep(0.05)
                continue
            if chunk is None:
                break
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk
    finally:
        if not future.done():
            future.cancel()
        decrement_pyro_active(slot)


def cooperative_sleep(seconds):
    if gevent is not None:
        gevent.sleep(seconds)
    else:
        time.sleep(seconds)


async def produce_pyrogram_stream(item, slot, out):
    try:
        client = PYRO_CLIENTS.get(slot["slot"])
        if not client:
            raise RuntimeError(f"{slot['name']} large-file client is not ready.")
        message_id = int(item.get("source_message_id") or item.get("code"))
        message = await client.get_messages(int(CONFIG["storage_channel_id"]), message_id)
        media = pyrogram_media_from(message)
        if not media:
            raise RuntimeError("No downloadable media found in this storage post.")
        async for chunk in client.stream_media(media):
            await async_queue_put(out, chunk)
    except Exception as exc:
        await async_queue_put(out, exc)
    finally:
        await async_queue_put(out, None)


async def async_queue_put(out, value):
    while True:
        try:
            out.put_nowait(value)
            return
        except queue.Full:
            await asyncio.sleep(0.05)


def best_large_stream_slot(attempts):
    ready = []
    with PYRO_LOCK:
        for slot, _bot_file in attempts:
            if slot["slot"] in PYRO_READY_SLOTS and slot["slot"] in PYRO_CLIENTS:
                ready.append(slot)
        if not ready:
            return None
        under_limit = [slot for slot in ready if PYRO_ACTIVE_STREAMS.get(slot["slot"], 0) < CONFIG["max_streams_per_bot"]]
        if not under_limit:
            return None
        return min(under_limit, key=lambda slot: PYRO_ACTIVE_STREAMS.get(slot["slot"], 0))


def increment_pyro_active(slot):
    with PYRO_LOCK:
        PYRO_ACTIVE_STREAMS[slot["slot"]] = PYRO_ACTIVE_STREAMS.get(slot["slot"], 0) + 1


def decrement_pyro_active(slot):
    with PYRO_LOCK:
        current = PYRO_ACTIVE_STREAMS.get(slot["slot"], 0) - 1
        if current > 0:
            PYRO_ACTIVE_STREAMS[slot["slot"]] = current
        else:
            PYRO_ACTIVE_STREAMS.pop(slot["slot"], None)


def import_code_from_storage(code, include_errors=False):
    errors = []
    if not CONFIG["enable_large_stream"] or not ensure_any_pyrogram_ready():
        errors.append({"bot": "global", "reason": "Large-file Pyrogram clients are not ready."})
        return (None, errors) if include_errors else None
    imported = None
    future_slots = {}
    ready_slots = []
    for slot in DIRECT_SLOTS:
        if not ensure_pyrogram_ready(slot, timeout=0.2):
            errors.append({"bot": slot["name"], "reason": "Streamer not ready for this bot."})
            continue
        ready_slots.append(slot)
        if CONFIG["max_import_bots"] and len(ready_slots) >= CONFIG["max_import_bots"]:
            break

    for slot in ready_slots:
        try:
            client = PYRO_CLIENTS.get(slot["slot"])
            if not client or not PYRO_LOOP:
                errors.append({"bot": slot["name"], "reason": "Streamer client missing."})
                continue
            future_slots[asyncio.run_coroutine_threadsafe(fetch_pyrogram_message(client, code), PYRO_LOOP)] = slot
        except Exception as exc:
            reason = str(exc)
            errors.append({"bot": slot["name"], "reason": reason})
            log_event("warn", f"{slot['name']} could not import storage post", {"code": code, "reason": reason})

    done, pending = wait(list(future_slots.keys()), timeout=CONFIG["import_timeout"])
    for future in pending:
        future.cancel()
        slot = future_slots[future]
        errors.append({"bot": slot["name"], "reason": f"Timed out after {CONFIG['import_timeout']} seconds."})

    for future in done:
        slot = future_slots[future]
        try:
            message = future.result()
            if not message or not pyrogram_media_from(message):
                errors.append({"bot": slot["name"], "reason": "Message exists but no supported media was found, or message was not returned."})
                continue
            item = INDEX.upsert_pyrogram_message(slot, message)
            if item:
                imported = item
        except Exception as exc:
            reason = str(exc)
            errors.append({"bot": slot["name"], "reason": reason})
            log_event("warn", f"{slot['name']} could not import storage post", {"code": code, "reason": reason})

    if imported:
        log_event("info", "Imported storage post by code", {"code": code, "file": imported.get("file_name"), "botRefs": len(imported.get("bots", {}))})
    return (imported, errors) if include_errors else imported


async def fetch_pyrogram_message(client, code):
    return await client.get_messages(int(CONFIG["storage_channel_id"]), int(code))


def pyrogram_media_from(message):
    return (
        getattr(message, "video", None)
        or getattr(message, "document", None)
        or getattr(message, "audio", None)
        or getattr(message, "animation", None)
    )


def guess_mime(file_name):
    mime_type, _ = mimetypes.guess_type(file_name or "")
    return mime_type or "application/octet-stream"


def process_storage_update(slot, update):
    message = update.get("channel_post") or update.get("message")
    if not message:
        return
    if CONFIG["storage_channel_id"] and str(message.get("chat", {}).get("id")) != CONFIG["storage_channel_id"]:
        return
    item = INDEX.upsert_message(slot, message)
    if item:
        log_event("info", f"{slot['name']} indexed file", {"code": item.get("code"), "file": item.get("file_name")})
        threading.Thread(target=cache_file_path, args=(slot, item.get("code")), daemon=True).start()


def cache_file_path(slot, code):
    item = INDEX.get(code)
    if not item:
        return
    bot_file = item.get("bots", {}).get(str(slot["slot"]), {})
    if not bot_file.get("file_id") or bot_file.get("file_path"):
        return
    try:
        telegram_file = telegram_api(slot, "getFile", {"file_id": bot_file["file_id"]}, timeout=8)
        INDEX.update_bot_file_path(code, slot, telegram_file["file_path"])
    except Exception as exc:
        if is_file_too_big_error(exc):
            return
        log_event("warn", f"{slot['name']} could not pre-cache file path", {"code": code, "reason": str(exc)})


def process_admin_update(slot, update):
    message = update.get("message") or {}
    text = (message.get("text") or "").strip()
    chat_id = message.get("chat", {}).get("id")
    from_id = str(message.get("from", {}).get("id", ""))
    if not text or not chat_id:
        return
    if CONFIG["admin_ids"] and from_id not in CONFIG["admin_ids"]:
        send_admin_message(slot, chat_id, "Access denied.")
        log_event("warn", "Rejected admin command", {"from": from_id})
        return

    command, *parts = text.split()
    command = command.split("@", 1)[0].lower()
    arg = " ".join(parts).strip()
    if command in {"/start", "/help"}:
        send_admin_message(slot, chat_id, admin_help())
    elif command in {"/ping", "/health"}:
        send_admin_message(slot, chat_id, f"Engine online.\n{json.dumps(INDEX.stats(), indent=2)}")
    elif command == "/stats":
        send_admin_message(slot, chat_id, json.dumps(INDEX.stats(), indent=2))
    elif command in {"/streams", "/streamstatus"}:
        send_admin_message(slot, chat_id, json.dumps(large_stream_status(), indent=2))
    elif command == "/warm":
        started = warm_cache()
        send_admin_message(slot, chat_id, f"Started cache warming for {started} bot/file reference(s).")
    elif command == "/parse":
        handle_admin_parse(slot, chat_id, arg)
    elif command == "/find":
        handle_admin_find(slot, chat_id, arg)
    elif command in {"/link", "/directlink"}:
        handle_admin_link(slot, chat_id, arg)
    else:
        send_admin_message(slot, chat_id, "Unknown command. Send /help.")


def handle_admin_parse(slot, chat_id, expression):
    try:
        parsed = parse_post_codes(expression)
        preview = ", ".join(parsed["codes"][:80])
        if len(parsed["codes"]) > 80:
            preview += "\n..."
        send_admin_message(slot, chat_id, f"Parsed {len(parsed['codes'])} code(s):\n{preview}")
    except Exception as exc:
        send_admin_message(slot, chat_id, f"Parse error: {exc}")


def handle_admin_find(slot, chat_id, code):
    if not re.match(r"^\d+$", code or ""):
        send_admin_message(slot, chat_id, "Usage: /find 1234")
        return
    item = INDEX.get(code)
    if not item:
        send_admin_message(slot, chat_id, f"No file indexed for code {code}.")
        return
    send_admin_message(slot, chat_id, f"Code {code}\n{item.get('file_name')}\n{format_bytes(item.get('file_size'))}\nBots: {len(item.get('bots', {}))}")


def handle_admin_link(slot, chat_id, expression):
    try:
        parsed = parse_post_codes(expression)
        send_admin_message(slot, chat_id, build_blogger_link(parsed["expression"], 6 * 60 * 60))
    except Exception as exc:
        send_admin_message(slot, chat_id, f"Could not create link: {exc}")


def send_admin_message(slot, chat_id, text):
    try:
        telegram_api(slot, "sendMessage", {"chat_id": chat_id, "text": text[:3900], "disable_web_page_preview": True}, timeout=10)
    except Exception as exc:
        log_event("error", "Failed to send admin message", {"reason": str(exc)})


def admin_help():
    return "\n".join([
        "ATX Direct Engine",
        "/ping - engine health",
        "/stats - index stats",
        "/streams - active large streams per bot",
        "/warm - pre-cache Telegram file paths",
        "/parse 1234_1235-1240 - test parser",
        "/find 1234 - find indexed file",
        "/link 1234_1235-1240 - create Blogger link",
    ])


def warm_cache():
    started = 0
    for code in INDEX.all_codes():
        item = INDEX.get(code)
        if not item:
            continue
        for slot in DIRECT_SLOTS:
            bot_file = item.get("bots", {}).get(str(slot["slot"]), {})
            if bot_file.get("file_id") and not bot_file.get("file_path"):
                threading.Thread(target=cache_file_path, args=(slot, code), daemon=True).start()
                started += 1
    return started


def start_pollers():
    if not CONFIG["enable_polling"]:
        return
    for slot in DIRECT_SLOTS:
        threading.Thread(target=poll_storage_forever, args=(slot,), daemon=True).start()
    for slot in MAINTENANCE_SLOTS:
        threading.Thread(target=poll_admin_forever, args=(slot,), daemon=True).start()


def setup_webhooks():
    results = []
    if not CONFIG["public_engine_url"]:
        return [{"ok": False, "description": "PUBLIC_ENGINE_URL is empty"}]
    for slot in DIRECT_SLOTS:
        url = f"{CONFIG['public_engine_url']}/telegram/webhook/{slot['slot']}"
        results.append(set_webhook(slot, url, ["message", "channel_post"]))
    for slot in MAINTENANCE_SLOTS:
        url = f"{CONFIG['public_engine_url']}/telegram/admin-webhook/{slot['slot']}"
        results.append(set_webhook(slot, url, ["message"]))
    return results


def start_pyrogram_streamer():
    global PYRO_LOOP
    if not CONFIG["enable_large_stream"]:
        return
    if PyrogramClient is None:
        PYRO_ERRORS["global"] = "Pyrogram is not installed."
        return
    api_id = int(os.getenv("API_ID", "0") or "0")
    api_hash = os.getenv("API_HASH", "")
    if not api_id or not api_hash or not DIRECT_SLOTS:
        PYRO_ERRORS["global"] = "API_ID, API_HASH, and DIRECT_BOT_TOKENS are required for large-file streaming."
        return

    def runner():
        global PYRO_LOOP
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        PYRO_LOOP = loop
        try:
            for slot in DIRECT_SLOTS:
                try:
                    client = PyrogramClient(
                        f"atx_large_file_streamer_{slot['slot']}",
                        api_id=api_id,
                        api_hash=api_hash,
                        bot_token=slot["token"],
                        in_memory=True,
                    )
                    start_result = client.start()
                    if inspect.isawaitable(start_result):
                        loop.run_until_complete(start_result)
                    with PYRO_LOCK:
                        PYRO_CLIENTS[slot["slot"]] = client
                        PYRO_READY_SLOTS.add(slot["slot"])
                        PYRO_ERRORS.pop(slot["slot"], None)
                except Exception as exc:
                    with PYRO_LOCK:
                        PYRO_ERRORS[slot["slot"]] = str(exc)
                    log_event("error", f"{slot['name']} large-file streamer failed to start", {"reason": str(exc)})
            loop.run_forever()
        except Exception as exc:
            PYRO_ERRORS["global"] = str(exc)
            log_event("error", "Large-file streamer loop failed", {"reason": str(exc)})
        finally:
            for client in list(PYRO_CLIENTS.values()):
                try:
                    stop_result = client.stop()
                    if inspect.isawaitable(stop_result):
                        loop.run_until_complete(stop_result)
                except Exception:
                    pass
            loop.close()

    threading.Thread(target=runner, daemon=True).start()


def ensure_pyrogram_ready(slot, timeout=10):
    if not CONFIG["enable_large_stream"]:
        return False
    deadline = time.time() + timeout
    while time.time() < deadline:
        with PYRO_LOCK:
            if slot["slot"] in PYRO_READY_SLOTS and slot["slot"] in PYRO_CLIENTS:
                return True
            reason = PYRO_ERRORS.get(slot["slot"]) or PYRO_ERRORS.get("global")
        if reason:
            log_event("error", f"{slot['name']} large-file streamer is not ready", {"reason": reason})
            return False
        time.sleep(0.2)
    return False


def ensure_any_pyrogram_ready(timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with PYRO_LOCK:
            if PYRO_READY_SLOTS:
                return True
            reason = PYRO_ERRORS.get("global")
        if reason:
            log_event("error", "Large-file import clients are not ready", {"reason": reason})
            return False
        time.sleep(0.2)
    return False


def large_stream_status():
    with PYRO_LOCK:
        bot_status = []
        for slot in DIRECT_SLOTS:
            bot_status.append({
                "slot": slot["slot"],
                "name": slot["name"],
                "ready": slot["slot"] in PYRO_READY_SLOTS and slot["slot"] in PYRO_CLIENTS,
                "activeStreams": PYRO_ACTIVE_STREAMS.get(slot["slot"], 0),
                "maxStreams": CONFIG["max_streams_per_bot"],
                "error": PYRO_ERRORS.get(slot["slot"]),
            })
        return {
            "readySlots": sorted(PYRO_READY_SLOTS),
            "readyBots": len(PYRO_READY_SLOTS),
            "expectedBots": len(DIRECT_SLOTS),
            "activeStreams": dict(PYRO_ACTIVE_STREAMS),
            "bots": bot_status,
            "errors": dict(PYRO_ERRORS),
        }


def telegram_status():
    results = []
    for slot in ALL_SLOTS:
        item = {"bot": slot["name"]}
        for method in ("getMe", "getWebhookInfo"):
            try:
                item[method] = telegram_api(slot, method, {}, timeout=8)
            except Exception as exc:
                item[method] = {"ok": False, "error": str(exc)}
        results.append(item)
    return {
        "engine": INDEX.stats(),
        "enablePolling": CONFIG["enable_polling"],
        "enableWebhooks": CONFIG["enable_webhooks"],
        "publicEngineUrl": CONFIG["public_engine_url"],
        "storageChannelId": CONFIG["storage_channel_id"],
        "logChannelId": CONFIG["log_channel_id"],
        "bots": results,
    }


def set_webhook(slot, url, allowed_updates):
    try:
        result = telegram_api(slot, "setWebhook", {"url": url, "allowed_updates": allowed_updates, "drop_pending_updates": False}, timeout=8)
        return {"bot": slot["name"], "ok": True, "url": url, "result": result}
    except Exception as exc:
        return {"bot": slot["name"], "ok": False, "url": url, "error": str(exc)}


def poll_storage_forever(slot):
    offset_key = f"storage-{slot['slot']}"
    offset = int(read_offsets().get(offset_key, 0))
    while True:
        try:
            updates = telegram_api(slot, "getUpdates", {"offset": offset, "timeout": 25, "allowed_updates": ["message", "channel_post"]}, timeout=35)
            for update in updates:
                offset = int(update["update_id"]) + 1
                write_offset(offset_key, offset)
                process_storage_update(slot, update)
                if not MAINTENANCE_SLOTS:
                    process_admin_update(slot, update)
        except Exception as exc:
            if is_poll_read_timeout(exc):
                time.sleep(1)
                continue
            log_event("error", f"{slot['name']} polling error", {"reason": str(exc)})
            time.sleep(3)


def poll_admin_forever(slot):
    offset_key = f"admin-{slot['slot']}"
    offset = int(read_offsets().get(offset_key, 0))
    while True:
        try:
            updates = telegram_api(slot, "getUpdates", {"offset": offset, "timeout": 25, "allowed_updates": ["message"]}, timeout=35)
            for update in updates:
                offset = int(update["update_id"]) + 1
                write_offset(offset_key, offset)
                process_admin_update(slot, update)
        except Exception as exc:
            if is_poll_read_timeout(exc):
                time.sleep(1)
                continue
            log_event("error", f"{slot['name']} admin polling error", {"reason": str(exc)})
            time.sleep(3)


def read_offsets():
    path = Path(CONFIG["offsets_file"])
    if not path.exists():
        return {}
    with OFFSET_LOCK:
        return json.loads(path.read_text("utf-8"))


def write_offset(key, offset):
    path = Path(CONFIG["offsets_file"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with OFFSET_LOCK:
        offsets = read_offsets()
        offsets[str(key)] = offset
        path.write_text(json.dumps(offsets, indent=2, sort_keys=True), "utf-8")


def is_poll_read_timeout(exc):
    return isinstance(exc, requests.exceptions.ReadTimeout) or "Read timed out" in str(exc)


def is_file_too_big_error(reason):
    return "file is too big" in str(reason).lower()


def large_file_fallback(code):
    if not CONFIG["large_file_base_url"]:
        return None
    return redirect(f"{CONFIG['large_file_base_url']}/download/{quote(str(code))}", code=302)


def extract_media(message):
    for key in ("document", "video", "animation", "audio"):
        media = message.get(key)
        if media and media.get("file_id"):
            return media
    return None


def find_explicit_code(caption):
    for pattern in (r"\bpost\s*code\s*[:#-]?\s*(\d{1,12})\b", r"\bcode\s*[:#-]?\s*(\d{1,12})\b", r"#(\d{1,12})\b"):
        match = re.search(pattern, caption or "", re.I)
        if match:
            return match.group(1)
    return ""


def caption_name(caption):
    first = next((line.strip() for line in (caption or "").splitlines() if line.strip()), "")
    return re.sub(r"\b(post\s*)?code\s*[:#-]?\s*\d+\b|#\d+\b", "", first, flags=re.I).strip()[:180]


def log_event(level, text, details=None):
    if not CONFIG["log_channel_id"] or not ALL_SLOTS:
        return
    details = details or {}
    lines = [f"[HF Direct Engine] {level.upper()} {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}", text]
    lines += [f"{key}: {value}" for key, value in details.items() if value not in (None, "")]
    try:
        slot = MAINTENANCE_SLOTS[0] if MAINTENANCE_SLOTS else ALL_SLOTS[0]
        telegram_api(slot, "sendMessage", {"chat_id": CONFIG["log_channel_id"], "text": "\n".join(lines)[:3900], "disable_web_page_preview": True}, timeout=8)
    except Exception:
        pass


def format_bytes(value):
    size = float(value or 0)
    if size <= 0:
        return ""
    units = ["B", "KB", "MB", "GB", "TB"]
    unit = 0
    while size >= 1024 and unit < len(units) - 1:
        size /= 1024
        unit += 1
    return f"{size:.0f} {units[unit]}" if size >= 10 or unit == 0 else f"{size:.1f} {units[unit]}"


def safe_filename(value):
    return re.sub(r'[\r\n"\\]', "_", value or "download.bin")


def error(code, message, status=400, details=None):
    return jsonify({"ok": False, "error": {"code": code, "message": message, "details": details or {}}}), status


start_pyrogram_streamer()
if CONFIG["enable_webhooks"]:
    threading.Thread(target=setup_webhooks, daemon=True).start()
start_pollers()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=CONFIG["port"], threaded=True)
