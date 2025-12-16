#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
app_alarm2ding.py  (history + channel gating + cleanup + deletes + reconcile)
-----------------------------------------------------------------------------
- 接收 AI 盒子告警 -> 去重 -> 本地落图 -> （按设备&通道开关 + 定时）转发钉钉
- 历史：/login -> /history 查询、筛选、导出 CSV、批量删除、按筛选条件删除全部（含“删记录尽量删图”）
- 设备：/devices 展示“通道列表”，逐条启/停；/devices/edit 配置周一~周日 + 多时间段 + webhook 绑定
- 存储：SQLite（./alarm2ding.db）+（可选）WAL/BusyTimeout 提升并发稳定性
- 出图：固定直链 http://<公网IP>:<port>/static/snaps/YYYYMMDD/<hash>.jpg （若未配置 IMAGE_PUBLIC_BASE，则仍保存相对 URL）
- 清理：每天定时清理旧日目录与容量兜底；并做“DB↔图片对账修复”（删坏记录 / 删孤儿图）
- 安全：可选 AUTH_TOKEN（/ai/message 鉴权）

依赖：
    pip install flask requests paho-mqtt python-dotenv
"""

from __future__ import annotations
import os, time, json, base64, hashlib, argparse, logging, sqlite3, shutil, re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, List
from markupsafe import Markup, escape
from urllib.parse import urlparse

from werkzeug.security import generate_password_hash, check_password_hash
from functools import lru_cache

from flask import (
    Flask, request, jsonify, redirect, url_for, session,
    render_template_string, make_response, abort
)
from ding_webhook import DingRobot, DingRobotError

# ---------------- Env & Logging ----------------
def _load_env():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
_load_env()

LOG_VERBOSE = os.getenv("LOG_VERBOSE", "0") == "1"
logging.basicConfig(
    level=logging.DEBUG if LOG_VERBOSE else logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
LOG = logging.getLogger("alarm2ding")

APP = Flask(__name__, static_folder="static", static_url_path="/static")
APP.secret_key = os.getenv("SECRET_KEY", "ABCDEFGHIJKLMN")

# ---------------- URL helpers ----------------
_SNAPS_RE = re.compile(r"/snaps/(\d{8})/([^/?#]+)$")

def _preview_url_for_img(img_url: str) -> Optional[str]:
    """把 http(s)://.../snaps/<day>/<file>.jpg 或 /static/snaps/... 转成 /view/<day>/<file> 的预览页链接"""
    if not img_url:
        return None
    try:
        p = urlparse(img_url)
        parts = [x for x in p.path.split("/") if x]
        if "snaps" in parts:
            i = parts.index("snaps")
            day = parts[i+1]
            fname = parts[i+2]
            base = f"{p.scheme}://{p.netloc}" if (p.scheme and p.netloc) else (IMAGE_PUBLIC_BASE or "")
            return (base.rstrip("/") + f"/view/{day}/{fname}")
    except Exception:
        pass
    return None

# 让模板里也能直接用 preview_from_url(...)
APP.jinja_env.globals["preview_from_url"] = _preview_url_for_img

def _topbar(brand: str, nav: List[Dict[str, Any]]) -> Markup:
    """
    nav: [{"label": "...", "href": "...", "active": bool}, ...]
    """
    out = []
    out.append('<div class="topbar"><div class="topbar-inner">')
    out.append(f'<div class="brand"><span class="dot"></span><span>{escape(brand)}</span></div>')
    out.append('<div class="nav">')
    for it in (nav or []):
        label = escape(str(it.get("label", "")))
        href  = escape(str(it.get("href", "#")))
        cls   = "active" if it.get("active") else ""
        out.append(f'<a href="{href}" class="{cls}">{label}</a>')
    out.append('</div></div></div>')
    return Markup("".join(out))

APP.jinja_env.globals["topbar"] = _topbar

# ---------------- Unified UI Theme & Header ----------------
THEME_CSS = r"""
:root{
  --bg:#f6f8fb; --card:#ffffff; --text:#1f2937; --muted:#6b7280; --line:#e5e7eb;
  --primary:#2563eb; --primary-600:#1d4ed8; --primary-50:#eff6ff;
  --ok:#16a34a; --ok-50:#e8f7ee; --warn:#d97706; --warn-50:#fff7ed; --err:#dc2626; --err-50:#fde8e8;
}
@media (prefers-color-scheme: dark){
  :root{
    --bg:#0b1220; --card:#0f172a; --text:#e5e7eb; --muted:#94a3b8; --line:#1f2937;
    --primary:#60a5fa; --primary-600:#3b82f6; --primary-50:#0b1220;
    --ok:#22c55e; --ok-50:#052e1b; --warn:#f59e0b; --warn-50:#2a1d04; --err:#f87171; --err-50:#2a0b0b;
  }
}

/* 基础 */
*{box-sizing:border-box}
html,body{height:100%}
body{
  margin:0;background:var(--bg);color:var(--text);
  font:14px/1.55 -apple-system,Segoe UI,Roboto,Helvetica,Arial;
  -webkit-text-size-adjust:100%;
}
a{color:var(--primary);text-decoration:none} a:hover{text-decoration:underline}
img{max-width:100%;height:auto;display:block}

/* 容器与卡片 */
.container{max-width:1180px;margin:3vh auto;padding:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;box-shadow:0 1px 2px rgba(0,0,0,.04);padding:18px}

/* 顶栏 */
.topbar{position:sticky;top:0;z-index:50;background:var(--bg);border-bottom:1px solid var(--line)}
.topbar-inner{max-width:1180px;margin:0 auto;min-height:var(--topbar-h);display:flex;align-items:center;justify-content:space-between;padding:12px 16px}
.brand{display:flex;align-items:center;gap:10px;font-weight:700}
.brand .dot{width:10px;height:10px;border-radius:50%;background:var(--primary)}
.nav{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
.nav a{padding:6px 10px;border-radius:8px}
.nav a.active{background:var(--primary-50);text-decoration:none}

/* 标题 */
h1,h2,h3{margin:8px 0 14px}

/* 表单/按钮（默认全响应） */
.inp, select{padding:8px;border:1px solid var(--line);background:var(--card);color:var(--text);border-radius:8px;min-height:36px}
.inp:focus, select:focus{outline:2px solid var(--primary-600);outline-offset:1px}
.btn{
  display:inline-flex;align-items:center;gap:6px;
  padding:8px 12px;border:1px solid var(--line);background:var(--card);
  color:var(--text);border-radius:10px;cursor:pointer;white-space:nowrap
}
.btn:hover{border-color:#cfd4dc}
.btn-primary{background:var(--primary);border-color:var(--primary);color:#fff}
.btn-primary:hover{background:var(--primary-600);border-color:var(--primary-600)}
.btn-danger{background:var(--err);border-color:var(--err);color:#fff}
.btn-ghost{background:transparent}

/* 徽章 */
.badge{display:inline-block;padding:3px 8px;border-radius:999px;font-size:12px}
.badge-ok{background:var(--ok-50);color:var(--ok)}
.badge-err{background:var(--err-50);color:var(--err)}
.badge-warn{background:var(--warn-50);color:var(--warn)}
.muted{color:var(--muted)}
.small{font-size:12px}

/* 栅格：自动响应（表单容器统一用 .form-grid 或页面里 class="filter"） */
.form-grid,
form.filter{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
  gap:8px;
}
.form-grid .inp, .form-grid select, form.filter .inp, form.filter select{width:100%}

/* 表格：桌面为常规表格，小屏自动横向滚动；表头吸顶 */
.table{width:100%;border-collapse:separate;border-spacing:0}
.table thead th{position:sticky;top:var(--topbar-h);background:var(--card);z-index:1}
.table th,.table td{border-bottom:1px solid var(--line);padding:10px 12px;text-align:left;vertical-align:top}

/* 小屏优化 */
@media (max-width: 860px){
  .topbar-inner{padding:10px 12px}
  .container{padding:10px}
  .nav{gap:8px}
  .btn{padding:8px 10px}
  .table{display:block;overflow:auto;-webkit-overflow-scrolling:touch}
  .toolbar, .ops{flex-wrap:wrap}
}
@media (max-width: 860px){
  .table.cardify{border:0;display:block;overflow:visible}
  .table.cardify thead{display:none}
  .table.cardify tbody{display:block}
  .table.cardify tr{display:block;background:var(--card);border:1px solid var(--line);border-radius:12px;margin:10px 0;padding:4px}
  .table.cardify td{display:flex;gap:10px;justify-content:space-between;border:none;padding:8px 10px}
  .table.cardify td::before{
    content: attr(data-label);
    font-weight:600;color:var(--muted);flex:0 0 42%;
  }
  .table.cardify td > *{max-width:58%;word-break:break-all;text-align:right}
}
"""

# 可选：把各页面内联的“基础样式”去掉，避免和主题冲突（用 env 控制）
STRIP_PAGE_BASE_CSS = os.getenv("STRIP_PAGE_BASE_CSS", "1") == "1"
_BASE_SELECTORS = (":root", "body{", ".container", ".card", ".btn", ".table")

def _inject_viewport_meta(html: str) -> str:
    if re.search(r'<meta\s+name=["\']viewport["\']', html, flags=re.I):
        return html
    tag = '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
    if re.search(r"</head>", html, flags=re.I):
        return re.sub(r"</head>", tag + "</head>", html, count=1, flags=re.I)
    if re.search(r"<title[^>]*>", html, flags=re.I):
        return re.sub(r"(<title[^>]*>)", tag + r"\1", html, count=1, flags=re.I)
    return tag + html

def _strip_conflicting_css(html: str) -> str:
    if not STRIP_PAGE_BASE_CSS:
        return html

    def _repl(m):
        attrs = m.group(1) or ""
        css   = m.group(2) or ""
        if "data-keep" in attrs:
            return m.group(0)
        if any(sel in css for sel in _BASE_SELECTORS):
            return ""
        return m.group(0)

    return re.sub(r"<style([^>]*)>(.*?)</style>", _repl, html, flags=re.I | re.S)

def _inject_theme_css(html: str) -> str:
    if 'id="app-theme"' in html:
        return html
    block = f'\n<style id="app-theme">{THEME_CSS}</style>\n'
    if re.search(r"</head>", html, flags=re.I):
        return re.sub(r"</head>", block + "</head>", html, count=1, flags=re.I)
    if re.search(r"</body>", html, flags=re.I):
        return re.sub(r"</body>", block + "</body>", html, count=1, flags=re.I)
    return html + block

@APP.after_request
def _after_inject_theme(resp):
    try:
        ct = (resp.headers.get("Content-Type") or "").lower()
        if "text/html" in ct and not resp.direct_passthrough:
            body = resp.get_data(as_text=True)
            body = _inject_viewport_meta(body)
            if not (request and request.path.startswith("/view/")):
                body = _strip_conflicting_css(body)
            body = _inject_theme_css(body)
            resp.set_data(body)
    except Exception as e:
        LOG.debug("theme inject fail: %s", e)
    return resp

# ---------------- Runtime Config ----------------
APP_NAME     = os.getenv("APP_NAME", "algo-edge")
DEDUP_WINDOW = float(os.getenv("DEDUP_WINDOW", "10"))

# 钉钉
ROBOT = DingRobot(
    access_token=os.getenv("DING_ACCESS_TOKEN", "").strip(),
    secret=os.getenv("DING_SECRET", "").strip(),
    timeout=8.0,
)

def _csv_env(name: str):
    raw = os.getenv(name, "")
    raw = raw.split("#", 1)[0]
    return [x.strip() for x in raw.split(",") if x.strip()]

AT_USER_IDS = _csv_env("AT_USER_IDS")
AT_MOBILES  = _csv_env("AT_MOBILES")

# 出图（固定直链）
IMAGE_PUBLIC_BASE = os.getenv("IMAGE_PUBLIC_BASE", "").rstrip("/")

HIDE_RTSP  = os.getenv("HIDE_RTSP", "0") == "1"
VISIBLE_AT = os.getenv("VISIBLE_AT", "0") == "1"

# 登录
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")

# 默认开关（首次出现时）
# - 设备：默认允许
# - 通道：默认不转发
DEVICE_FORWARD_DEFAULT  = 1 if os.getenv("DEVICE_FORWARD_DEFAULT", "1") == "1" else 0
CHANNEL_FORWARD_DEFAULT = 1 if os.getenv("CHANNEL_FORWARD_DEFAULT", "0") == "1" else 0

# 可选鉴权
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "").strip()

# MQTT（如需）
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "").strip()
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC       = os.getenv("MQTT_TOPIC", "xinhuoaie-event/#")

# 自动清理（图片）
SNAP_RETAIN_DAYS = int(os.getenv("SNAP_RETAIN_DAYS", "30"))  # 0=不按天清理
SNAP_MAX_GB      = float(os.getenv("SNAP_MAX_GB", "0"))      # 0=不设容量上限
CLEAN_AT         = os.getenv("CLEAN_AT", "03:10")            # 每日 HH:MM

# ---- DB 轮巡清理（防止 alarm2ding.db 无限增大）----
DB_RETAIN_DAYS = int(os.getenv("DB_RETAIN_DAYS", str(SNAP_RETAIN_DAYS)))  # 默认跟随 snaps
DB_MAX_ROWS    = int(os.getenv("DB_MAX_ROWS", "0"))
DB_SWEEP_SEC   = int(os.getenv("DB_SWEEP_SEC", "60"))
DB_VACUUM      = os.getenv("DB_VACUUM", "1") == "1"

# ---- 对账修复：DB↔图片一致性 ----
RECONCILE_DAILY = os.getenv("RECONCILE_DAILY", "1") == "1"
BROKEN_REF_POLICY = os.getenv("BROKEN_REF_POLICY", "delete_record")  # delete_record | clear_url
ORPHAN_FILE_POLICY = os.getenv("ORPHAN_FILE_POLICY", "delete_file")  # delete_file | keep
RECONCILE_MAX_URLS = int(os.getenv("RECONCILE_MAX_URLS", "200000"))   # 0=不限制（谨慎）

# SQLite 稳定性（WAL + busy_timeout）
SQLITE_WAL = os.getenv("SQLITE_WAL", "1") == "1"
SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000"))

# 运行目录 & 数据库
DATA_DIR = Path(".").resolve()
DB_PATH  = DATA_DIR / "alarm2ding.db"

_recent_keys: Dict[str, float] = {}
_db_sweep_last: float = 0.0

# 算法映射（可按需扩充）
ALGO_MAP = {
    11: "禁区闯入", 12: "翻越围栏", 13: "安全帽", 14: "反光衣", 15: "打电话",
    16: "睡岗", 18: "奔跑", 19: "跌倒", 21: "人员聚集", 30: "人员滞留",
    31: "动态人流统计", 36: "车辆违停", 49: "驾驶室手势", 1015: "疲劳检测",
    1021: "玩手机", 1025: "行人闯红灯", 1062: "未佩戴口罩", 11000: "人形检测",
    12000: "人脸检测", 1210: "人脸识别（含人体属性）", 2001: "火", 2002: "烟",
    20500: "画面监测", 2060: "抛物监测", 20700: "动物监测", 2080: "地面状态",
    2090: "市容监测", 3001: "仅检测车辆", 3002: "车牌识别(非必检)", 3011: "车辆违停",
}

# ---------------- Utils ----------------
def _prune_recent_keys(now: float, ttl: float):
    if not hasattr(_prune_recent_keys, "_cnt"):
        _prune_recent_keys._cnt = 0
    _prune_recent_keys._cnt += 1
    if _prune_recent_keys._cnt % 200 != 0 and len(_recent_keys) < 10000:
        return
    dead = [k for k, t in list(_recent_keys.items()) if now - t > max(ttl*2, 30)]
    for k in dead:
        _recent_keys.pop(k, None)

def _safe_str(d: Dict[str, Any], key: str, default: str = "") -> str:
    v = d.get(key, default)
    return str(v) if v is not None else default

def _safe_int(d: Dict[str, Any], key: str, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(d[key])
    except Exception:
        return default

def _parse_time(s: str) -> str:
    if not s:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        if s.isdigit() and len(s) in (10, 13):
            ts = int(s) / (1000 if len(s) == 13 else 1)
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return s

def _event_day(payload: Dict[str, Any]) -> str:
    """图片目录 day：优先用 signTime 对齐历史记录；避免 now() 导致错位"""
    st = _parse_time(_safe_str(payload, "signTime"))
    if len(st) >= 10 and st[4] == "-" and st[7] == "-":
        return st[:10].replace("-", "")
    return datetime.now().strftime("%Y%m%d")

def _dedup_key(payload: Dict[str, Any]) -> str:
    dev   = _safe_str(payload, "deviceId") or _safe_str(payload, "GBID") or _safe_str(payload, "indexCode")
    t     = _safe_int(payload, "type", -1)
    track = _safe_int(payload, "trackId", -1)
    st    = _parse_time(_safe_str(payload, "signTime"))
    st_sec = st.split(".")[0]
    raw   = f"{dev}|{t}|{track}|{st_sec}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def _algo_name(type_id: Optional[int], type_name: str) -> str:
    if type_name:
        return f"{type_name}({type_id})"
    if type_id in ALGO_MAP:
        return f"{ALGO_MAP[type_id]}({type_id})"
    return f"未知({type_id})"

def _pos_key(payload: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    device_id   = _safe_str(payload, "deviceId") or "-"
    device_name = _safe_str(payload, "deviceName")
    box_name    = _safe_str(payload, "boxName")
    idx         = _safe_str(payload, "indexCode")
    gbid        = _safe_str(payload, "GBID")
    channel_key = idx or gbid or device_name or "-"
    channel_name= device_name or idx or gbid or "-"
    return device_id, channel_key, channel_name, box_name, (idx or gbid)

def _in_time_window(now_hhmm: str, start_hhmm: Optional[str], end_hhmm: Optional[str]) -> bool:
    if not start_hhmm or not end_hhmm:
        return True
    try:
        nh = int(now_hhmm[:2]); nm = int(now_hhmm[3:5]); n = nh*60 + nm
        sh = int(start_hhmm[:2]); sm = int(start_hhmm[3:5]); s = sh*60 + sm
        eh = int(end_hhmm[:2]);   em = int(end_hhmm[3:5]);   e = eh*60 + em
        if s == e:
            return True
        if s < e:
            return s <= n < e
        else:
            return n >= s or n < e
    except Exception:
        return True

# ---- snaps <-> url/path helpers ----
def _snap_rel_from_url(img_url: str) -> Optional[str]:
    """从 image_url 提取 'snaps/YYYYMMDD/xxx.jpg' 相对路径"""
    if not img_url:
        return None
    try:
        p = urlparse(img_url)
        m = _SNAPS_RE.search(p.path)
        if not m:
            return None
        day, fname = m.group(1), m.group(2)
        return f"snaps/{day}/{fname}"
    except Exception:
        return None

def _snap_local_path_from_rel(rel: str) -> Path:
    return Path(APP.static_folder) / rel

def _db_count_refs_for_rel(rel: str) -> int:
    pat = "%" + "/" + rel.replace("\\", "/")
    conn = _db()
    try:
        r = conn.execute("SELECT COUNT(1) AS c FROM messages WHERE image_url LIKE ?", (pat,)).fetchone()
        return int(r["c"]) if r else 0
    finally:
        conn.close()

def _delete_db_rows_by_rel(rel: str) -> int:
    if not rel:
        return 0
    pat = "%" + "/" + rel.replace("\\", "/")
    conn = _db()
    try:
        cur = conn.execute("DELETE FROM messages WHERE image_url LIKE ?", (pat,))
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()

def _delete_snap_if_orphan(rel: str):
    """当 DB 不再引用该图片时，删除本地文件（以及空目录）"""
    if not rel:
        return
    try:
        if _db_count_refs_for_rel(rel) > 0:
            return
        p = _snap_local_path_from_rel(rel)
        if p.exists():
            p.unlink()
        parent = p.parent
        if parent.exists() and parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
    except Exception as e:
        LOG.warning("snap rm fail: %s (%s)", rel, e)

def _fetch_rels_by_ids(ids: List[int]) -> List[str]:
    if not ids:
        return []
    qmarks = ",".join("?" for _ in ids)
    conn = _db()
    try:
        rows = conn.execute(f"SELECT image_url FROM messages WHERE id IN ({qmarks})", ids).fetchall()
        rels = []
        for r in rows:
            rel = _snap_rel_from_url(r["image_url"] or "")
            if rel:
                rels.append(rel)
        return rels
    finally:
        conn.close()

# ---- 图片处理（base64 -> 本地落盘 -> URL） ----
def _save_base64_then_public(payload: Dict[str, Any]) -> Optional[str]:
    b64_fields = ["signBigAvatarBase64", "signBigAvatar", "signAvatar"]
    b64 = None
    which = None
    for k in b64_fields:
        if payload.get(k):
            b64 = payload[k]; which = k; break
    if not b64:
        LOG.info("b64: no base64 field -> skip")
        return None
    try:
        if "," in b64 and b64.strip().lower().startswith("data:"):
            b64 = b64.split(",", 1)[1]
        b64 = re.sub(r"\s+", "", b64)  # 去换行空格
        b64 += "=" * ((4 - len(b64) % 4) % 4)  # 补齐 padding
        blob = base64.b64decode(b64, validate=False)

        day  = _event_day(payload)  # ★ 用 signTime 对齐目录
        out_dir = Path(APP.static_folder) / "snaps" / day
        out_dir.mkdir(parents=True, exist_ok=True)

        h = hashlib.md5(blob).hexdigest()[:16]
        out_path = out_dir / f"{h}.jpg"

        if not out_path.exists():
            out_path.write_bytes(blob)
            LOG.info("b64: saved (%s) -> %s", which, out_path)

        # ★ 最稳：IMAGE_PUBLIC_BASE 不设也给一个相对 URL，保证 DB↔文件可对账
        if IMAGE_PUBLIC_BASE:
            url = f"{IMAGE_PUBLIC_BASE}/snaps/{day}/{h}.jpg"
        else:
            url = f"/static/snaps/{day}/{h}.jpg"
        return url
    except Exception as e:
        LOG.warning("b64: decode fail: %s", e)
        return None

def _resolve_image_url(payload: Dict[str, Any]) -> Optional[str]:
    return _save_base64_then_public(payload)

# ---------------- SQLite DAO ----------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
  device_id TEXT PRIMARY KEY,
  alias     TEXT,
  enabled   INTEGER NOT NULL DEFAULT 1,
  first_seen TEXT,
  last_seen  TEXT,
  cnt        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS channels (
  device_id    TEXT NOT NULL,
  channel_key  TEXT NOT NULL,
  channel_name TEXT,
  box_name     TEXT,
  index_or_gbid TEXT,
  enabled      INTEGER NOT NULL DEFAULT 1,
  first_seen   TEXT,
  last_seen    TEXT,
  cnt          INTEGER NOT NULL DEFAULT 0,
  rule_mask    INTEGER NOT NULL DEFAULT 0,
  rule_start   TEXT,
  rule_end     TEXT,
  PRIMARY KEY(device_id, channel_key)
);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT,
  device_id TEXT,
  channel_key TEXT,
  channel_name TEXT,
  type INTEGER,
  type_name TEXT,
  box_name TEXT,
  device_name TEXT,
  score TEXT,
  image_url TEXT,
  forwarded INTEGER NOT NULL DEFAULT 0,
  forward_reason TEXT,
  dedup_key TEXT,
  raw_json  TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
CREATE INDEX IF NOT EXISTS idx_messages_device ON messages(device_id);
CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(device_id, channel_key);

-- 多时间段规则（通道 × 星期 × 多段）
CREATE TABLE IF NOT EXISTS channel_rules (
  device_id   TEXT NOT NULL,
  channel_key TEXT NOT NULL,
  weekday     INTEGER NOT NULL,      -- 0=周一 ... 6=周日
  seg_idx     INTEGER NOT NULL,      -- 段序号：0,1,2...
  start_hhmm  TEXT NOT NULL,         -- 'HH:MM'
  end_hhmm    TEXT NOT NULL,         -- 'HH:MM'
  PRIMARY KEY (device_id, channel_key, weekday, seg_idx)
);
CREATE INDEX IF NOT EXISTS idx_rules_key_day ON channel_rules(device_id, channel_key, weekday);

-- 用户与权限
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  active   INTEGER NOT NULL DEFAULT 1,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS user_channels (
  user_id INTEGER NOT NULL,
  device_id TEXT NOT NULL,
  channel_key TEXT NOT NULL,
  PRIMARY KEY (user_id, device_id, channel_key)
);

-- 多 Webhook 与路由
CREATE TABLE IF NOT EXISTS webhooks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  access_token TEXT NOT NULL,
  secret TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  is_default INTEGER NOT NULL DEFAULT 0,
  created_at TEXT
);

-- 通道 → 多个 webhook 绑定
CREATE TABLE IF NOT EXISTS channel_webhooks (
  device_id TEXT NOT NULL,
  channel_key TEXT NOT NULL,
  webhook_id INTEGER NOT NULL,
  PRIMARY KEY (device_id, channel_key, webhook_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_dedup ON messages(dedup_key);
"""

def _db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        if SQLITE_WAL:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return conn

def init_db():
    conn = _db()
    try:
        conn.executescript(SCHEMA); conn.commit()
    finally:
        conn.close()

def ensure_migrations():
    conn = _db()
    try:
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN forward_reason TEXT")
        except Exception:
            pass
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username TEXT UNIQUE NOT NULL,
          password_hash TEXT NOT NULL,
          is_admin INTEGER NOT NULL DEFAULT 0,
          active   INTEGER NOT NULL DEFAULT 1,
          created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS user_channels (
          user_id INTEGER NOT NULL,
          device_id TEXT NOT NULL,
          channel_key TEXT NOT NULL,
          PRIMARY KEY (user_id, device_id, channel_key)
        );
        CREATE TABLE IF NOT EXISTS webhooks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          access_token TEXT NOT NULL,
          secret TEXT,
          enabled INTEGER NOT NULL DEFAULT 1,
          is_default INTEGER NOT NULL DEFAULT 0,
          created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS channel_webhooks (
          device_id TEXT NOT NULL,
          channel_key TEXT NOT NULL,
          webhook_id INTEGER NOT NULL,
          PRIMARY KEY (device_id, channel_key, webhook_id)
        );
        """)
        conn.commit()
    finally:
        conn.close()
    _bootstrap_admin_if_absent()

def upsert_device(device_id: str, seen_ts: str) -> int:
    conn = _db()
    try:
        row = conn.execute("SELECT enabled, cnt FROM devices WHERE device_id=?", (device_id,)).fetchone()
        if row:
            enabled = int(row["enabled"])
            cnt = int(row["cnt"]) + 1
            conn.execute("UPDATE devices SET last_seen=?, cnt=? WHERE device_id=?", (seen_ts, cnt, device_id))
            conn.commit()
            return enabled
        else:
            conn.execute("INSERT INTO devices(device_id, enabled, first_seen, last_seen, cnt) VALUES(?,?,?,?,?)",
                         (device_id, DEVICE_FORWARD_DEFAULT, seen_ts, seen_ts, 1))
            conn.commit()
            return DEVICE_FORWARD_DEFAULT
    finally:
        conn.close()

def upsert_channel(device_id: str, channel_key: str, channel_name: str,
                   box_name: str, index_or_gbid: str, seen_ts: str) -> Tuple[int, int, Optional[str], Optional[str]]:
    conn = _db()
    try:
        row = conn.execute(
            "SELECT enabled, cnt, rule_mask, rule_start, rule_end FROM channels WHERE device_id=? AND channel_key=?",
            (device_id, channel_key)
        ).fetchone()
        if row:
            enabled = int(row["enabled"])
            cnt = int(row["cnt"]) + 1
            conn.execute("""UPDATE channels SET last_seen=?, cnt=?, channel_name=?, box_name=?, index_or_gbid=?
                            WHERE device_id=? AND channel_key=?""",
                         (seen_ts, cnt, channel_name, box_name, index_or_gbid, device_id, channel_key))
            conn.commit()
            return enabled, int(row["rule_mask"]), row["rule_start"], row["rule_end"]
        else:
            conn.execute("""INSERT INTO channels(device_id, channel_key, channel_name, box_name, index_or_gbid,
                             enabled, first_seen, last_seen, cnt, rule_mask, rule_start, rule_end)
                             VALUES(?,?,?,?,?,?,?, ?, ?, 0, NULL, NULL)""",
                         (device_id, channel_key, channel_name, box_name, index_or_gbid,
                          CHANNEL_FORWARD_DEFAULT, seen_ts, seen_ts, 1))
            conn.commit()
            return CHANNEL_FORWARD_DEFAULT, 0, None, None
    finally:
        conn.close()

def set_channel_enabled(device_id: str, channel_key: str, enabled: int):
    conn = _db()
    try:
        conn.execute("UPDATE channels SET enabled=? WHERE device_id=? AND channel_key=?",
                     (1 if enabled else 0, device_id, channel_key))
        conn.commit()
    finally:
        conn.close()

def list_channels(device_filter: str = "") -> List[sqlite3.Row]:
    conn = _db()
    try:
        if device_filter:
            return conn.execute(
                "SELECT * FROM channels WHERE device_id=? ORDER BY last_seen DESC",
                (device_filter,)
            ).fetchall()
        return conn.execute("SELECT * FROM channels ORDER BY last_seen DESC").fetchall()
    finally:
        conn.close()

def insert_message(rec: Dict[str, Any]):
    conn = _db()
    try:
        conn.execute("""INSERT OR IGNORE INTO messages
        (ts, device_id, channel_key, channel_name, type, type_name, box_name, device_name,
         score, image_url, forwarded, forward_reason, dedup_key, raw_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (rec["ts"], rec["device_id"], rec["channel_key"], rec["channel_name"], rec["type"],
         rec["type_name"], rec["box_name"], rec["device_name"], rec["score"], rec["image_url"],
         1 if rec["forwarded"] else 0, rec.get("forward_reason",""), rec["dedup_key"], rec["raw_json"]))
        conn.commit()
    finally:
        conn.close()

def query_messages(filters: Dict[str, Any], limit: int, offset: int) -> Tuple[List[sqlite3.Row], int]:
    wh, args = [], []
    if filters.get("device_id"): wh.append("device_id = ?"); args.append(filters["device_id"])
    if filters.get("channel_key"): wh.append("channel_key = ?"); args.append(filters["channel_key"])
    if filters.get("type") is not None and filters["type"] != "": wh.append("type = ?"); args.append(int(filters["type"]))
    if filters.get("forwarded") in ("0","1"): wh.append("forwarded = ?"); args.append(int(filters["forwarded"]))
    if filters.get("from"):      wh.append("ts >= ?"); args.append(filters["from"])
    if filters.get("to"):        wh.append("ts <= ?"); args.append(filters["to"])

    if filters.get("visible_uid") is not None:
        wh.append("""EXISTS (
            SELECT 1 FROM user_channels uc
            WHERE uc.user_id=?
              AND uc.device_id = messages.device_id
              AND uc.channel_key = messages.channel_key
        )""")
        args.append(int(filters["visible_uid"]))

    where = ("WHERE " + " AND ".join(wh)) if wh else ""
    conn = _db()
    try:
        total = conn.execute(f"SELECT COUNT(1) AS c FROM messages {where}", args).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM messages {where} ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?",
            args + [limit, offset]
        ).fetchall()
        return rows, total
    finally:
        conn.close()

def delete_messages_by_ids(ids: List[int]) -> int:
    if not ids:
        return 0
    qmarks = ",".join("?" for _ in ids)
    conn = _db()
    try:
        cur = conn.execute(f"DELETE FROM messages WHERE id IN ({qmarks})", ids)
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()

def delete_messages_by_filters(filters: Dict[str, Any]) -> int:
    wh, args = [], []
    if filters.get("device_id"): wh.append("device_id = ?"); args.append(filters["device_id"])
    if filters.get("channel_key"): wh.append("channel_key = ?"); args.append(filters["channel_key"])
    if filters.get("type") is not None and filters["type"] != "": wh.append("type = ?"); args.append(int(filters["type"]))
    if filters.get("forwarded") in ("0","1"): wh.append("forwarded = ?"); args.append(int(filters["forwarded"]))
    if filters.get("from"): wh.append("ts >= ?"); args.append(filters["from"])
    if filters.get("to"):   wh.append("ts <= ?"); args.append(filters["to"])
    if filters.get("visible_uid") is not None:
        wh.append("""EXISTS (
            SELECT 1 FROM user_channels uc
            WHERE uc.user_id=?
              AND uc.device_id = messages.device_id
              AND uc.channel_key = messages.channel_key
        )""")
        args.append(int(filters["visible_uid"]))
    where = ("WHERE " + " AND ".join(wh)) if wh else ""
    conn = _db()
    try:
        cur = conn.execute(f"DELETE FROM messages {where}", args)
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()

def channel_has_any_rules(device_id: str, channel_key: str) -> bool:
    conn = _db()
    try:
        r = conn.execute(
            "SELECT 1 FROM channel_rules WHERE device_id=? AND channel_key=? LIMIT 1",
            (device_id, channel_key)
        ).fetchone()
        return r is not None
    finally:
        conn.close()

def channel_rules_for_weekday(device_id: str, channel_key: str, weekday: int) -> List[Tuple[str,str]]:
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT start_hhmm, end_hhmm FROM channel_rules "
            "WHERE device_id=? AND channel_key=? AND weekday=? ORDER BY seg_idx ASC",
            (device_id, channel_key, int(weekday))
        ).fetchall()
        return [(r["start_hhmm"], r["end_hhmm"]) for r in rows]
    finally:
        conn.close()

def replace_channel_rules_for_day(device_id: str, channel_key: str, weekday: int,
                                  segments: List[Tuple[str,str]]):
    conn = _db()
    try:
        conn.execute("DELETE FROM channel_rules WHERE device_id=? AND channel_key=? AND weekday=?",
                     (device_id, channel_key, int(weekday)))
        for i, (s,e) in enumerate(segments):
            conn.execute(
                "INSERT INTO channel_rules(device_id, channel_key, weekday, seg_idx, start_hhmm, end_hhmm) "
                "VALUES(?,?,?,?,?,?)",
                (device_id, channel_key, int(weekday), i, s, e)
            )
        conn.commit()
    finally:
        conn.close()

def summarize_rules_short(device_id: str, channel_key: str) -> str:
    labels = "一二三四五六日"
    has_any = channel_has_any_rules(device_id, channel_key)
    if not has_any:
        return "未配置"
    parts = []
    for d in range(7):
        segs = channel_rules_for_weekday(device_id, channel_key, d)
        if not segs:
            seg_txt = "-"
        else:
            if any(s == e for s, e in segs):
                seg_txt = "全天"
            else:
                seg_txt = ",".join([f"{s}-{e}" for s, e in segs])
        parts.append(f"{labels[d]}:{seg_txt}")
    return " ".join(parts)

def migrate_legacy_channel_rules_once():
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT device_id, channel_key, rule_mask, rule_start, rule_end "
            "FROM channels WHERE (rule_mask<>0 OR rule_start IS NOT NULL OR rule_end IS NOT NULL)"
        ).fetchall()
        for r in rows:
            dev, ck = r["device_id"], r["channel_key"]
            if channel_has_any_rules(dev, ck):
                continue
            mask = int(r["rule_mask"] or 0)
            s = r["rule_start"] or "00:00"
            e = r["rule_end"] or "00:00"
            if mask == 0:
                for d in range(7):
                    replace_channel_rules_for_day(dev, ck, d, [(s,e)])
            else:
                for d in range(7):
                    if (mask & (1<<d)) != 0:
                        replace_channel_rules_for_day(dev, ck, d, [(s,e)])
            conn.execute("UPDATE channels SET rule_mask=0, rule_start=NULL, rule_end=NULL "
                         "WHERE device_id=? AND channel_key=?", (dev, ck))
        conn.commit()
    finally:
        conn.close()

def _now_str(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _bootstrap_admin_if_absent():
    conn = _db()
    try:
        r = conn.execute("SELECT COUNT(1) AS c FROM users").fetchone()
        if (r and int(r["c"]) == 0):
            u = os.getenv("ADMIN_USER", "admin")
            p = os.getenv("ADMIN_PASS", "admin")
            conn.execute("INSERT INTO users(username, password_hash, is_admin, active, created_at) VALUES(?,?,?,?,?)",
                         (u, generate_password_hash(p), 1, 1, _now_str()))
            conn.commit()
            LOG.warning("bootstrap: created admin user '%s' (please change password)", u)
    finally:
        conn.close()

def user_by_username(username: str):
    conn = _db()
    try:
        return conn.execute("SELECT * FROM users WHERE username=? AND active=1", (username,)).fetchone()
    finally:
        conn.close()

def user_by_id(uid: int):
    conn = _db()
    try:
        return conn.execute("SELECT * FROM users WHERE id=? AND active=1", (uid,)).fetchone()
    finally:
        conn.close()

def user_list():
    conn = _db()
    try:
        return conn.execute("SELECT id,username,is_admin,active,created_at FROM users ORDER BY id ASC").fetchall()
    finally:
        conn.close()

def user_add(username: str, password: str, is_admin: int):
    conn = _db()
    try:
        conn.execute("INSERT INTO users(username,password_hash,is_admin,active,created_at) VALUES(?,?,?,?,?)",
                     (username, generate_password_hash(password), int(is_admin), 1, _now_str()))
        conn.commit()
    finally:
        conn.close()

def user_delete(uid: int):
    conn = _db()
    try:
        conn.execute("DELETE FROM users WHERE id=?", (uid,))
        conn.execute("DELETE FROM user_channels WHERE user_id=?", (uid,))
        conn.commit()
    finally:
        conn.close()

def user_visible_pairs(uid: int) -> set[tuple[str,str]]:
    u = user_by_id(uid)
    if not u: return set()
    if int(u["is_admin"]) == 1: return set()
    conn = _db()
    try:
        rows = conn.execute("SELECT device_id, channel_key FROM user_channels WHERE user_id=?", (uid,)).fetchall()
        return {(r["device_id"], r["channel_key"]) for r in rows}
    finally:
        conn.close()

def replace_user_visible_pairs(uid: int, pairs: list[tuple[str,str]]):
    conn = _db()
    try:
        conn.execute("DELETE FROM user_channels WHERE user_id=?", (uid,))
        for dev, ck in pairs:
            conn.execute("INSERT OR IGNORE INTO user_channels(user_id,device_id,channel_key) VALUES(?,?,?)", (uid, dev, ck))
        conn.commit()
    finally:
        conn.close()

def webhooks_list(active_only=True):
    conn = _db()
    try:
        if active_only:
            return conn.execute("SELECT * FROM webhooks WHERE enabled=1 ORDER BY id ASC").fetchall()
        return conn.execute("SELECT * FROM webhooks ORDER BY id ASC").fetchall()
    finally:
        conn.close()

def webhook_add(name: str, token: str, secret: str, enabled: int, is_default: int):
    conn = _db()
    try:
        cur = conn.execute(
            "INSERT INTO webhooks(name,access_token,secret,enabled,is_default,created_at) VALUES(?,?,?,?,?,?)",
            (name, token, secret, int(enabled), 0, _now_str())  # 先插入，默认先置 0
        )
        conn.commit()
        wid = int(cur.lastrowid or 0)
    finally:
        conn.close()

    if wid > 0:
        if int(is_default) == 1:
            webhook_set_default(wid)
        else:
            # 如果还没有默认，但新增的是 enabled=1，则自动补一个默认
            if int(enabled) == 1 and webhook_get_default_enabled_id() is None:
                webhook_set_default(wid)

def webhook_update_enable(wid: int, enabled: int, is_default: Optional[int]=None):
    conn = _db()
    try:
        if is_default is None:
            conn.execute("UPDATE webhooks SET enabled=? WHERE id=?", (int(enabled), wid))
        else:
            conn.execute(
                "UPDATE webhooks SET enabled=?, is_default=? WHERE id=?",
                (int(enabled), int(is_default), wid)
            )
        conn.commit()
    finally:
        conn.close()

def webhook_delete(wid: int):
    conn = _db()
    try:
        conn.execute("DELETE FROM webhooks WHERE id=?", (wid,))
        conn.execute("DELETE FROM channel_webhooks WHERE webhook_id=?", (wid,))
        conn.commit()
    finally:
        conn.close()

def channel_webhook_ids(device_id: str, channel_key: str) -> list[int]:
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT webhook_id FROM channel_webhooks WHERE device_id=? AND channel_key=?",
            (device_id, channel_key)
        ).fetchall()
        return [int(r["webhook_id"]) for r in rows]
    finally:
        conn.close()

def replace_channel_webhooks(device_id: str, channel_key: str, webhook_ids: list[int]):
    conn = _db()
    try:
        conn.execute("DELETE FROM channel_webhooks WHERE device_id=? AND channel_key=?", (device_id, channel_key))
        for wid in webhook_ids:
            conn.execute("INSERT OR IGNORE INTO channel_webhooks(device_id,channel_key,webhook_id) VALUES(?,?,?)",
                         (device_id, channel_key, int(wid)))
        conn.commit()
    finally:
        conn.close()

def webhook_get_default_enabled_id() -> Optional[int]:
    conn = _db()
    try:
        r = conn.execute(
            "SELECT id FROM webhooks WHERE enabled=1 AND is_default=1 ORDER BY id ASC LIMIT 1"
        ).fetchone()
        return int(r["id"]) if r else None
    finally:
        conn.close()

def webhook_set_default(wid: int):
    """设置唯一默认（并强制 enabled=1）"""
    conn = _db()
    try:
        conn.execute("UPDATE webhooks SET is_default=0")
        conn.execute("UPDATE webhooks SET is_default=1, enabled=1 WHERE id=?", (int(wid),))
        conn.commit()
    finally:
        conn.close()

def webhook_ensure_some_default():
    """如果存在 enabled=1 的 webhook 但没有默认，则挑一个最小 id 当默认"""
    conn = _db()
    try:
        r = conn.execute(
            "SELECT 1 FROM webhooks WHERE enabled=1 AND is_default=1 LIMIT 1"
        ).fetchone()
        if r:
            return
        r2 = conn.execute(
            "SELECT id FROM webhooks WHERE enabled=1 ORDER BY id ASC LIMIT 1"
        ).fetchone()
        if r2:
            conn.execute("UPDATE webhooks SET is_default=0")
            conn.execute("UPDATE webhooks SET is_default=1 WHERE id=?", (int(r2["id"]),))
            conn.commit()
    finally:
        conn.close()


@lru_cache(maxsize=128)
def _robot_cached(wid: int):
    conn = _db()
    try:
        r = conn.execute("SELECT access_token, secret, enabled FROM webhooks WHERE id=?", (wid,)).fetchone()
        if not r or int(r["enabled"]) != 1:
            return None
        return DingRobot(access_token=r["access_token"], secret=(r["secret"] or ""), timeout=8.0)
    finally:
        conn.close()

# ---------------- DB sweep & vacuum helpers ----------------
def _db_file_size_bytes() -> int:
    try:
        return DB_PATH.stat().st_size
    except Exception:
        return 0

def _vacuum_db_safely() -> bool:
    """VACUUM 会占用额外临时空间，低磁盘时跳过。"""
    try:
        sz = _db_file_size_bytes()
        if sz <= 0:
            return True
        free = shutil.disk_usage(str(DB_PATH.parent)).free
        need = int(sz * 1.2)
        if free < need:
            LOG.warning("vacuum: skip (free=%s < need~%s)", free, need)
            return False
        c = _db()
        try:
            c.execute("VACUUM")
            c.commit()
        finally:
            c.close()
        return True
    except Exception as e:
        LOG.warning("vacuum: fail: %s", e)
        return False

def _db_rotate_once(vacuum: bool=False) -> int:
    if DB_RETAIN_DAYS <= 0 and DB_MAX_ROWS <= 0:
        return 0

    deleted = 0
    conn = _db()
    try:
        if DB_RETAIN_DAYS > 0:
            cutoff = (datetime.now() - timedelta(days=DB_RETAIN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
            cur = conn.execute("DELETE FROM messages WHERE ts < ?", (cutoff,))
            deleted += (cur.rowcount or 0)

        if DB_MAX_ROWS > 0:
            r = conn.execute("SELECT COUNT(1) AS c FROM messages").fetchone()
            total = int(r["c"]) if r else 0
            over = total - int(DB_MAX_ROWS)
            if over > 0:
                cur = conn.execute(
                    "DELETE FROM messages WHERE id IN ("
                    "  SELECT id FROM messages ORDER BY ts ASC, id ASC LIMIT ?"
                    ")",
                    (over,)
                )
                deleted += (cur.rowcount or 0)

        conn.commit()
    except sqlite3.OperationalError as e:
        LOG.warning("dbclean: skip (%s)", e)
        return 0
    finally:
        conn.close()

    if vacuum and DB_VACUUM and deleted > 0:
        _vacuum_db_safely()

    if deleted > 0:
        LOG.info("dbclean: deleted=%s (retain_days=%s max_rows=%s vacuum=%s)",
                 deleted, DB_RETAIN_DAYS, DB_MAX_ROWS, int(vacuum and DB_VACUUM))
    return deleted

def _db_sweep_maybe(now: float):
    global _db_sweep_last
    if DB_SWEEP_SEC <= 0:
        return
    if DB_MAX_ROWS <= 0:
        return
    if (now - _db_sweep_last) < DB_SWEEP_SEC:
        return
    _db_sweep_last = now
    _db_rotate_once(vacuum=False)

# ---------------- Reconcile: DB <-> snaps ----------------
def reconcile_db_and_snaps() -> Dict[str, int]:
    """
    最稳策略：
      - DB 引用但文件不存在：默认删记录（BROKEN_REF_POLICY=delete_record）
      - 文件存在但 DB 不引用：默认删文件（ORPHAN_FILE_POLICY=delete_file）
    """
    root = Path(APP.static_folder) / "snaps"
    root.mkdir(parents=True, exist_ok=True)

    referenced: set[str] = set()
    scanned_urls = 0

    truncated = False
    conn = _db()
    try:
        cur = conn.execute("SELECT id, image_url FROM messages WHERE image_url IS NOT NULL AND image_url<>''")
        for r in cur:
            scanned_urls += 1
            if RECONCILE_MAX_URLS > 0 and scanned_urls > RECONCILE_MAX_URLS:
                LOG.warning("reconcile: stop (scanned_urls>%s)", RECONCILE_MAX_URLS)
                truncated = True
                break
            rel = _snap_rel_from_url(r["image_url"] or "")
            if rel:
                referenced.add(rel)
    finally:
        conn.close()

    # 1) broken refs
    broken = 0
    fixed_rows = 0
    for rel in list(referenced):
        if not _snap_local_path_from_rel(rel).exists():
            broken += 1
            if BROKEN_REF_POLICY == "clear_url":
                pat = "%" + "/" + rel
                c2 = _db()
                try:
                    cur = c2.execute("UPDATE messages SET image_url=NULL WHERE image_url LIKE ?", (pat,))
                    c2.commit()
                    fixed_rows += (cur.rowcount or 0)
                finally:
                    c2.close()
            else:
                fixed_rows += _delete_db_rows_by_rel(rel)
            referenced.discard(rel)

    # 2) orphan files
    orphan = 0
    deleted_files = 0
    if ORPHAN_FILE_POLICY == "delete_file" and (not truncated):
        for p in root.rglob("*.jpg"):
            try:
                rel = f"snaps/{p.parent.name}/{p.name}"
                if rel not in referenced:
                    orphan += 1
                    p.unlink(missing_ok=True)
                    deleted_files += 1
            except Exception:
                pass
    elif truncated:
        LOG.warning("reconcile: truncated scan -> skip orphan deletion to avoid false deletes")

    # 3) remove empty dirs
    removed_dirs = 0
    for d in sorted(root.glob("*")):
        try:
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
                removed_dirs += 1
        except Exception:
            pass

    stats = {
        "scanned_urls": scanned_urls,
        "broken_refs": broken,
        "fixed_rows": fixed_rows,
        "orphan_files": orphan,
        "deleted_files": deleted_files,
        "removed_dirs": removed_dirs,
    }
    LOG.info("reconcile: %s", stats)
    return stats

# ---------------- Markdown 构造 ----------------
def _build_md(payload: Dict[str, Any], img_url: Optional[str]) -> Tuple[str, str]:
    type_id   = _safe_int(payload, "type", None)
    type_name = _safe_str(payload, "typeName")
    title = f"[{APP_NAME}] 告警：{_algo_name(type_id, type_name)}"

    st   = _parse_time(_safe_str(payload, "signTime"))
    box  = _safe_str(payload, "boxName")
    box_id = _safe_str(payload, "boxId")
    cam  = _safe_str(payload, "deviceName")
    score = _safe_str(payload, "score")

    lines = []
    if img_url:
        lines.append(f"![snap]({img_url})\n")

    lines += [
        f"- **时间**：`{st}`",
        f"- **算法**：`{_algo_name(type_id, type_name)}`",
        f"- **设备**：`{cam or '-'} / {box or '-'}(boxId={box_id or '-'})`",
    ]

    attr_bits = []
    for k in ("age", "gender", "mask", "count"):
        if payload.get(k) is not None:
            attr_bits.append(f"{k}={payload.get(k)}")
    if attr_bits:
        lines.append(f"- **attr**：`{' , '.join(attr_bits)}`")

    if VISIBLE_AT and (AT_MOBILES or AT_USER_IDS):
        pass

    return title, "\n".join(lines)

# ---------------- Core Handle ----------------
def _handle_record_and_forward(payload: Dict[str, Any], echo: bool=False) -> Dict[str, Any]:
    dkey = _dedup_key(payload)
    now  = time.time()
    last = _recent_keys.get(dkey)
    if last and (now - last) < DEDUP_WINDOW:
        return {"code": 200, "message": "重复告警抑制"}
    _recent_keys[dkey] = now
    _prune_recent_keys(now, DEDUP_WINDOW)

    st         = _parse_time(_safe_str(payload, "signTime"))
    type_id    = _safe_int(payload, "type", None)
    type_name  = _safe_str(payload, "typeName")
    box_name   = _safe_str(payload, "boxName")
    device_name= _safe_str(payload, "deviceName")
    score      = _safe_str(payload, "score")

    dev_id, ch_key, ch_name, box_nm, idx_or_gbid = _pos_key(payload)
    dev_enabled = upsert_device(dev_id, st)
    ch_enabled, rule_mask, rule_start, rule_end = upsert_channel(
        dev_id, ch_key, ch_name, box_nm, idx_or_gbid, st
    )

    now_dt   = datetime.now()
    now_dow  = now_dt.weekday()
    now_hm   = now_dt.strftime("%H:%M")

    has_rules = channel_has_any_rules(dev_id, ch_key)
    if has_rules:
        segs = channel_rules_for_weekday(dev_id, ch_key, now_dow)
        in_time_multi = any(_in_time_window(now_hm, s, e) for (s,e) in segs) if segs else False
        time_ok = in_time_multi
    else:
        time_ok = True

    forward_ok = (dev_enabled == 1) and (ch_enabled == 1) and time_ok

    img_url = _resolve_image_url(payload)

    forwarded = False
    forward_reason = ""
    title, text_md = _build_md(payload, img_url)

    if not echo and forward_ok:
        target_ids = channel_webhook_ids(dev_id, ch_key)
        if not target_ids:
            did = webhook_get_default_enabled_id()
            target_ids = [did] if did else []

        succ = 0; total = 0; errs = []
        for wid in (target_ids or []):
            total += 1
            bot = _robot_cached(wid)
            if not bot:
                errs.append(f"wid={wid}禁用/不存在")
                continue
            try:
                bot.send_markdown(title=title, text_md=text_md,
                                  at_user_ids=AT_USER_IDS or None,
                                  at_mobiles=AT_MOBILES or None)
                succ += 1
            except DingRobotError as e:
                errs.append(f"wid={wid}:{e}")

        if total == 0:
            forwarded = False
            forward_reason = "未转发（无可用webhook）"
        else:
            forwarded = (succ > 0)
            if forwarded:
                forward_reason = f"已转发({succ}/{total})"
            else:
                forward_reason = "未转发（全部失败：" + "；".join(errs[:2]) + "）"
    else:
        if echo:
            forward_reason = "未转发（echo调试）"
        else:
            reasons = []
            if dev_enabled != 1: reasons.append("设备禁用")
            if ch_enabled  != 1: reasons.append("通道禁用")
            if not time_ok:      reasons.append("非时间段")
            forward_reason = "未转发（" + ("，".join(reasons) or "未知原因") + "）"

    rec = {
        "ts": st,
        "device_id": dev_id,
        "channel_key": ch_key,
        "channel_name": ch_name,
        "type": type_id,
        "type_name": type_name,
        "box_name": box_name,
        "device_name": device_name,
        "score": score,
        "image_url": img_url,
        "forwarded": forwarded,
        "forward_reason": forward_reason,
        "dedup_key": dkey,
        "raw_json": json.dumps(payload, ensure_ascii=False)
    }
    try:
        insert_message(rec)
    except Exception as e:
        LOG.error("db insert fail: %s", e)

    _db_sweep_maybe(time.time())

    if echo:
        return {"code": 200, "message": "echo", "title": title,
                "image_url": img_url, "forward_enabled": bool(forward_ok)}
    return {"code": 200, "message": "数据接收成功"}

# ---------------- Flask Routes ----------------
@APP.get("/healthz")
def healthz():
    return jsonify(ok=True, app=APP_NAME, time=time.time())

@APP.post("/ai/message")
def ai_message():
    if AUTH_TOKEN:
        t = request.args.get("token") or request.headers.get("X-Auth-Token", "")
        if t != AUTH_TOKEN:
            return jsonify(code=401, message="unauthorized"), 401
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        return jsonify(code=400, message="JSON解析失败"), 400
    resp = _handle_record_and_forward(payload, echo=(request.args.get("echo") == "1"))
    return jsonify(resp)

# ---------- Auth & Admin UI ----------
def login_required(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*a, **kw):
        if not session.get("authed"):
            return redirect(url_for("login", next=request.path))
        return f(*a, **kw)
    return wrapper

def admin_required(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*a, **kw):
        if not session.get("authed"):
            return redirect(url_for("login", next=request.path))
        if not session.get("is_admin"):
            return redirect(url_for("history"))
        return f(*a, **kw)
    return wrapper

@APP.route("/login", methods=["GET","POST"])
def login():
    err = ""
    if request.method == "POST":
        u = request.form.get("username", "").strip()
        p = request.form.get("password", "")
        row = user_by_username(u)
        if row and check_password_hash(row["password_hash"], p) and int(row["active"]) == 1:
            session["authed"] = True
            session["user"] = u
            session["uid"] = int(row["id"])
            session["is_admin"] = (int(row["is_admin"]) == 1)
            nxt = request.args.get("next") or url_for("history")
            return redirect(nxt)
        err = "用户名或密码不正确"

    return render_template_string("""
<!doctype html>
<title>登录 - Alarm2Ding</title>
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">

{{ topbar('Alarm2Ding', nav) }}

<div class="container" style="max-width:460px">
  <div class="card">
    <h2 style="margin:0 0 12px">账户登录</h2>
    {% if err %}
      <div class="badge badge-err" style="display:block;margin-bottom:10px">{{ err }}</div>
    {% endif %}
    <form method="post" class="form-grid">
      <input name="username" class="inp" placeholder="用户名" autofocus required>
      <input name="password" type="password" class="inp" placeholder="密码" required>
      <button type="submit" class="btn btn-primary" style="width:max-content">登录</button>
    </form>
  </div>
</div>
""", err=err, nav=[])

@APP.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ---------- Maintenance (admin) ----------
@APP.get("/maintenance")
@admin_required
def maintenance_page():
    last = session.get("maintenance_last") or {}
    nav = [
      {"label":"维护", "href":url_for("maintenance_page"), "active":True},
      {"label":"Webhook", "href":url_for("webhooks_page")},
      {"label":"用户", "href":url_for("users_page")},
      {"label":"通道", "href":url_for("devices")},
      {"label":"历史记录", "href":url_for("history")},
      {"label":"退出", "href":url_for("logout")},
    ]
    return render_template_string("""
<!doctype html>
<title>维护 - Alarm2Ding</title>
<meta name="viewport" content="width=device-width,initial-scale=1">

{{ topbar('维护', nav) }}

<div class="container">
  <div class="card">
    <h3 style="margin:0 0 10px">一键操作</h3>
    <div style="display:flex;gap:10px;flex-wrap:wrap">
      <form method="post" action="{{ url_for('maintenance_reconcile') }}">
        <button class="btn btn-primary">对账修复（DB↔图片）</button>
      </form>
      <form method="post" action="{{ url_for('maintenance_clean') }}" onsubmit="return confirm('立即执行清理？');">
        <button class="btn">立即清理（图片 + DB轮巡）</button>
      </form>
      <form method="post" action="{{ url_for('maintenance_vacuum') }}" onsubmit="return confirm('VACUUM 可能耗时且占用临时空间，确定？');">
        <button class="btn">立即 VACUUM</button>
      </form>
    </div>
    <div class="muted small" style="margin-top:10px">
      最稳策略：图片缺失→删记录；记录缺失→删孤儿图。每日定时也会执行（可用 env 控制）。
    </div>
  </div>

  <div class="card" style="margin-top:12px">
    <h3 style="margin:0 0 10px">最近一次结果</h3>
    <pre style="white-space:pre-wrap;margin:0">{{ last | tojson(indent=2) }}</pre>
  </div>
</div>
""", last=last, nav=nav)

@APP.post("/maintenance/reconcile")
@admin_required
def maintenance_reconcile():
    stats = reconcile_db_and_snaps()
    session["maintenance_last"] = {"op":"reconcile", "at":_now_str(), "stats":stats}
    return redirect(url_for("maintenance_page"))

@APP.post("/maintenance/clean")
@admin_required
def maintenance_clean():
    _clean_old_snaps_once()
    _db_rotate_once(vacuum=True)
    stats = reconcile_db_and_snaps()
    session["maintenance_last"] = {"op":"clean+db+reconcile", "at":_now_str(), "stats":stats}
    return redirect(url_for("maintenance_page"))

@APP.post("/maintenance/vacuum")
@admin_required
def maintenance_vacuum():
    ok = _vacuum_db_safely()
    session["maintenance_last"] = {"op":"vacuum", "at":_now_str(), "ok":ok, "db_size":_db_file_size_bytes()}
    return redirect(url_for("maintenance_page"))

# ---------- User pages ----------
@APP.get("/users")
@admin_required
def users_page():
    rows = user_list()
    nav = [
      {"label":"用户", "href":url_for("users_page"), "active":True},
      {"label":"维护", "href":url_for("maintenance_page")},
      {"label":"Webhook", "href":url_for("webhooks_page")},
      {"label":"通道", "href":url_for("devices")},
      {"label":"历史记录", "href":url_for("history")},
      {"label":"退出", "href":url_for("logout")},
    ]
    return render_template_string("""
<!doctype html>
<title>用户管理 - Alarm2Ding</title>
<meta name="viewport" content="width=device-width,initial-scale=1">

{{ topbar('用户管理', nav) }}

<div class="container">

  <div class="card" style="margin-top:12px">
    <h3 style="margin:0 0 10px">新增用户</h3>
    <form method="post" action="{{ url_for('users_add') }}" class="form-grid">
      <input name="username" placeholder="用户名" class="inp" required>
      <input name="password" placeholder="初始密码" class="inp" required>
      <label class="inp" style="display:flex;align-items:center;gap:8px;border:none">
        <input type="checkbox" name="is_admin" value="1"> 管理员
      </label>
      <div></div><div></div>
      <button class="btn btn-primary" style="width:max-content">添加</button>
    </form>
  </div>

  <div class="card">
    <table class="table cardify">
      <thead>
        <tr>
          <th>ID</th>
          <th>用户名</th>
          <th>角色</th>
          <th>状态</th>
          <th>创建时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
      {% for r in rows %}
        <tr>
          <td data-label="ID">{{ r['id'] }}</td>
          <td data-label="用户名">{{ r['username'] }}</td>
          <td data-label="角色">
            {% if r['is_admin'] %}
              <span class="badge badge-ok">管理员</span>
            {% else %}
              <span class="badge">普通用户</span>
            {% endif %}
          </td>
          <td data-label="状态">
            {% if r['active'] %}
              <span class="badge badge-ok">启用</span>
            {% else %}
              <span class="badge badge-err">停用</span>
            {% endif %}
          </td>
          <td data-label="创建时间">{{ r['created_at'] or '' }}</td>
          <td data-label="操作">
            <div class="ops">
              <a class="btn" href="{{ url_for('users_perm', uid=r['id']) }}">配置可见通道</a>
              {% if not r['is_admin'] %}
              <form method="post" action="{{ url_for('users_del') }}" onsubmit="return confirm('删除该用户？不可恢复');" style="display:inline">
                <input type="hidden" name="uid" value="{{ r['id'] }}">
                <button class="btn btn-danger">删除</button>
              </form>
              {% else %}
                <span class="muted">管理员不可删除</span>
              {% endif %}
            </div>
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</div>

<style>
.ops{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
</style>
""", rows=rows, nav=nav)

@APP.post("/users/add")
@admin_required
def users_add():
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "")
    is_admin = 1 if request.form.get("is_admin")=="1" else 0
    if username and password:
        user_add(username, password, is_admin)
    return redirect(url_for("users_page"))

@APP.post("/users/del")
@admin_required
def users_del():
    try:
        uid = int(request.form.get("uid") or "0")
        user_delete(uid)
    except Exception:
        pass
    return redirect(url_for("users_page"))

@APP.route("/users/perm", methods=["GET","POST"])
@admin_required
def users_perm():
    uid = int(request.args.get("uid") or request.form.get("uid") or "0")
    u = user_by_id(uid)
    if not u:
        return redirect(url_for("users_page"))

    if request.method == "POST":
        pairs = []
        for k, v in request.form.items():
            if k.startswith("ck_") and v == "1":
                _, dev, ck = k.split("___", 2)
                pairs.append((dev, ck))
        replace_user_visible_pairs(uid, pairs)
        return redirect(url_for("users_page"))

    rows = list_channels("")
    vis = user_visible_pairs(uid)

    nav = [
      {"label":"用户", "href":url_for("users_page"), "active":True},
      {"label":"维护", "href":url_for("maintenance_page")},
      {"label":"Webhook", "href":url_for("webhooks_page")},
      {"label":"通道", "href":url_for("devices")},
      {"label":"历史记录", "href":url_for("history")},
      {"label":"退出", "href":url_for("logout")},
    ]
    
    return render_template_string("""
<!doctype html>
<title>配置可见通道 - Alarm2Ding</title>
<meta name="viewport" content="width=device-width,initial-scale=1">

{{ topbar('配置可见通道', nav) }}

<div class="container">

  <div class="card" style="margin-top:12px">
    <h3 style="margin:0 0 8px">用户：{{ u['username'] }}</h3>
    <div class="muted">勾选后该用户即可在“历史记录”中看到选中的通道告警。</div>
  </div>

  <form method="post">
    <input type="hidden" name="uid" value="{{ u['id'] }}">

    <div class="card" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px">
      <input id="kw" class="inp" placeholder="关键字过滤（device_id / 位置键 / 位置名 / box / index/gbid）" style="min-width:260px">
      <button type="button" class="btn" onclick="selectAll(true)">全选可见</button>
      <button type="button" class="btn" onclick="selectAll(false)">全不选</button>
      <button type="button" class="btn" onclick="invertSel()">反选</button>
      <span class="muted small" id="stat"></span>
    </div>

    <div class="card">
      <table class="table cardify" id="tab">
        <thead>
          <tr>
            <th style="width:28px"><input type="checkbox" id="chk_all" onclick="toggleAll()"></th>
            <th>设备ID</th>
            <th>位置键</th>
            <th>位置名</th>
            <th>box</th>
            <th>index/gbid</th>
          </tr>
        </thead>
        <tbody>
        {% for r in rows %}
          {% set checked = ((r['device_id'], r['channel_key']) in vis) %}
          <tr>
            <td data-label="选">
              <input type="checkbox"
                     name="ck___{{ r['device_id'] }}___{{ r['channel_key'] }}"
                     value="1" {% if checked %}checked{% endif %}>
            </td>
            <td data-label="设备ID"><code>{{ r['device_id'] }}</code></td>
            <td data-label="位置键"><code>{{ r['channel_key'] }}</code></td>
            <td data-label="位置名">{{ r['channel_name'] or '' }}</td>
            <td data-label="box">{{ r['box_name'] or '' }}</td>
            <td data-label="index/gbid">{{ r['index_or_gbid'] or '' }}</td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>

    <div style="display:flex;gap:10px;margin-top:12px">
      <button class="btn btn-primary" type="submit">保存</button>
      <a class="btn" href="{{ url_for('users_page') }}">返回</a>
    </div>
  </form>
</div>

<script>
const $ = (s, p=document) => p.querySelector(s);
const $$ = (s, p=document) => Array.from(p.querySelectorAll(s));

function toggleAll(){
  const c = $('#chk_all').checked;
  $$('#tab tbody input[type="checkbox"]').forEach(x => { if (!x.closest('tr').hidden) x.checked = c; });
  updateStat();
}
function selectAll(v){
  $$('#tab tbody tr').forEach(tr => {
    if (!tr.hidden){
      const cb = tr.querySelector('input[type="checkbox"]');
      if (cb) cb.checked = v;
    }
  });
  updateStat();
}
function invertSel(){
  $$('#tab tbody tr').forEach(tr => {
    if (!tr.hidden){
      const cb = tr.querySelector('input[type="checkbox"]');
      if (cb) cb.checked = !cb.checked;
    }
  });
  updateStat();
}
function updateStat(){
  const all = $$('#tab tbody input[type="checkbox"]').filter(cb => !cb.closest('tr').hidden);
  const on  = all.filter(cb => cb.checked);
  $('#stat').textContent = `当前可见：${all.length} 行，已选：${on.length}`;
}
$('#kw').addEventListener('input', e => {
  const kw = e.target.value.trim().toLowerCase();
  $$('#tab tbody tr').forEach(tr => {
    const txt = tr.innerText.toLowerCase();
    tr.hidden = kw ? !txt.includes(kw) : false;
  });
  updateStat();
});
updateStat();
</script>
""", u=u, rows=rows, vis=vis, nav=nav)

# ---------- webhooks settings pages ----------
@APP.route("/webhooks", methods=["GET","POST"])
@admin_required
def webhooks_page():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        token= (request.form.get("token") or "").strip()
        secret=(request.form.get("secret") or "").strip()
        enabled = 1 if request.form.get("enabled")=="1" else 0
        is_def  = 1 if request.form.get("is_default")=="1" else 0
        if name and token:
            webhook_add(name, token, secret, enabled, is_def)
        return redirect(url_for("webhooks_page"))

    rows = webhooks_list(active_only=False)
    
    nav = [
      {"label":"Webhook", "href":url_for("webhooks_page"), "active":True},
      {"label":"维护", "href":url_for("maintenance_page")},
      {"label":"用户", "href":url_for("users_page")},
      {"label":"通道", "href":url_for("devices")},
      {"label":"历史记录", "href":url_for("history")},
      {"label":"退出", "href":url_for("logout")},
    ]
    
    return render_template_string("""
<!doctype html>
<title>Webhook 管理 - Alarm2Ding</title>
<meta name="viewport" content="width=device-width,initial-scale=1">

{{ topbar('Webhook 管理', nav) }}

<div class="container">

  <div class="card" style="margin-top:12px">
    <h3 style="margin:0 0 10px">新增 Webhook</h3>
    <form method="post" class="form-grid">
      <input name="name" placeholder="名称" class="inp" required>
      <input name="token" placeholder="access_token" class="inp" required>
      <input name="secret" placeholder="secret（可空）" class="inp">
      <label class="inp" style="display:flex;align-items:center;gap:8px;border:none">
        <input type="checkbox" name="enabled" value="1" checked> 启用
      </label>
      <label class="inp" style="display:flex;align-items:center;gap:8px;border:none">
        <input type="checkbox" name="is_default" value="1"> 默认
      </label>
      <button class="btn btn-primary" style="width:max-content">添加</button>
    </form>
    <div class="muted" style="margin-top:6px">提示：若通道未绑定任何 webhook，则回退使用“默认 webhook”。</div>
  </div>

  <div class="card">
    <table class="table cardify">
      <thead>
        <tr>
          <th>ID</th>
          <th>名称</th>
          <th>状态</th>
          <th>默认</th>
          <th>创建时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
      {% for r in rows %}
        {% set enabled = (r['enabled']==1) %}
        {% set isdef = (r['is_default']==1) %}
        <tr>
          <td data-label="ID">{{ r['id'] }}</td>
          <td data-label="名称">{{ r['name'] }}</td>
          <td data-label="状态">
            {% if enabled %}
              <span class="badge badge-ok">启用</span>
            {% else %}
              <span class="badge badge-err">禁用</span>
            {% endif %}
          </td>
          <td data-label="默认">
            {% if isdef %}
              <span class="badge badge-warn">默认</span>
            {% endif %}
          </td>
          <td data-label="创建时间">{{ r['created_at'] or '' }}</td>
          <td data-label="操作">
            <div class="ops">
              <form method="post" action="{{ url_for('webhooks_toggle') }}" style="display:inline">
                <input type="hidden" name="wid" value="{{ r['id'] }}">
                <input type="hidden" name="enabled" value="{{ 0 if enabled else 1 }}">
                <button class="btn">{{ '禁用' if enabled else '启用' }}</button>
              </form>
              <form method="post" action="{{ url_for('webhooks_toggle_default') }}" style="display:inline">
                <input type="hidden" name="wid" value="{{ r['id'] }}">
                <input type="hidden" name="is_default" value="{{ 0 if isdef else 1 }}">
                <button class="btn">{{ '取消默认' if isdef else '设为默认' }}</button>
              </form>
              <form method="post" action="{{ url_for('webhooks_del') }}" style="display:inline" onsubmit="return confirm('删除该 webhook？不可恢复');">
                <input type="hidden" name="wid" value="{{ r['id'] }}">
                <button class="btn btn-danger">删除</button>
              </form>
            </div>
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</div>

<style>
.ops{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
</style>
""", rows=rows, nav=nav)

@APP.post("/webhooks/toggle")
@admin_required
def webhooks_toggle():
    wid = int(request.form.get("wid"))
    enabled = int(request.form.get("enabled"))

    conn = _db()
    try:
        # 如果要禁用当前默认，则先清掉默认标记
        if enabled == 0:
            conn.execute("UPDATE webhooks SET enabled=0, is_default=0 WHERE id=?", (wid,))
        else:
            conn.execute("UPDATE webhooks SET enabled=1 WHERE id=?", (wid,))
        conn.commit()
    finally:
        conn.close()

    if enabled == 0:
        webhook_ensure_some_default()

    _robot_cached.cache_clear()
    return redirect(url_for("webhooks_page"))

@APP.post("/webhooks/toggle_default")
@admin_required
def webhooks_toggle_default():
    wid = int(request.form.get("wid"))
    is_def = int(request.form.get("is_default"))  # 1=设为默认，0=取消默认

    if is_def == 1:
        webhook_set_default(wid)
    else:
        conn = _db()
        try:
            conn.execute("UPDATE webhooks SET is_default=0 WHERE id=?", (wid,))
            conn.commit()
        finally:
            conn.close()
        webhook_ensure_some_default()

    _robot_cached.cache_clear()
    return redirect(url_for("webhooks_page"))

@APP.post("/webhooks/del")
@admin_required
def webhooks_del():
    wid = int(request.form.get("wid"))
    webhook_delete(wid)
    _robot_cached.cache_clear()
    return redirect(url_for("webhooks_page"))

# ---------- Device & Channel pages ----------
@APP.route("/devices", methods=["GET","POST"])
@login_required
def devices():
    if request.method == "POST":
        if not session.get("is_admin"):
            abort(403)
        device_id   = request.form.get("device_id", "")
        channel_key = request.form.get("channel_key", "")
        enabled     = 1 if request.form.get("enabled") == "1" else 0
        if device_id and channel_key:
            set_channel_enabled(device_id, channel_key, enabled)
        back_device = request.args.get("device_id","")
        back_qs = ("?device_id="+back_device) if back_device else ""
        return redirect(url_for("devices") + back_qs)

    device_filter = (request.args.get("device_id") or "").strip()
    rows = list_channels(device_filter=device_filter)

    if not session.get("is_admin"):
        vset = user_visible_pairs(int(session.get("uid")))
        rows = [r for r in rows if (r["device_id"], r["channel_key"]) in vset]

    rows2 = []
    for r in rows:
        rule_label = summarize_rules_short(r["device_id"], r["channel_key"])
        d = dict(r)
        d["rule_label"] = rule_label
        rows2.append(d)

    nav = [{"label":"通道", "href":url_for("devices"), "active":True},
       {"label":"历史记录", "href":url_for("history")},
       {"label":"退出", "href":url_for("logout")}]
    if session.get("is_admin"):
      nav.insert(1, {"label":"维护", "href":url_for("maintenance_page")})
      nav.insert(2, {"label":"用户", "href":url_for("users_page")})
      nav.insert(3, {"label":"Webhook", "href":url_for("webhooks_page")})

    return render_template_string("""
<!doctype html>
<title>通道管理 - Alarm2Ding</title>
<meta name="viewport" content="width=device-width,initial-scale=1">

{{ topbar('通道管理', nav) }}

<div class="container">

  <div class="card" style="margin-bottom:12px;margin-top:12px">
    <form method="get" style="display:flex;gap:8px;flex-wrap:wrap">
      <input name="device_id" class="inp" placeholder="按 device_id 过滤" value="{{ request.args.get('device_id','') }}" style="min-width:220px">
      <button type="submit" class="btn">筛选</button>
    </form>
  </div>

  <div class="card">
    <table class="table cardify">
      <thead><tr>
        <th>设备ID</th><th>位置键</th><th>位置名</th><th>box</th><th>index/gbid</th>
        <th>状态</th><th>规则摘要</th><th>操作</th>
      </tr></thead>
      <tbody>
        {% for r in rows %}
        <tr>
          <td data-label="设备ID"><code>{{ r['device_id'] }}</code></td>
          <td data-label="位置键"><code>{{ r['channel_key'] }}</code></td>
          <td data-label="位置名">{{ r['channel_name'] or '' }}</td>
          <td data-label="box">{{ r['box_name'] or '' }}</td>
          <td data-label="index/gbid">{{ r['index_or_gbid'] or '' }}</td>
          <td data-label="状态">
            {% if r['enabled'] %}
              <span class="badge badge-ok">转发</span>
            {% else %}
              <span class="badge badge-err">不转发</span>
            {% endif %}
          </td>
          <td data-label="规则摘要" style="font-size:12px;line-height:1.3">{{ r['rule_label'] }}</td>
          <td data-label="操作">
            <div class="ops">
              {% if session.get('is_admin') %}
              <form method="post">
                <input type="hidden" name="device_id" value="{{ r['device_id'] }}">
                <input type="hidden" name="channel_key" value="{{ r['channel_key'] }}">
                <input type="hidden" name="enabled" value="{{ 0 if r['enabled'] else 1 }}">
                <button type="submit" class="btn">{{ '禁用转发' if r['enabled'] else '启用转发' }}</button>
              </form>
              <a class="btn" href="{{ url_for('edit_channel_rule') }}?device_id={{ r['device_id'] }}&channel_key={{ r['channel_key'] }}">编辑规则</a>
              {% else %}
                <span class="muted small">无权限</span>
              {% endif %}
            </div>
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>

<style>
.ops{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.ops form{display:inline}
</style>
""", rows=rows2, nav=nav)

@APP.route("/devices/edit", methods=["GET","POST"])
@admin_required
def edit_channel_rule():
    device_id   = (request.args.get("device_id") or request.form.get("device_id") or "").strip()
    channel_key = (request.args.get("channel_key") or request.form.get("channel_key") or "").strip()
    if not device_id or not channel_key:
        return redirect(url_for("devices"))

    conn = _db()
    try:
        r = conn.execute("SELECT * FROM channels WHERE device_id=? AND channel_key=?",
                         (device_id, channel_key)).fetchone()
        if not r:
            return redirect(url_for("devices"))
    finally:
        conn.close()

    if request.method == "POST":
        sel = []
        for k, v in request.form.items():
            if k.startswith("wh_") and v == "1":
                sel.append(int(k.split("_",1)[1]))
        replace_channel_webhooks(device_id, channel_key, sel)

        for d in range(7):
            if request.form.get(f"day{d}_allday") == "1":
                replace_channel_rules_for_day(device_id, channel_key, d, [("00:00", "00:00")])
                continue

            segs: List[Tuple[str,str]] = []
            prefix_s = f"day{d}_start_"
            for k, v in request.form.items():
                if k.startswith(prefix_s):
                    idx = k[len(prefix_s):]
                    s = (v or "").strip()
                    e = (request.form.get(f"day{d}_end_{idx}") or "").strip()
                    if s or e:
                        s = s or "00:00"
                        e = e or "00:00"
                        segs.append((s, e))
            replace_channel_rules_for_day(device_id, channel_key, d, segs)

        return redirect(url_for("devices") + f"?device_id={device_id}")

    days_rules: List[List[Tuple[str,str]]] = []
    for d in range(7):
        days_rules.append(channel_rules_for_weekday(device_id, channel_key, d))

    whs = webhooks_list(active_only=False)
    bound = set(channel_webhook_ids(device_id, channel_key))

    nav = [
      {"label":"通道", "href":url_for("devices", device_id=device_id), "active":True},
      {"label":"历史记录", "href":url_for("history")},
      {"label":"退出", "href":url_for("logout")},
    ]
    if session.get("is_admin"):
        nav.insert(1, {"label":"维护", "href":url_for("maintenance_page")})
        nav.insert(2, {"label":"用户", "href":url_for("users_page")})
        nav.insert(3, {"label":"Webhook", "href":url_for("webhooks_page")})


    return render_template_string(r"""
<!doctype html>
<title>编辑规则 - Alarm2Ding</title>
<meta name="viewport" content="width=device-width,initial-scale=1">

{{ topbar('编辑规则', nav) }}

<div class="container" style="max-width:820px">
  <div class="card">
    <h3>编辑规则（每天可设多个时间段）</h3>
    <div class="muted" style="margin:-2px 0 12px">
      设备：<code>{{ device_id }}</code>　位置键：<code>{{ channel_key }}</code>　位置名：{{ channel_name or '' }}
    </div>

    <form method="post">
      <input type="hidden" name="device_id" value="{{ device_id }}">
      <input type="hidden" name="channel_key" value="{{ channel_key }}">

      {% set labels = ['周一','周二','周三','周四','周五','周六','周日'] %}
      {% for d in range(7) %}
        {% set is_all = (days_rules[d]|length==1) and (days_rules[d][0][0]==days_rules[d][0][1]) %}
        <fieldset style="border:1px solid var(--line);border-radius:10px;margin:12px 0;padding:12px">
          <legend style="font-weight:600;color:var(--primary)">{{ labels[d] }}</legend>

          <label style="display:inline-flex;align-items:center;gap:8px;margin:4px 0 6px">
            <input type="checkbox" id="day{{d}}_allday" name="day{{d}}_allday" value="1" {% if is_all %}checked{% endif %} onchange="toggleAllDay({{d}})">
            <span class="badge">全天</span>
          </label>

          <div id="day{{d}}_box" data-idx="{{ days_rules[d]|length }}" class="{% if is_all %}hide{% endif %}">
            {% for seg in days_rules[d] %}
              {% if not (days_rules[d]|length==1 and seg[0]==seg[1]) %}
                {% set i = loop.index0 %}
                <div class="row seg" style="display:flex;gap:8px;align-items:center;margin:6px 0">
                  <input name="day{{d}}_start_{{ i }}" class="inp" placeholder="HH:MM" value="{{ seg[0] }}" style="width:120px">
                  <span>~</span>
                  <input name="day{{d}}_end_{{ i }}" class="inp" placeholder="HH:MM" value="{{ seg[1] }}" style="width:120px">
                  <button type="button" class="btn" onclick="this.parentNode.remove()">删除</button>
                </div>
              {% endif %}
            {% endfor %}
          </div>

          <div style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap">
            <button type="button" class="btn" onclick="addRow({{d}})">+ 添加一段</button>
            <button type="button" class="btn" onclick="clearDay({{d}})">清空本日</button>
          </div>
        </fieldset>
      {% endfor %}

      <fieldset style="border:1px solid var(--line);border-radius:10px;margin:12px 0;padding:12px">
        <legend style="font-weight:600;color:var(--primary)">推送到哪些 Webhook</legend>
        <div style="display:flex;flex-wrap:wrap;gap:12px">
          {% for w in whs %}
            <label style="display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);border-radius:8px;padding:6px 8px">
              <input type="checkbox" name="wh_{{ w['id'] }}" value="1" {% if w['id'] in bound %}checked{% endif %}>
              <span>{{ w['name'] }}{% if not w['enabled'] %}（禁用）{% endif %}{% if w['is_default'] %}（默认）{% endif %}</span>
            </label>
          {% endfor %}
        </div>
        <div class="muted" style="margin-top:6px">若本通道未勾选任何 webhook，则退回使用“默认 webhook”。可在“Webhook 管理”页设置默认。</div>
      </fieldset>

      <div class="toolbar" style="display:flex;gap:10px;margin-top:14px;flex-wrap:wrap">
        <button type="submit" class="btn btn-primary">保存</button>
        <a class="btn" href="{{ back_url }}">返回</a>
      </div>
    </form>
  </div>
</div>

<style>
.hide{display:none}
</style>

<script>
function addRow(d){
  const box = document.getElementById('day'+d+'_box');
  const allday = document.getElementById('day'+d+'_allday').checked;
  if (allday){ alert('已勾选全天，需先取消“全天”再添加时段'); return; }
  const idx = parseInt(box.dataset.idx || '0');
  const html = '<div class="row seg" style="display:flex;gap:8px;align-items:center;margin:6px 0">'
             + '<input name="day'+d+'_start_'+idx+'" class="inp" placeholder="HH:MM" value="" style="width:120px">'
             + '<span>~</span>'
             + '<input name="day'+d+'_end_'+idx+'" class="inp" placeholder="HH:MM" value="" style="width:120px">'
             + '<button type="button" class="btn" onclick="this.parentNode.remove()">删除</button>'
             + '</div>';
  box.insertAdjacentHTML('beforeend', html);
  box.dataset.idx = (idx+1);
}
function clearDay(d){
  const box = document.getElementById('day'+d+'_box');
  box.innerHTML = '';
  box.dataset.idx = 0;
}
function toggleAllDay(d){
  const isAll = document.getElementById('day'+d+'_allday').checked;
  const box = document.getElementById('day'+d+'_box');
  if (isAll){
    clearDay(d);
    box.classList.add('hide');
  }else{
    box.classList.remove('hide');
  }
}
</script>
""",
        device_id=device_id, channel_key=channel_key,
        channel_name=(r["channel_name"] or ""),
        days_rules=days_rules,
        whs=whs,
        bound=bound,
        back_url=(url_for("devices") + f"?device_id={device_id}"),
        nav=nav
    )

# ---------- History ----------
@APP.get("/history")
@login_required
def history():
    from urllib.parse import urlencode

    q_device = (request.args.get("device_id") or "").strip()
    q_channel= (request.args.get("channel_key") or "").strip()
    q_type   = (request.args.get("type") or "").strip()
    q_fw     = (request.args.get("forwarded") or "").strip()
    q_from   = (request.args.get("from") or "").strip()
    q_to     = (request.args.get("to") or "").strip()
    page     = max(1, int(request.args.get("page") or "1"))
    size     = max(1, min(100, int(request.args.get("size") or "20")))
    off      = (page - 1) * size

    filters = {
        "device_id": q_device or None,
        "channel_key": q_channel or None,
        "type": q_type or None,
        "forwarded": q_fw if q_fw in ("0", "1") else None,
        "from": q_from or None,
        "to": q_to or None,
        "visible_uid": (None if session.get("is_admin") else int(session.get("uid")))
    }

    rows, total = query_messages(filters, size, off)
    pages = max(1, (total + size - 1) // size)

    if (request.args.get("export") or "").lower() == "csv":
        import csv, io
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["id","ts","device_id","channel_key","channel_name",
                    "type","type_name","box_name","device_name",
                    "score","image_url","forwarded","forward_reason"])
        for r in rows:
            w.writerow([
                r["id"], r["ts"], r["device_id"], r["channel_key"], r["channel_name"] or "",
                r["type"], r["type_name"] or "", r["box_name"] or "", r["device_name"] or "",
                r["score"] or "", r["image_url"] or "", r["forwarded"], r["forward_reason"] or "",
            ])
        resp = make_response(buf.getvalue())
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = f'attachment; filename="history_page{page}.csv"'
        return resp

    base_params = {}
    if q_device: base_params["device_id"] = q_device
    if q_channel: base_params["channel_key"] = q_channel
    if q_type:   base_params["type"] = q_type
    if q_fw in ("0","1"): base_params["forwarded"] = q_fw
    if q_from:   base_params["from"] = q_from
    if q_to:     base_params["to"] = q_to
    if size:     base_params["size"] = str(size)

    def build_history_url(extra: dict) -> str:
        params = base_params.copy()
        params.update(extra)
        qs = urlencode(params)
        return url_for("history") + (("?" + qs) if qs else "")

    export_url = build_history_url({"page": page, "export": "csv"})
    page_links = [{"p": p, "url": build_history_url({"page": p}), "cur": (p == page)}
                  for p in range(1, pages + 1)]

    devices_url = url_for("devices")
    logout_url  = url_for("logout")
    nav = [
        {"label":"历史记录", "href":url_for("history"), "active":True},
        {"label":"通道", "href":url_for("devices")},
        {"label":"退出", "href":url_for("logout")},
    ]
    if session.get("is_admin"):
        nav.insert(1, {"label":"维护", "href":url_for("maintenance_page")})
        nav.insert(2, {"label":"用户", "href":url_for("users_page")})
        nav.insert(3, {"label":"Webhook", "href":url_for("webhooks_page")})

    return render_template_string("""
<!doctype html>
<title>历史记录 - Alarm2Ding</title>
<meta name="viewport" content="width=device-width,initial-scale=1">

{{ topbar('历史记录', nav) }}

<div class="container">

  <form method="get" class="filter" style="margin-top:12px">
    <input name="device_id" class="inp" value="{{ request.args.get('device_id','') }}" placeholder="device_id">
    <input name="channel_key" class="inp" value="{{ request.args.get('channel_key','') }}" placeholder="channel_key(位置键)">
    <input name="type" class="inp" value="{{ request.args.get('type','') }}" placeholder="type">
    <select name="forwarded" class="inp">
      <option value="">转发=全部</option>
      <option value="1" {% if request.args.get('forwarded')=='1' %}selected{% endif %}>仅已转发</option>
      <option value="0" {% if request.args.get('forwarded')=='0' %}selected{% endif %}>仅未转发</option>
    </select>
    <input name="from" class="inp" value="{{ request.args.get('from','') }}" placeholder="从(YYYY-MM-DD HH:MM:SS)">
    <input name="to"   class="inp" value="{{ request.args.get('to','') }}"   placeholder="到(YYYY-MM-DD HH:MM:SS)">
    <input name="size" class="inp" value="{{ request.args.get('size','20') }}" placeholder="每页(1-100)">
    <button type="submit" class="btn">查询</button>
  </form>

  <div style="display:flex;gap:14px;align-items:center;margin:10px 0;flex-wrap:wrap">
    <a class="btn" href="{{ export_url }}">导出当前页 CSV</a>
    <form method="post" action="{{ delete_all_url }}" onsubmit="return confirm('确定要删除【当前筛选条件匹配的全部记录】吗？不可恢复！');">
      <input type="hidden" name="device_id" value="{{ request.args.get('device_id','') }}">
      <input type="hidden" name="channel_key" value="{{ request.args.get('channel_key','') }}">
      <input type="hidden" name="type" value="{{ request.args.get('type','') }}">
      <input type="hidden" name="forwarded" value="{{ request.args.get('forwarded','') }}">
      <input type="hidden" name="from" value="{{ request.args.get('from','') }}">
      <input type="hidden" name="to" value="{{ request.args.get('to','') }}">
      <button type="submit" class="btn btn-danger">按当前筛选全部删除</button>
    </form>
  </div>

  <form method="post" action="{{ delete_sel_url }}" onsubmit="return confirm('删除所选记录？不可恢复！');">
    <table class="table cardify">
      <thead><tr>
        <th style="width:28px"><input type="checkbox" id="chk_all" onclick="toggleAll()"></th>
        <th>ID</th><th>时间</th><th>设备</th><th>位置键</th><th>位置名</th>
        <th>算法</th><th>位置</th><th>score</th><th>图片</th><th>状态</th>
      </tr></thead>
      <tbody>
        {% for r in rows %}
        {% set ok = (r['forwarded']==1) %}
        <tr>
          <td data-label="选"><input type="checkbox" name="ids" value="{{ r['id'] }}"></td>
          <td data-label="ID">{{ r['id'] }}</td>
          <td data-label="时间">{{ r['ts'] }}</td>
          <td data-label="设备"><code>{{ r['device_id'] }}</code></td>
          <td data-label="位置键"><code>{{ r['channel_key'] }}</code></td>
          <td data-label="位置名">{{ r['channel_name'] or '' }}</td>
          <td data-label="算法">{{ r['type_name'] }} ({{ r['type'] }})</td>
          <td data-label="位置">{{ r['box_name'] or '' }} / {{ r['device_name'] or '' }}</td>
          <td data-label="score">{{ r['score'] or '' }}</td>
          <td data-label="图片">
            {% if r['image_url'] %}
              <a href="{{ r['image_url'] }}" target="_blank" rel="noopener noreferrer">原图</a>
              {% set pv = preview_from_url(r['image_url']) %}
              {% if pv %} · <a href="{{ pv }}" target="_blank" rel="noopener noreferrer">预览</a>{% endif %}
            {% endif %}
          </td>
          <td data-label="状态">
            {% if ok %}
              <span class="badge badge-ok">已转发</span>
            {% else %}
              {% set rsn = (r['forward_reason'] or '未转发') %}
              {% if '异常' in rsn %}
                <span class="badge badge-err">{{ rsn }}</span>
              {% elif '非时间段' in rsn or '禁用' in rsn %}
                <span class="badge badge-warn">{{ rsn }}</span>
              {% else %}
                <span class="badge">{{ rsn }}</span>
              {% endif %}
            {% endif %}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    <div style="margin-top:8px">
      <button type="submit" class="btn btn-danger">删除所选</button>
    </div>
  </form>

  <div class="pager" style="margin-top:12px">
    {% for it in page_links %}
      {% if it.cur %}
        <b>[{{ it.p }}]</b>
      {% else %}
        <a href="{{ it.url }}">[{{ it.p }}]</a>
      {% endif %}
    {% endfor %}
    <span class="muted" style="margin-left:12px">共 {{ total }} 条</span>
  </div>
</div>
<script>
function toggleAll(){
  const c = document.getElementById('chk_all').checked;
  document.querySelectorAll('input[name="ids"]').forEach(x => x.checked = c);
}
</script>
""",
        rows=rows, total=total, export_url=export_url, page_links=page_links,
        delete_sel_url=url_for("history_delete_selected"),
        delete_all_url=url_for("history_delete_all"),
        devices_url=devices_url, logout_url=logout_url, 
        nav=nav
    )

@APP.post("/history/delete")
@login_required
def history_delete_selected():
    ids = [x for x in request.form.getlist("ids") if x.isdigit()]
    if not ids:
        return redirect(url_for("history"))
    ids = [int(x) for x in ids]

    if session.get("is_admin"):
        rels = _fetch_rels_by_ids(ids)
        n = delete_messages_by_ids(ids)
        for rel in set(rels):
            _delete_snap_if_orphan(rel)
        LOG.info("history: admin deleted %s rows", n)
        return redirect(url_for("history"))

    vset = user_visible_pairs(int(session.get("uid")))
    if not vset:
        return redirect(url_for("history"))

    qmarks = ",".join("?" for _ in ids)
    conn = _db()
    try:
        rows = conn.execute(f"SELECT id, device_id, channel_key FROM messages WHERE id IN ({qmarks})", ids).fetchall()
    finally:
        conn.close()
    allowed_ids = [int(r["id"]) for r in rows if (r["device_id"], r["channel_key"]) in vset]
    rels = _fetch_rels_by_ids(allowed_ids)
    n = delete_messages_by_ids(allowed_ids)
    for rel in set(rels):
        _delete_snap_if_orphan(rel)
    LOG.info("history: user %s deleted %s rows (filtered from %s)", session.get("uid"), n, len(ids))
    return redirect(url_for("history"))

def _count_messages_by_filters(filters: Dict[str, Any]) -> int:
    wh, args = [], []
    if filters.get("device_id"): wh.append("device_id = ?"); args.append(filters["device_id"])
    if filters.get("channel_key"): wh.append("channel_key = ?"); args.append(filters["channel_key"])
    if filters.get("type") is not None and filters["type"] != "": wh.append("type = ?"); args.append(int(filters["type"]))
    if filters.get("forwarded") in ("0","1"): wh.append("forwarded = ?"); args.append(int(filters["forwarded"]))
    if filters.get("from"): wh.append("ts >= ?"); args.append(filters["from"])
    if filters.get("to"):   wh.append("ts <= ?"); args.append(filters["to"])
    if filters.get("visible_uid") is not None:
        wh.append("""EXISTS (
            SELECT 1 FROM user_channels uc
            WHERE uc.user_id=?
              AND uc.device_id = messages.device_id
              AND uc.channel_key = messages.channel_key
        )""")
        args.append(int(filters["visible_uid"]))
    where = ("WHERE " + " AND ".join(wh)) if wh else ""
    conn = _db()
    try:
        r = conn.execute(f"SELECT COUNT(1) AS c FROM messages {where}", args).fetchone()
        return int(r["c"]) if r else 0
    finally:
        conn.close()

def _fetch_rels_by_filters(filters: Dict[str, Any], max_collect: int) -> List[str]:
    """在删除前抓取将被删除的 image_url（规模过大时不要用）"""
    wh, args = [], []
    if filters.get("device_id"): wh.append("device_id = ?"); args.append(filters["device_id"])
    if filters.get("channel_key"): wh.append("channel_key = ?"); args.append(filters["channel_key"])
    if filters.get("type") is not None and filters["type"] != "": wh.append("type = ?"); args.append(int(filters["type"]))
    if filters.get("forwarded") in ("0","1"): wh.append("forwarded = ?"); args.append(int(filters["forwarded"]))
    if filters.get("from"): wh.append("ts >= ?"); args.append(filters["from"])
    if filters.get("to"):   wh.append("ts <= ?"); args.append(filters["to"])
    if filters.get("visible_uid") is not None:
        wh.append("""EXISTS (
            SELECT 1 FROM user_channels uc
            WHERE uc.user_id=?
              AND uc.device_id = messages.device_id
              AND uc.channel_key = messages.channel_key
        )""")
        args.append(int(filters["visible_uid"]))
    where = ("WHERE " + " AND ".join(wh)) if wh else ""

    rels: List[str] = []
    conn = _db()
    try:
        cur = conn.execute(f"SELECT image_url FROM messages {where}", args)
        for r in cur:
            if max_collect > 0 and len(rels) >= max_collect:
                break
            rel = _snap_rel_from_url(r["image_url"] or "")
            if rel:
                rels.append(rel)
    finally:
        conn.close()
    return rels

@APP.post("/history/delete_all")
@login_required
def history_delete_all():
    filters = {
        "device_id": (request.form.get("device_id") or "").strip() or None,
        "channel_key": (request.form.get("channel_key") or "").strip() or None,
        "type": (request.form.get("type") or "").strip() or None,
        "forwarded": (request.form.get("forwarded") or "").strip() or None,
        "from": (request.form.get("from") or "").strip() or None,
        "to": (request.form.get("to") or "").strip() or None,
    }
    if not session.get("is_admin"):
        filters["visible_uid"] = int(session.get("uid"))

    # 最稳：小规模时删记录并尽量删图；大规模时删记录后跑 reconcile（避免内存爆）
    total = _count_messages_by_filters(filters)
    rels = []
    if total <= 5000:
        rels = _fetch_rels_by_filters(filters, max_collect=10000)

    n = delete_messages_by_filters(filters)
    LOG.info("history: deleted by filters %s rows (total=%s)", n, total)

    if rels:
        for rel in set(rels):
            _delete_snap_if_orphan(rel)
    else:
        # 大规模删除：用对账清理孤儿图、坏记录
        reconcile_db_and_snaps()

    return redirect(url_for("history"))

# ---------------- Cleanup (daily) ----------------
def _clean_old_snaps_once():
    if SNAP_RETAIN_DAYS == 0 and SNAP_MAX_GB <= 0:
        LOG.info("clean: disabled (SNAP_RETAIN_DAYS=0 & SNAP_MAX_GB<=0)")
        return
    root = Path(APP.static_folder) / "snaps"
    if not root.exists():
        return

    # 1) 按天清理（先删 DB，再删目录）
    if SNAP_RETAIN_DAYS > 0:
        cutoff = (datetime.now() - timedelta(days=SNAP_RETAIN_DAYS)).strftime("%Y%m%d")
        removed_dirs = 0
        for sub in sorted(root.iterdir()):
            if not sub.is_dir():
                continue
            name = sub.name
            if name.isdigit() and len(name) == 8 and name < cutoff:
                try:
                    # 先删 DB 引用该天的记录，避免“坏记录”
                    conn = _db()
                    try:
                        cur = conn.execute("DELETE FROM messages WHERE image_url LIKE ?", (f"%/snaps/{name}/%",))
                        conn.commit()
                        if (cur.rowcount or 0) > 0:
                            LOG.info("clean: removed db rows for day %s: %s", name, cur.rowcount)
                    finally:
                        conn.close()

                    shutil.rmtree(sub, ignore_errors=True)
                    removed_dirs += 1
                except Exception as e:
                    LOG.warning("clean: rm dir %s fail: %s", sub, e)
        LOG.info("clean: removed old day dirs=%s (cutoff=%s)", removed_dirs, cutoff)

    # 2) 容量兜底（删文件后也删 DB 引用）
    if SNAP_MAX_GB > 0:
        files, total = [], 0
        for p in root.rglob("*.jpg"):
            try:
                st = p.stat()
                sz = st.st_size
                total += sz
                files.append((p, st.st_mtime, sz))
            except Exception:
                pass
        limit = int(SNAP_MAX_GB * 1024 * 1024 * 1024)
        if total > limit:
            files.sort(key=lambda x: x[1])
            freed = 0
            for p, _, sz in files:
                try:
                    rel = f"snaps/{p.parent.name}/{p.name}"
                    p.unlink(missing_ok=True)
                    freed += sz
                    _deleted = _delete_db_rows_by_rel(rel)
                    if _deleted:
                        LOG.info("clean: removed db rows for %s: %s", rel, _deleted)
                    if total - freed <= limit:
                        break
                except Exception:
                    pass
            LOG.info("clean: total=%s > limit=%s, freed=%s", total, limit, freed)

def _schedule_daily_cleanup():
    import threading, time as _t
    hh, mm = (CLEAN_AT or "03:10").split(":")
    hh, mm = int(hh), int(mm)

    def _worker():
        while True:
            now = datetime.now()
            tgt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if tgt <= now:
                tgt += timedelta(days=1)
            wait = (tgt - now).total_seconds()
            LOG.info("clean: next at %s (%.0fs later)", tgt, wait)
            _t.sleep(wait)
            try:
                _clean_old_snaps_once()
                _db_rotate_once(vacuum=True)
                if RECONCILE_DAILY:
                    reconcile_db_and_snaps()
            except Exception as e:
                LOG.error("clean: run error %s", e)

    threading.Thread(target=_worker, daemon=True).start()

@APP.get("/view/<day>/<fname>")
def view_snap(day: str, fname: str):
    if (not day.isdigit()) or (len(day) != 8) or ("/" in fname) or (".." in fname):
        abort(404)
    local = Path(APP.static_folder) / "snaps" / day / fname
    if not local.exists():
        abort(404)

    if IMAGE_PUBLIC_BASE:
        img_src = f"{IMAGE_PUBLIC_BASE}/snaps/{day}/{fname}"
    else:
        img_src = url_for("static", filename=f"snaps/{day}/{fname}", _external=True)

    html = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="format-detection" content="telephone=no,email=no">
<title>预览</title>
<style data-keep>
:root{{ --bg:#0b1220; --card:#0f172a; --text:#e5e7eb; --line:#1f2937; }}
@media (prefers-color-scheme: light){{
  :root{{ --bg:#f6f8fb; --card:#ffffff; --text:#1f2937; --line:#e5e7eb; }}
}}
html,body{{height:100%;margin:0;-webkit-text-size-adjust:100%;background:var(--bg);color:var(--text);font:15px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial}}
.topbar{{position:sticky;top:0;background:var(--bg);border-bottom:1px solid var(--line);padding:10px 14px}}
.topbar a{{color:inherit;text-decoration:none;opacity:.8}}
.wrap{{height:calc(100% - 52px);display:flex;align-items:center;justify-content:center;padding:10px}}
.pic{{max-width:100%;max-height:100%;object-fit:contain;transition:transform .15s ease;touch-action:manipulation;}}
.zoom .pic{{max-width:none;max-height:none;transform:scale(1.1)}}
.tip{{opacity:.7;font-size:12px;text-align:center;padding:6px 0}}
</style>
<div class="topbar"><a href="{img_src}" target="_blank">查看原图</a></div>
<div id="wrap" class="wrap">
  <img id="pic" class="pic" src="{img_src}" alt="snap">
</div>
<div class="tip">轻触切换放大/缩小；原图在右上角</div>
<script>
  var zoom=false, wrap=document.getElementById('wrap');
  wrap.addEventListener('click', function() {{
    zoom=!zoom; document.body.classList.toggle('zoom', zoom);
  }});
</script>
"""
    return render_template_string(html)

# ---------------- MQTT (optional) ----------------
def _run_mqtt_if_configured():
    if not MQTT_BROKER_HOST:
        return
    import threading
    import paho.mqtt.client as mqtt

    def _on_connect(client, userdata, flags, rc, properties=None):
        LOG.info("[mqtt] connected rc=%s, sub %s", rc, MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC, qos=1)

    def _on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8", "ignore"))
        except Exception:
            return
        try:
            _handle_record_and_forward(payload, echo=False)
        except Exception as e:
            LOG.error("mqtt handle fail: %s", e)

    def _worker():
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.on_connect = _on_connect
        client.on_message = _on_message
        client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
        client.loop_forever()

    threading.Thread(target=_worker, daemon=True).start()

# ---------------- Main ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=11888)
    parser.add_argument("--reconcile-now", action="store_true", help="启动后立即对账一次")
    parser.add_argument("--vacuum-now", action="store_true", help="启动后立即 VACUUM（谨慎）")
    args = parser.parse_args()

    Path(APP.static_folder, "snaps").mkdir(parents=True, exist_ok=True)
    init_db()
    ensure_migrations()
    migrate_legacy_channel_rules_once()
    _run_mqtt_if_configured()
    _schedule_daily_cleanup()

    if args.vacuum_now:
        _vacuum_db_safely()
    if args.reconcile_now:
        reconcile_db_and_snaps()

    APP.run(host=args.host, port=args.port, threaded=True)

if __name__ == "__main__":
    main()
