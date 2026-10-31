import os, json, time, threading, requests, pytz, secrets
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from flask import Flask, request, jsonify, send_from_directory
from datetime import datetime, timedelta
import re

app = Flask(__name__, static_folder='static')
VERSION = "8.0"

TEHRAN = pytz.timezone("Asia/Tehran")

# ==================== متغیرهای محیطی ====================
ALERTS_FILE = "alerts.json"
FIRED_BACKUP_FILE = "fired_backup.json"  # لایه‌ی دفاعی دوم — جلوگیری از فایر مجدد بعد از ری‌استارت اگه Supabase قطع بود
BOT_TOKEN_ENV = os.environ.get("BOT_TOKEN", "")
YOUR_CHAT_ID = "109419675"
BROADCAST_MODE = os.environ.get("BROADCAST_MODE", "false").lower() == "true"

_cache_alerts = None
# قفل محافظ کش آلارم‌ها — جلوگیری از race condition وقتی چند thread هم‌زمان
# (مثلاً check_alerts و add_alert) با فاصله‌ی کم روی _cache_alerts کار می‌کنن
_alerts_cache_lock = threading.RLock()

def now_teh():
    return datetime.now(TEHRAN).strftime("%Y-%m-%d %H:%M:%S")

def now_pretty():
    return now_teh()  # alias برای سازگاری

def get_pip_multiplier(symbol):
    sym_up = symbol.upper()
    crypto_list = ['BTC','ETH','SOL','BNB','XRP','ADA','DOGE','TRX','TON','AVAX','MATIC','DOT','LINK','UNI','ATOM','LTC','SHIB','OP','ARB','NEAR','FTM','SAND','MANA']
    if any(x in sym_up for x in crypto_list):
        return 1
    if "XAU" in sym_up or "XAG" in sym_up:
        return 10
    if "JPY" in sym_up:
        return 100
    return 10000

def is_crypto_symbol(sym):
    sym_up = sym.upper()
    crypto_list = ['BTC','ETH','SOL','BNB','XRP','ADA','DOGE','TRX','TON','AVAX','MATIC','DOT','LINK','UNI','ATOM','LTC','SHIB','OP','ARB','NEAR','FTM','SAND','MANA']
    return any(c in sym_up for c in crypto_list)

def _nearby_pip_threshold(symbol: str) -> object:
    """آستانه نزدیکی برای پیشنهاد ستاره — فقط وب. طلا ۵۰، جفت‌ارز ۳۰، کریپتو None (چک نمی‌شه)."""
    sym_up = (symbol or "").upper()
    if is_crypto_symbol(sym_up):
        return None
    if "XAU" in sym_up or "XAG" in sym_up:
        return 50
    return 30

def _price_distance_pips(symbol: str, p1: float, p2: float) -> float:
    try:
        return abs(float(p1) - float(p2)) * get_pip_multiplier(symbol)
    except Exception:
        return 1e18

def _find_nearby_team_alerts(symbol: str, target: float, condition: str, exclude_name: str = "") -> list:
    """آلارم‌های تیمی فعال هم‌نماد هم‌جهت در فاصله آستانه."""
    thr = _nearby_pip_threshold(symbol)
    if thr is None:
        return []
    data = load_alerts()
    out = []
    excl = (exclude_name or "").strip().lower()
    for a in data.get("alerts", []):
        if not a.get("active"):
            continue
        if a.get("is_private"):
            continue
        if (a.get("symbol") or "").upper() != (symbol or "").upper():
            continue
        if (a.get("condition") or "above") != (condition or "above"):
            continue
        if excl and (a.get("created_by") or "").strip().lower() == excl:
            continue
        dist = _price_distance_pips(symbol, target, a.get("target_price") or 0)
        if dist <= thr:
            out.append({
                "id": a.get("id"),
                "symbol": a.get("symbol"),
                "target_price": a.get("target_price"),
                "condition": a.get("condition"),
                "created_by": a.get("created_by") or "—",
                "created_at": a.get("created_at") or "",
                "distance_pips": round(dist, 1),
                "direction": "بالاتر" if float(a.get("target_price") or 0) > float(target) else (
                    "پایین‌تر" if float(a.get("target_price") or 0) < float(target) else "هم‌قیمت"
                ),
                "supporters": a.get("supporters") or [],
                "threshold": thr,
            })
    out.sort(key=lambda x: x["distance_pips"])
    return out[:5]

def _add_supporter_to_alert(alert_id: str, supporter_name: str) -> object:
    """ستاره/تأیید روی آلارم موجود — برمی‌گرداند آلارم آپدیت‌شده یا None"""
    name = (supporter_name or "").strip()
    if not name or not alert_id:
        return None
    with _alerts_cache_lock:
        data = load_alerts()
        target = None
        for a in data.get("alerts", []):
            if str(a.get("id")) == str(alert_id) and a.get("active") and not a.get("is_private"):
                target = a
                break
        if not target:
            return None
        sups = list(target.get("supporters") or [])
        if any((s.get("name") or "").strip().lower() == name.lower() for s in sups if isinstance(s, dict)):
            return target  # قبلاً تأیید کرده
        if (target.get("created_by") or "").strip().lower() == name.lower():
            return target  # سازنده خودش
        sups.append({"name": name, "at": now_teh()})
        target["supporters"] = sups
        save_alerts(data)
    # persist
    try:
        _sb_upsert_alert(target)
    except Exception as e:
        print(f"[support] upsert exc: {e}")
    return target

def _empty_alerts():
    return {
        "alerts": [], "archive": [], "telegram": {"bot_token": "", "chat_ids": []},
        "users": [], "errors": [], "last_update": None
    }

def fix_alerts(data):
    e = _empty_alerts()
    for k in e:
        if k not in data:
            data[k] = e[k]
    return data

# =====================================================================
# Supabase — جدول alerts (هر آلارم یه ردیف + یه ردیف config با id='__config__')
# =====================================================================

def _sb_upsert_alert(a):
    """یه آلارم رو upsert کن"""
    if not SUPABASE_KEY: return
    try:
        # فیلدهای پایه که حتماً توی جدول هستن
        payload = {
            "id":           a["id"],
            "symbol":       a.get("symbol",""),
            "type":         a.get("type","forex"),
            "condition":    a.get("condition","above"),
            "target_price": float(a.get("target_price",0)),
            "status":       "expired" if a.get("expired_at") else ("fired" if a.get("fired_at") else ("active" if a.get("active") else "cancelled")),
            "created_by":   a.get("created_by",""),
            "created_at":   a.get("created_at", now_teh()),
            "comment":      a.get("comment",""),
            "is_private":   bool(a.get("is_private", False)),
            "notify_only":  str(a.get("notify_only","")) if a.get("notify_only") else None,
            "active":       bool(a.get("active", True)),
            "last_price":   float(a["last_price"]) if a.get("last_price") is not None else None,
            "last_checked": a.get("last_checked"),
            "fired_at":     a.get("fired_at"),
            "fired_price":  float(a["fired_price"]) if a.get("fired_price") is not None else None,
            "instant":      bool(a.get("instant", False)),
        }
        # فیلدهای اختیاری — فقط اگه توی جدول وجود داشت اضافه میشن
        if a.get("tag"):
            payload["tag"] = a["tag"]
        if a.get("private_cid") is not None:
            payload["private_cid"] = str(a["private_cid"]) if a["private_cid"] else None
        if a.get("expires_at") is not None:
            payload["expires_at"] = a["expires_at"]
        if a.get("expired_at") is not None:
            payload["expired_at"] = a["expired_at"]
        if a.get("supporters") is not None:
            payload["supporters"] = a.get("supporters") or []

        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/alerts",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=10)
        if r.status_code not in (200,201,204):
            err_text = r.text
            # اگه ستون پیدا نشد، بدون اون فیلد دوباره تلاش کن
            if r.status_code == 400 and "PGRST204" in err_text:
                import re as _re
                missing = _re.search(r"Could not find the '([^']+)'", err_text)
                if missing:
                    col = missing.group(1)
                    print(f"[alerts] ستون '{col}' توی DB نیست — بدون اون retry میکنیم")
                    payload.pop(col, None)
                    r2 = requests.post(
                        f"{SUPABASE_URL}/rest/v1/alerts",
                        headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
                        json=payload, timeout=10)
                    if r2.status_code not in (200,201,204):
                        print(f"[alerts] upsert {a['id']} retry failed: {r2.status_code} {r2.text[:120]}")
                    return
            print(f"[alerts] upsert {a['id']}: {r.status_code} {r.text[:120]}")
    except Exception as e:
        print(f"[alerts] upsert error: {e}")

def _sb_upsert_config(tg, users, errors):
    """config (token + chat_ids + users) رو توی یه ردیف ثابت ذخیره کن"""
    if not SUPABASE_KEY: return
    try:
        payload = {
            "id": "__config__",
            "symbol": "__config__",
            "type": "config",
            "condition": "none",
            "target_price": 0,
            "telegram_token": tg.get("bot_token",""),
            "chat_ids": tg.get("chat_ids", []),
            "users": users,
            "active": False,
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/alerts",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=10)
        if r.status_code not in (200,201,204):
            print(f"[alerts] config save: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[alerts] config save error: {e}")

def _sb_load_gold_bot_subscribers() -> list:
    """chat_id هایی که رو ربات جدید طلا /start زدن"""
    if not SUPABASE_KEY:
        return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__&select=gold_bot_subscribers",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            rows = r.json()
            if rows and rows[0].get("gold_bot_subscribers") is not None:
                val = rows[0]["gold_bot_subscribers"]
                return val if isinstance(val, list) else json.loads(val)
    except Exception as e:
        print(f"[goldbot] load subscribers exc: {e}")
    return []

def _sb_save_gold_bot_subscribers(ids: list):
    if not SUPABASE_KEY:
        return
    try:
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json={"gold_bot_subscribers": ids}, timeout=8)
    except Exception as e:
        print(f"[goldbot] save subscribers exc: {e}")


def _sb_load_muted_instant_senders() -> list:
    """اسم‌هایی که آلارم فوریشون ذخیره میشه ولی به تلگرام ارسال نمیشه — پیش‌فرض لیست خالی"""
    if not SUPABASE_KEY:
        return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__&select=muted_instant_senders",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            rows = r.json()
            if rows and rows[0].get("muted_instant_senders") is not None:
                val = rows[0]["muted_instant_senders"]
                return val if isinstance(val, list) else json.loads(val)
    except Exception as e:
        print(f"[assign] load muted_instant_senders exc: {e}")
    return []

def _sb_save_muted_instant_senders(names: list):
    """فقط همین یک فیلد رو patch می‌کنه"""
    if not SUPABASE_KEY:
        return
    try:
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json={"muted_instant_senders": names}, timeout=8)
    except Exception as e:
        print(f"[assign] save muted_instant_senders exc: {e}")

def _sb_load_deprioritize_masoud() -> bool:
    """می‌خونه آیا اولویت پایین مسعود فعاله یا نه — پیش‌فرض True اگه چیزی پیدا نشد"""
    if not SUPABASE_KEY:
        return True
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__&select=deprioritize_masoud",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            rows = r.json()
            if rows and rows[0].get("deprioritize_masoud") is not None:
                return bool(rows[0]["deprioritize_masoud"])
    except Exception as e:
        print(f"[assign] load deprioritize_masoud exc: {e}")
    return True

def _sb_save_deprioritize_masoud(value: bool):
    """فقط همین یک فیلد رو patch می‌کنه — با upsert_config تداخل نداره"""
    if not SUPABASE_KEY:
        return
    try:
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json={"deprioritize_masoud": value}, timeout=8)
    except Exception as e:
        print(f"[assign] save deprioritize_masoud exc: {e}")

def _sb_load_news_reminder_state() -> dict:
    """وضعیت هشدار ۱۵ دقیقه‌ای اخبار قرمز رو می‌خونه — {date, events, sent} —
    نیازمند یه ستون jsonb به اسم news_reminder_state روی ردیف __config__ جدول alerts.
    اگه ستون هنوز ساخته نشده یا خالیه، دیکشنری خالی برمی‌گردونه (fallback به حافظه‌ی موقت)."""
    if not SUPABASE_KEY:
        return {}
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__&select=news_reminder_state",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            rows = r.json()
            if rows and rows[0].get("news_reminder_state") is not None:
                val = rows[0]["news_reminder_state"]
                return val if isinstance(val, dict) else json.loads(val)
    except Exception as e:
        print(f"[news] load news_reminder_state exc: {e}")
    return {}

def _sb_save_news_reminder_state(state: dict):
    """فقط همین یک فیلد رو patch می‌کنه — با upsert_config تداخل نداره"""
    if not SUPABASE_KEY:
        return
    try:
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json={"news_reminder_state": state}, timeout=8)
    except Exception as e:
        print(f"[news] save news_reminder_state exc: {e}")

def _sb_load_unavailable_members() -> list:
    """اسم اعضایی که موقتاً از چرخه‌ی تقسیم آلارم کنار گذاشته شدن (مرخصی/غیره) —
    پیش‌فرض لیست خالی (یعنی همه در دسترسن) اگه چیزی پیدا نشد. نیازمند یه ستون
    jsonb به اسم unavailable_members روی ردیف __config__ جدول alerts."""
    if not SUPABASE_KEY:
        return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__&select=unavailable_members",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            rows = r.json()
            if rows and rows[0].get("unavailable_members") is not None:
                val = rows[0]["unavailable_members"]
                return val if isinstance(val, list) else json.loads(val)
    except Exception as e:
        print(f"[assign] load unavailable_members exc: {e}")
    return []

def _sb_save_unavailable_members(members: list):
    """فقط همین یک فیلد رو patch می‌کنه — با upsert_config تداخل نداره"""
    if not SUPABASE_KEY:
        return
    try:
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json={"unavailable_members": members}, timeout=8)
    except Exception as e:
        print(f"[assign] save unavailable_members exc: {e}")

def _sb_load_all_alerts():
    """همه ردیف‌های جدول alerts رو بخون و به فرمت داخلی تبدیل کن"""
    if not SUPABASE_KEY: return None
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?select=*&limit=2000",
            headers=_sb_h(), timeout=10)
        if r.status_code != 200:
            print(f"[alerts] load failed: {r.status_code} {r.text[:80]}")
            return None
        rows = r.json()
        if not rows:
            print("[alerts] Supabase: جدول خالیه")
            return None

        config_row = next((x for x in rows if x["id"] == "__config__"), None)
        tg = {"bot_token": "", "chat_ids": []}
        users = []
        if config_row:
            tg["bot_token"] = config_row.get("telegram_token","") or ""
            raw_cids = config_row.get("chat_ids") or []
            tg["chat_ids"] = raw_cids if isinstance(raw_cids, list) else json.loads(raw_cids)
            raw_users = config_row.get("users") or []
            users = raw_users if isinstance(raw_users, list) else json.loads(raw_users)

        alerts = []
        archive = []
        for row in rows:
            if row["id"] == "__config__": continue
            a = {
                "id":           row["id"],
                "symbol":       row.get("symbol",""),
                "type":         row.get("type","forex"),
                "condition":    row.get("condition","above"),
                "target_price": row.get("target_price",0),
                "created_by":   row.get("created_by",""),
                "created_at":   row.get("created_at",""),
                "comment":      row.get("comment",""),
                "is_private":   row.get("is_private", False),
                "private_cid":  row.get("private_cid") or row.get("notify_only"),
                "notify_only":  row.get("notify_only"),
                "active":       row.get("active", True),
                "last_price":   row.get("last_price"),
                "last_checked": row.get("last_checked"),
                "fired_at":     row.get("fired_at"),
                "fired_price":  row.get("fired_price"),
                "supporters":   row.get("supporters") or [],
            }
            status = row.get("status","active")
            if status == "fired" or row.get("fired_at"):
                archive.append(a)
            elif status == "active" and row.get("active"):
                alerts.append(a)

        data = {"alerts": alerts, "archive": archive, "telegram": tg,
                "users": users, "errors": [], "last_update": now_teh()}
        print(f"[alerts] Loaded from Supabase — {len(alerts)} active, {len(archive)} archived")
        return data
    except Exception as e:
        print(f"[alerts] load error: {e}")
        return None



def load_alerts():
    global _cache_alerts
    with _alerts_cache_lock:
        if _cache_alerts is not None:
            return _cache_alerts
        # 1. Supabase
        d = _sb_load_all_alerts()
        if d is not None:
            _cache_alerts = d
            return _cache_alerts
        # 2. local fallback
        if os.path.exists(ALERTS_FILE):
            try:
                with open(ALERTS_FILE, "r", encoding="utf-8") as f:
                    _cache_alerts = fix_alerts(json.load(f))
                    return _cache_alerts
            except: pass
        _cache_alerts = _empty_alerts()
        return _cache_alerts

def _sb_delete_alert(aid):
    """یه آلارم رو از Supabase حذف کن"""
    _deleted_ids.add(str(aid))  # فوری به blacklist اضافه کن
    if not SUPABASE_KEY: return
    try:
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.{aid}",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            timeout=8)
        print(f"[alerts] delete {aid}: status={r.status_code} body={r.text[:80]}")
    except Exception as e:
        print(f"[alerts] delete error: {e}")

def save_alerts(data):
    """cache رو آپدیت کن + local backup — Supabase رو در background بزن"""
    global _cache_alerts
    with _alerts_cache_lock:
        _cache_alerts = data
    # local backup سریع
    try:
        with open(ALERTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[alerts] local backup error: {e}")
    # Supabase در background — بلاک نمیشه
    if SUPABASE_KEY:
        snapshot = (
            data.get("telegram",{}),
            data.get("users",[]),
            data.get("errors",[]),
            list(data.get("alerts",[]))
        )
        def _bg(snap=snapshot):
            tg, users, errors, alerts = snap
            _sb_upsert_config(tg, users, errors)
            for a in alerts:
                _sb_upsert_alert(a)
        threading.Thread(target=_bg, daemon=True).start()

def save_alert_expired(a):
    """
    وقتی آلارم منقضی می‌شه (تاریخ انقضاش رسیده و فایر نشده) — به‌جای حذف کامل
    از دیتابیس، فقط با status='expired' علامت‌گذاری می‌شه (دقیقاً مثل fired،
    ولی با یه وضعیت جدا). یعنی رکوردش برای همیشه تو Supabase می‌مونه و با SQL
    قابل خروجی گرفتنه، فقط دیگه تو لیست «آلارم‌های فعال» و چک قیمت نیست.
    """
    global _cache_alerts
    a["expired_at"] = now_teh()
    a["active"] = False
    ok = _sb_upsert_alert_retry(a, max_tries=3)
    if not ok:
        print(f"[alerts] ⚠️ expired state برای {a.get('id')} تو Supabase ذخیره نشد!")
    with _alerts_cache_lock:
        if _cache_alerts is not None:
            _cache_alerts["alerts"] = [x for x in _cache_alerts.get("alerts", []) if x.get("id") != a.get("id")]

def save_alert_fired(a):
    """
    وقتی آلارم fire میشه — ردیف رو آپدیت کن و cache رو پاک کن تا archive درست لود بشه.
    این ذخیره‌سازی حیاتیه: اگه fail بشه و سرور همون لحظه ری‌استارت/دیپلوی بشه،
    آلارم دوباره fired_at ندار می‌مونه و بعد از بالا اومدن دوباره فایر می‌شه.
    برای همین با چند تلاش (retry) کار می‌کنیم و هم‌زمان یه بک‌آپ محلی فوری هم می‌زنیم.
    """
    global _cache_alerts
    ok = _sb_upsert_alert_retry(a, max_tries=3)
    if not ok:
        print(f"[alerts] ⚠️ CRITICAL: fired state برای {a.get('id')} بعد از چند تلاش تو Supabase ذخیره نشد!")
    # بک‌آپ محلی فوری و مستقل از Supabase — حتی اگه Supabase کامل قطع باشه
    try:
        _local_mark_fired_backup(a)
    except Exception as e:
        print(f"[alerts] local fired backup error: {e}")
    # به‌جای پاک کردن کامل کش (که باعث یه fetch کامل و پرهزینه‌ی بعدی از
    # Supabase می‌شد — شامل کل آرشیو تاریخی، و از هر جای برنامه‌ای که بعدش
    # load_alerts() صدا بزنه، حتی poll_telegram که تقریباً پیوسته در حال اجراست)،
    # مستقیم خودِ کش حافظه رو اصلاح می‌کنیم: این آلارم از فعال به آرشیو منتقل
    # می‌شه، بدون این‌که نیاز به خوندن دوباره از Supabase باشه.
    with _alerts_cache_lock:
        if _cache_alerts is not None:
            _cache_alerts["alerts"] = [x for x in _cache_alerts.get("alerts", []) if x.get("id") != a.get("id")]
            _cache_alerts.setdefault("archive", []).append(a)

def _sb_upsert_alert_retry(a, max_tries=3):
    """
    _sb_upsert_alert رو با چند تلاش صدا بزن و مطمئن شو fired_at واقعاً ثبت شده —
    با یه GET تأیید می‌کنیم، نه فقط status code ارسال.
    """
    if not SUPABASE_KEY:
        return True  # بدون Supabase چیزی برای تایید نیست، local backup کافیه
    for attempt in range(1, max_tries + 1):
        _sb_upsert_alert(a)
        try:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/alerts?id=eq.{a['id']}&select=fired_at,active",
                headers=_sb_h(), timeout=8)
            if r.status_code == 200:
                rows = r.json()
                if rows and rows[0].get("fired_at"):
                    return True
        except Exception as e:
            print(f"[alerts] verify fired attempt {attempt} exc: {e}")
        print(f"[alerts] fired upsert تلاش {attempt}/{max_tries} تایید نشد — دوباره امتحان می‌کنیم")
        time.sleep(0.5)
    return False

def _local_mark_fired_backup(a):
    """
    یه فایل کوچیک محلی جدا از alerts.json که فقط آیدی آلارم‌های fired رو نگه می‌داره —
    حتی اگه Supabase کامل قطع باشه، بعد از ری‌استارت این فایل خونده می‌شه تا
    از فایر مجدد جلوگیری بشه (لایه‌ی دفاعی دوم).
    """
    fired_ids = set()
    if os.path.exists(FIRED_BACKUP_FILE):
        try:
            with open(FIRED_BACKUP_FILE, "r", encoding="utf-8") as f:
                fired_ids = set(json.load(f))
        except Exception:
            fired_ids = set()
    fired_ids.add(str(a["id"]))
    try:
        with open(FIRED_BACKUP_FILE, "w", encoding="utf-8") as f:
            json.dump(list(fired_ids), f)
    except Exception as e:
        print(f"[alerts] local fired backup write error: {e}")

# =====================================================================
# Supabase
# =====================================================================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://erwimqqskkzcsayvhxot.supabase.co")
APP_BASE_URL = os.environ.get("APP_BASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
GOLD_BOT_TOKEN = os.environ.get("GOLD_BOT_TOKEN", "")  # ربات جدا و مخصوص طلا (تستی)

def _sb_h():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

def log_error(msg):
    try:
        data = load_alerts()
        errs = data.get("errors", [])
        errs.append({"time": now_teh(), "msg": str(msg)})
        data["errors"] = errs[-20:]
        save_alerts(data)
    except:
        pass
    print(f"[ERR] {msg}")

def is_forex_market_open():
    now_utc = datetime.utcnow()
    wd = now_utc.weekday()
    if wd == 5: return False
    if wd == 6: return now_utc.hour >= 21
    return True

H = {"User-Agent": "Mozilla/5.0 (compatible; PriceBot/1.0)"}
_last_known = {}

def get_forex_prices_batch(symbols):
    if not symbols: return {}
    clean = [s.upper().replace("/", "").replace(" ", "") for s in symbols]
    qs = "&".join(f"symbols={s}" for s in clean)
    url = f"https://biquote.io/api/latest?{qs}"
    try:
        r = requests.get(url, timeout=12, headers=H)
        r.raise_for_status()
        raw = r.json()
        result = {}
        if isinstance(raw, list):
            for item in raw:
                sym = item.get("symbol","").upper().replace("/","")
                bid = item.get("bid") or item.get("price") or item.get("last")
                if sym and bid and float(bid) > 0:
                    result[sym] = float(bid)
                    _last_known[sym] = {"price": float(bid), "ts": now_teh(), "stale": False}
        elif isinstance(raw, dict):
            for sym, data in raw.items():
                if isinstance(data, dict):
                    bid = data.get("bid") or data.get("price") or data.get("last")
                elif isinstance(data, (int, float)):
                    bid = data
                else:
                    bid = None
                if bid and float(bid) > 0:
                    result[sym.upper()] = float(bid)
                    _last_known[sym.upper()] = {"price": float(bid), "ts": now_teh(), "stale": False}
        if result: return result
    except Exception: pass
    result = {}
    for sym in clean:
        if sym in _last_known:
            cached = _last_known[sym]
            _last_known[sym]["stale"] = True
            result[sym] = cached["price"]
        else:
            try:
                base, quote = sym[:3], sym[3:6]
                r3 = requests.get(f"https://api.frankfurter.app/latest?from={base}&to={quote}", timeout=7)
                if r3.ok:
                    rate = r3.json().get("rates", {}).get(quote)
                    if rate:
                        result[sym] = float(rate)
                        _last_known[sym] = {"price": float(rate), "ts": now_teh(), "stale": False}
            except Exception: pass
    return result

def get_forex_price(symbol):
    sym = symbol.upper().replace("/","").replace(" ","")
    batch = get_forex_prices_batch([sym])
    return batch.get(sym)

CG_MAP = {
    "BTC":"bitcoin","ETH":"ethereum","BNB":"binancecoin","SOL":"solana",
    "XRP":"ripple","ADA":"cardano","DOGE":"dogecoin","TRX":"tron",
    "TON":"toncoin","AVAX":"avalanche-2","LINK":"chainlink","DOT":"polkadot",
    "MATIC":"matic-network","UNI":"uniswap","ATOM":"cosmos","LTC":"litecoin",
    "SHIB":"shiba-inu","OP":"optimism","ARB":"arbitrum","NEAR":"near",
}

def _cg_price(base):
    gid = CG_MAP.get(base)
    if not gid: return None
    try:
        d = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={gid}&vs_currencies=usd", headers=H, timeout=8).json()
        return float(d[gid]["usd"])
    except:
        return None

def get_crypto_price(symbol):
    base = symbol.upper()
    for s in ["USDT","USDC","USD","BUSD"]:
        base = base.replace(s,"")
    base = base.replace("/","").strip()
    try:
        r = requests.get(f"https://biquote.io/api/latest?symbols={base}USD", timeout=8, headers=H)
        if r.ok:
            raw = r.json()
            bid = None
            if isinstance(raw, list) and raw:
                bid = raw[0].get("bid") or raw[0].get("price") or raw[0].get("last")
            elif isinstance(raw, dict):
                bid = raw.get("bid") or raw.get("price") or raw.get("last")
            if bid and float(bid) > 100:
                return float(bid)
    except Exception: pass
    sources = [
        ("OKX", lambda: float(requests.get(f"https://www.okx.com/api/v5/market/ticker?instId={base}-USDT", headers=H).json()["data"][0]["last"])),
        ("Binance-USDT", lambda: float(requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={base}USDT", headers=H).json()["price"])),
    ]
    for name, fn in sources:
        try:
            p = fn()
            if p and p > 0:
                return float(p)
        except: pass
    log_error(f"Crypto price failed for {symbol}")
    return None

def get_price(symbol, asset_type):
    if asset_type == "crypto":
        return get_crypto_price(symbol)
    return get_forex_price(symbol)

# ── ردیابی پیام‌های ربات برای پاک‌سازی چت ────────────────────
# { chat_id: [msg_id, msg_id, ...] }  — فقط پیام‌های غیر-fire
_bot_msg_ids: dict = {}
_BOT_MSG_MAX = 200  # حداکثر تعداد id هر چت

# ── ردیابی پیام‌های fired alert در همه چت‌ها ──────────────────
# { alert_id: { chat_id: message_id, ... } }
_fired_msg_ids: dict = {}

def _sb_save_fired_msgs(alert_id: str, cid_to_mid: dict):
    """map چت‌آیدی → مسیج‌آیدی یه آلارم fired رو توی Supabase ذخیره کن"""
    if not SUPABASE_KEY: return
    try:
        payload = {
            "id": alert_id,
            "msg_map": json.dumps(cid_to_mid),
            "created_at": now_teh()
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/fired_msgs",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=8)
        if r.status_code not in (200, 201, 204):
            print(f"[fired_msgs] save error: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[fired_msgs] save exc: {e}")

def _sb_load_fired_msgs():
    """همه fired_msgs رو از Supabase بخون و توی _fired_msg_ids لود کن"""
    if not SUPABASE_KEY: return
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/fired_msgs?select=*&limit=2000",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200:
            for row in r.json():
                aid = row.get("id")
                raw = row.get("msg_map", "{}")
                if aid:
                    _fired_msg_ids[aid] = json.loads(raw) if isinstance(raw, str) else (raw or {})
            print(f"[fired_msgs] لود شد — {len(_fired_msg_ids)} آلارم")
        else:
            print(f"[fired_msgs] load error: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[fired_msgs] load exc: {e}")

def _sb_delete_fired_msgs(alert_id: str):
    """fired_msgs یه آلارم رو از Supabase پاک کن"""
    if not SUPABASE_KEY: return
    try:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/fired_msgs?id=eq.{alert_id}",
            headers=_sb_h(), timeout=8)
    except Exception as e:
        print(f"[fired_msgs] delete exc: {e}")

# ── شمارنده هشتگ نماد ────────────────────────────────────────
# { "XAUUSD": 12, "BTCUSDT": 3, ... }  — در حافظه cache
_sym_counters: dict = {}

def _sb_next_sym_counter(sym: str) -> int:
    """
    شمارنده نماد رو یکی افزایش بده و مقدار جدید رو برگردون.
    اگه جدول symbol_counters نداشتیم از حافظه استفاده میکنه.
    """
    global _sym_counters
    if SUPABASE_KEY:
        try:
            # خوندن مقدار فعلی
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/symbol_counters?id=eq.{sym}&select=counter",
                headers=_sb_h(), timeout=6)
            if r.status_code == 200 and r.json():
                cur = int(r.json()[0]["counter"])
            else:
                cur = _sym_counters.get(sym, 0)
            new_val = cur + 1
            # upsert
            requests.post(
                f"{SUPABASE_URL}/rest/v1/symbol_counters",
                headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
                json={"id": sym, "counter": new_val}, timeout=6)
            _sym_counters[sym] = new_val
            return new_val
        except Exception as e:
            print(f"[counter] exc: {e}")
    # fallback حافظه
    _sym_counters[sym] = _sym_counters.get(sym, 0) + 1
    return _sym_counters[sym]

def _sb_load_sym_counters():
    """همه شمارنده‌ها رو از Supabase لود کن"""
    if not SUPABASE_KEY: return
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/symbol_counters?select=*",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            for row in r.json():
                _sym_counters[row["id"]] = int(row["counter"])
            print(f"[counter] لود شد — {len(_sym_counters)} نماد")
    except Exception as e:
        print(f"[counter] load exc: {e}")

def _make_alarm_tag(sym: str) -> str:
    """هشتگ منحصربه‌فرد برای آلارم — مثلاً #XAUUSD7"""
    n = _sb_next_sym_counter(sym)
    return f"#{sym}{n}"

def _extract_symbol_from_tag(tag: str) -> str:
    """از تگ آلارم (مثلاً #XAUUSD7) نماد رو استخراج کن (XAUUSD)"""
    m = re.match(r'^#?([A-Za-z]+)\d*$', tag or "")
    return m.group(1) if m else ""

# =====================================================================
# 📋 سیستم تقسیم مسئولیت آلارم‌ها بین اعضای تیم (بدون شیفت — رندوم عادلانه)
# =====================================================================

# همه اعضای تیم + آیدی عددی تلگرام‌شون (برای تشخیص دکمه‌ی مخصوص «دیدم»)
TEAM_MEMBERS = ["اتابک", "مهران", "علی", "پیمان", "مسعود", "زرداری"]

TEAM_MEMBER_IDS = {
    "مسعود":  "109419675",
    "پیمان":  "5089282711",
    "علی":    "138116289",
    "اتابک":  "349639401",
    "مهران":  "117005892",
    "زرداری": "90178485",
}
# معکوس: آیدی → اسم (برای چک سریع از from.id در callback)
TEAM_ID_TO_NAME = {v: k for k, v in TEAM_MEMBER_IDS.items()}

# ── سیستم امتیازدهی آلارم فایرشده ──────────────────────────────
RATING_RESULT_NOTIFY_USERNAME = "ali_nr2020"
RATING_RESULT_NOTIFY_CID = TEAM_MEMBER_IDS.get("علی", "")
_pending_rate: dict = {}
_rating_locks: dict = {}
_rating_locks_guard = threading.Lock()
_rating_result_notified: set = set()  # alert_idهایی که نتیجه نهایی‌شون یک‌بار اعلام شده
_rating_result_notified_lock = threading.Lock()
RATE_REASON_TTL = 300  # ثانیه — مهلت نوشتن دلیل بعد از رای
RATE_NOTIFY_WAIT = 120  # ثانیه — حداکثر صبر برای دلیلِ آخرین رأی‌دهنده قبل از ارسال نتیجه

def _rate_pending_valid(cid) -> bool:
    """آیا این کاربر هنوز یه پرامپت دلیلِ معتبر (منقضی‌نشده) داره؟ منقضی‌شده‌ها پاک می‌شن"""
    pr = _pending_rate.get(cid)
    if not isinstance(pr, dict):
        return False
    if time.time() - pr.get("ts", 0) > RATE_REASON_TTL:
        _pending_rate.pop(cid, None)
        return False
    return True

def _rate_has_open_reasons(alert_id: str) -> bool:
    """فقط دلیلِ «آخرین رأی‌دهنده» (کسی که رأیش رأی‌گیری رو بسته) نتیجه رو عقب می‌ندازه، حداکثر RATE_NOTIFY_WAIT ثانیه؛
    دلیل بقیه‌ی رأی‌دهنده‌ها منتظر نمی‌مونه."""
    now = time.time()
    return any(isinstance(v, dict) and v.get("alert_id") == alert_id and v.get("closer")
               and now - v.get("ts", 0) <= RATE_NOTIFY_WAIT
               for v in list(_pending_rate.values()))

def _rating_lock_for(alert_id: str) -> threading.Lock:
    with _rating_locks_guard:
        if alert_id not in _rating_locks:
            _rating_locks[alert_id] = threading.Lock()
        return _rating_locks[alert_id]

def _norm_name(s: str) -> str:
    return (s or "").strip().lower()

def _eligible_raters(creator_name: str) -> list:
    cn = _norm_name(creator_name)
    return [m for m in TEAM_MEMBERS if _norm_name(m) != cn]

def _sb_load_ratings(alert_id: str) -> list:
    if not SUPABASE_KEY:
        return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alarm_ratings?alert_id=eq.{alert_id}&select=*&order=voted_at.asc",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            return r.json() or []
    except Exception as e:
        print(f"[rating] load exc: {e}")
    return []

def _sb_save_rating(alert_id: str, voter_cid: str, voter_name: str, score: int,
                    reason: str, creator_name: str):
    if not SUPABASE_KEY:
        print("[rating] no SUPABASE_KEY")
        return False
    try:
        payload = {
            "id": f"{alert_id}_{voter_cid}",
            "alert_id": str(alert_id),
            "voter_cid": str(voter_cid),
            "voter_name": voter_name,
            "score": int(score),
            "reason": reason or "",
            "voted_at": now_teh(),
            "creator_name": creator_name or "",
            "status": "open",
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/alarm_ratings",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=8)
        print(f"[rating] save status={r.status_code} body={r.text[:120]}")
        return r.status_code in (200, 201, 204)
    except Exception as e:
        print(f"[rating] save exc: {e}")
        return False

def _resolve_creator_name(name: str, cid=None) -> str:
    """اسم سازنده‌ی آلارم (custom_name / یوزرنیم / آیدی) رو به اسم عضو تیم تو TEAM_MEMBERS تبدیل کن،
    تا سازنده از رأی‌گیری آلارم خودش کنار گذاشته بشه."""
    n = (name or "").strip()
    if cid and TEAM_ID_TO_NAME.get(str(cid)):
        return TEAM_ID_TO_NAME[str(cid)]
    if n in TEAM_MEMBERS:
        return n
    if n in TEAM_ID_TO_NAME:
        return TEAM_ID_TO_NAME[n]
    m = next((t for t in TEAM_MEMBERS if _norm_name(t) == _norm_name(n)), None)
    if m:
        return m
    try:
        for usr in load_alerts().get("users", []):
            cn_u = (usr.get("custom_name") or "").strip()
            if cn_u and _norm_name(cn_u) == _norm_name(n):
                tn = TEAM_ID_TO_NAME.get(str(usr.get("chat_id", "")))
                if tn:
                    return tn
    except Exception as e:
        print(f"[rating] resolve creator exc: {e}")
    return n

def _compute_rating_state(votes: list, creator_name: str, force_closed: bool = False, force_kind: str = "manual") -> dict:
    eligible = _eligible_raters(creator_name)
    max_voters = max(1, len(eligible))
    # همه رای‌های ذخیره‌شده نشون داده می‌شن (حتی اگه اسم دقیقاً تو TEAM_MEMBERS نبود)
    # فقط رای سازنده آلارم (اگه اشتباهاً ثبت شده) کنار گذاشته می‌شه
    cn = _norm_name(creator_name)
    by_key = {}
    for v in votes:
        vn = _norm_name(v.get("voter_name", ""))
        vc = str(v.get("voter_cid") or "")
        if cn and vn == cn:
            continue
        key = vn or vc or str(id(v))
        by_key[key] = v
    clean = list(by_key.values())
    total = sum(int(v.get("score", 0) or 0) for v in clean)
    n = len(clean)
    remaining = max(0, max_voters - n)
    max_possible = total + remaining * 5
    min_possible = total + remaining * 1
    def _tier(t):
        # آستانه‌ها روی مجموع: <10 پرریسک | ≥10 B | ≥15 A (به ۱۵ برسه همون لحظه بسته می‌شه)
        return "setup_a" if t >= 15 else ("setup_b" if t >= 10 else "risky")
    label, closed = "pending", False
    if remaining == 0:
        closed, label = True, _tier(total)
    elif _tier(total) == _tier(max_possible):
        # مجموع فقط می‌تونه بالا بره؛ اگه حتی با ۵ی کامل از بقیه هم سطح عوض نمی‌شه، همین الان قطعیه
        # (برچسب همیشه با مجموعِ نمایش‌داده‌شده می‌خونه)
        closed, label = True, _tier(total)
    manual = False
    if force_closed and not closed:
        # بستن دستی/خودکار: با رأی‌های موجود و همین آستانه‌ها روی مجموع فعلی محاسبه کن
        manual, closed = True, True
        label = "no_votes" if n == 0 else _tier(total)
    label_fa = {
        "pending": "⏳ در حال جمع‌آوری رای",
        "setup_a": "🟢 Setup A",
        "setup_b": "🔵 Setup B",
        "risky": "🔴 پرریسک",
        "no_votes": "⏱ بسته شد — رایی ثبت نشد",
    }.get(label, label)
    if manual and label != "no_votes":
        label_fa += " · ⏱ بسته‌شده خودکار" if force_kind == "auto" else " · 🔒 بسته‌شده دستی"
    return {
        "manual": manual,
        "total": total, "voters_count": n, "max_voters": max_voters,
        "remaining": remaining, "closed": closed, "label": label,
        "label_fa": label_fa, "votes": clean,
        "max_possible": max_possible, "min_possible": min_possible,
    }

def _build_rating_section(state: dict, cancelled: bool = False) -> str:
    lines = ["", "────────────", "⭐ <b>امتیازدهی تیم</b>"]
    lines.append(f"مجموع: <b>{state['total']}</b>  ·  {state['voters_count']}/{state['max_voters']} رای")
    lines.append("❌ <b>لغو شد</b> (آلارم false شد)" if cancelled else state["label_fa"])
    if state["votes"]:
        lines.append("")
        for v in state["votes"]:
            why = (v.get("reason") or "").strip()
            sc = v.get("score", "?")
            nm = v.get("voter_name", "—")
            lines.append(f"• <b>{nm}</b>: {sc}/5" + (f" — <i>{why}</i>" if why else ""))
    if not state["closed"] and not cancelled:
        lines.append("")
        lines.append("<i>روی دکمه ۰ تا ۵ بزن (بعد از رای، با «ویرایش» می‌تونی عوضش کنی)</i>")
    return "\n".join(lines)

def _rating_keyboard(alert_id: str, state: dict) -> list:
    if state.get("closed"):
        return []
    aid = str(alert_id)
    # تلگرام رنگ واقعی دکمه نداره — با ایموجی طیف قرمز→سبز
    return [[
        {"text": "❌ 0", "callback_data": f"rate:{aid}:0"},
        {"text": "🔴 1", "callback_data": f"rate:{aid}:1"},
        {"text": "🟠 2", "callback_data": f"rate:{aid}:2"},
        {"text": "🟡 3", "callback_data": f"rate:{aid}:3"},
        {"text": "🟢 4", "callback_data": f"rate:{aid}:4"},
        {"text": "💚 5", "callback_data": f"rate:{aid}:5"},
    ], [
        {"text": "🔒 بستن دستی و محاسبه", "callback_data": f"rate_close:{aid}"},
    ]]

_rate_editing = set()  # {(alert_id, chat_id)} — کسایی که الان تو حالت ویرایشِ رای/کامنت‌ان

def _rating_keyboard_for(alert_id: str, state: dict, cid) -> list:
    """کیبورد اختصاصی هر چت: کسی که رای داده فقط دکمه‌ی «ویرایش» می‌بینه (تا اتفاقی رایش عوض نشه)،
    با زدنش دکمه‌های امتیاز + «فقط کامنت» + «انصراف» دوباره باز می‌شن."""
    if state.get("closed"):
        return []
    aid, cid = str(alert_id), str(cid)
    voted = any(str(v.get("voter_cid")) == cid for v in state.get("votes", []))
    if voted and (aid, cid) not in _rate_editing:
        return [[{"text": "✏️ ویرایش رای و کامنت", "callback_data": f"rate_edit:{aid}"}]]
    kb = _rating_keyboard(aid, state)
    if voted:
        kb.insert(1, [
            {"text": "✏️ فقط کامنت", "callback_data": f"rate_comment:{aid}"},
            {"text": "↩️ انصراف", "callback_data": f"rate_edit_cancel:{aid}"},
        ])
    return kb

def _edit_markup(token: str, chat_id, mid, kb: list):
    try:
        requests.post(f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
                      json={"chat_id": str(chat_id), "message_id": mid, "reply_markup": {"inline_keyboard": kb}},
                      timeout=10, headers=H)
    except Exception as e:
        print(f"[rating] edit markup exc: {e}")

def _load_fired_msg_map(alert_id: str) -> dict:
    """map پیام فایر رو از حافظه + Supabase یکی کن (برای multi-instance حیاتی است)"""
    msg_map = dict(_fired_msg_ids.get(alert_id) or {})
    if SUPABASE_KEY:
        try:
            r_fm = requests.get(
                f"{SUPABASE_URL}/rest/v1/fired_msgs?id=eq.{alert_id}&select=msg_map",
                headers=_sb_h(), timeout=8)
            if r_fm.status_code == 200 and r_fm.json():
                raw_fm = r_fm.json()[0].get("msg_map") or "{}"
                remote = json.loads(raw_fm) if isinstance(raw_fm, str) else (raw_fm or {})
                if isinstance(remote, dict):
                    # chat_id → message_id از DB اولویت داره (کامل‌تره)
                    for k, v in remote.items():
                        if str(k).startswith("__"):
                            if k not in msg_map or not msg_map.get(k):
                                msg_map[k] = v
                        else:
                            msg_map[k] = v
                    if remote.get("__text__"):
                        msg_map["__text__"] = remote["__text__"]
                    if remote.get("__creator__"):
                        msg_map["__creator__"] = remote["__creator__"]
        except Exception as e:
            print(f"[rating] fired_msgs fetch: {e}")
    return msg_map

def _strip_rating_section(text: str) -> str:
    """بخش امتیاز رو از متن جدا کن تا پایهٔ پیام فایر بمونه"""
    if not text:
        return ""
    for sep in ("────────────", "⭐ امتیازدهی تیم", "⭐ <b>امتیازدهی تیم</b>"):
        if sep in text:
            text = text.split(sep)[0]
    return text.rstrip()

def _refresh_rating_messages(token: str, alert_id: str, creator_name: str, extra_chat_msg=None, fallback_text=None):
    """
    extra_chat_msg: (chat_id, message_id) پیام کلیک‌شده
    fallback_text: متن فعلی همان پیام (از callback_query.message) اگه __text__ تو DB نبود
    """
    votes = _sb_load_ratings(alert_id)
    state = _compute_rating_state(votes, creator_name)
    print(f"[rating] refresh start aid={alert_id} votes={state['voters_count']} total={state['total']}")
    msg_map = _load_fired_msg_map(alert_id)
    if msg_map.get("__rating_cancelled__") == "1":
        return state  # آلارم false شده — پیام نباید دوباره به حالت رأی‌گیری برگرده
    if msg_map.get("__rating_manual__") in ("1", "auto"):
        state = _compute_rating_state(votes, creator_name, force_closed=True,
                                      force_kind=("auto" if msg_map.get("__rating_manual__") == "auto" else "manual"))
    if extra_chat_msg:
        ec, em = extra_chat_msg
        if ec and em:
            msg_map[str(ec)] = int(em)
            print(f"[rating] extra chat={ec} mid={em}")
    base = msg_map.get("__text__", "") or ""
    if not base and msg_map.get("__text_rated__"):
        base = _strip_rating_section(msg_map.get("__text_rated__", ""))
    if not base and fallback_text:
        base = _strip_rating_section(fallback_text)
        print(f"[rating] using fallback_text len={len(base)}")
    if not base:
        print(f"[rating] base text STILL missing aid={alert_id} keys={list(msg_map.keys())}")
        # حتی بدون پایه، حداقل تلاش کن پیام کلیک‌شده رو با فقط بخش امتیاز ادیت نکن — بی‌فایده‌ست
        return state
    base = _strip_rating_section(base)
    full_text = base + _build_rating_section(state)
    targets = [(k, v) for k, v in msg_map.items() if not str(k).startswith("__")]
    print(f"[rating] will edit {len(targets)} chats: {[t[0] for t in targets]}")
    edited = 0
    for tc, tm in targets:
        try:
            mid = int(tm)
            payload = {"chat_id": str(tc), "message_id": mid, "text": full_text,
                       "parse_mode": "HTML",
                       "reply_markup": {"inline_keyboard": _rating_keyboard_for(alert_id, state, tc)}}
            r = requests.post(
                f"https://api.telegram.org/bot{token}/editMessageText",
                json=payload, timeout=10, headers=H)
            body = (r.text or "")[:180]
            print(f"[rating] edit chat={tc} mid={mid} -> {r.status_code} {body}")
            if r.status_code == 200 or (r.status_code == 400 and "not modified" in body):
                edited += 1
            elif r.status_code == 400:
                # بدون HTML دوباره تلاش (گاهی parse خطا می‌ده)
                payload.pop("parse_mode", None)
                r2 = requests.post(
                    f"https://api.telegram.org/bot{token}/editMessageText",
                    json=payload, timeout=10, headers=H)
                print(f"[rating] edit retry plain chat={tc} -> {r2.status_code} {(r2.text or '')[:120]}")
                if r2.status_code == 200 or (r2.status_code == 400 and "not modified" in (r2.text or "")):
                    edited += 1
        except Exception as e:
            print(f"[rating] edit exc {tc}: {e}")
    print(f"[rating] refresh done {alert_id}: edited {edited}/{len(targets)}")
    msg_map["__text__"] = base
    msg_map["__text_rated__"] = full_text
    msg_map["__creator__"] = creator_name or msg_map.get("__creator__", "")
    if state["closed"]:
        msg_map["__rating_closed__"] = "1"
    _fired_msg_ids[alert_id] = msg_map
    try:
        _sb_save_fired_msgs(alert_id, msg_map)
    except Exception as e:
        print(f"[rating] save map exc: {e}")
    return state

RATE_AUTO_CLOSE_GOLD = 15 * 60      # طلا (معمولی و فوری)
RATE_AUTO_CLOSE_INSTANT = 15 * 60   # فوریِ بقیه‌ی نمادها
RATE_AUTO_CLOSE_REGULAR = 60 * 60   # معمولیِ بقیه‌ی نمادها

def _rate_auto_close_seconds(sym: str, instant: bool) -> int:
    su = (sym or "").upper()
    if "XAU" in su or "GOLD" in su:
        return RATE_AUTO_CLOSE_GOLD
    return RATE_AUTO_CLOSE_INSTANT if instant else RATE_AUTO_CLOSE_REGULAR

def _rate_auto_close(alert_id: str):
    """زمان رأی‌گیری تموم شد → با رأی‌های موجود بسته و محاسبه می‌شه؛ بدون رای فقط بسته می‌شه (نتیجه‌ای نمی‌ره)."""
    try:
        with _rating_lock_for(alert_id):
            mm = _load_fired_msg_map(alert_id)
            if mm.get("__rating_cancelled__") == "1" or mm.get("__rating_closed__") == "1":
                return
            creator = _resolve_creator_name(mm.get("__creator__", ""))
            mm["__rating_closed__"] = "1"
            mm["__rating_manual__"] = "auto"
            _fired_msg_ids[alert_id] = mm
            try:
                _sb_save_fired_msgs(alert_id, mm)
            except Exception as e:
                print(f"[rating] auto close save exc: {e}")
        token = _get_token_and_cids()[0]
        _refresh_rating_messages(token, alert_id, creator)
        _rate_try_notify(token, alert_id, creator)
        print(f"[rating] auto closed {alert_id}")
    except Exception as e:
        print(f"[rating] auto close exc: {e}")

def _rating_autoclose_loop():
    """هر ۲۰ ثانیه آلارم‌های باز رو چک می‌کنه؛ deadline تو map (Supabase) ذخیره‌ست، پس بعد از ری‌استارت هم کار می‌کنه."""
    while True:
        try:
            now = time.time()
            for aid, mm in list(_fired_msg_ids.items()):
                if not isinstance(mm, dict):
                    continue
                dl = mm.get("__rating_deadline__")
                if not dl or mm.get("__rating_closed__") == "1" or mm.get("__rating_cancelled__") == "1":
                    continue
                try:
                    due = now >= float(dl)
                except (TypeError, ValueError):
                    continue
                if due:
                    _rate_auto_close(aid)
        except Exception as e:
            print(f"[rating] autoclose loop exc: {e}")
        time.sleep(20)

def _send_result_to_gold_bot(text: str) -> bool:
    """نتیجه‌ی Setup A/B رو با GOLD_BOT_TOKEN برای مشترک‌های ربات طلا (+ علی) می‌فرسته.
    اگه توکن/گیرنده‌ای نبود False برمی‌گردونه تا نتیجه گم نشه و روال قبلی (ربات اصلی) اجرا بشه."""
    if not GOLD_BOT_TOKEN:
        return False
    with _gold_bot_lock:
        targets = set(str(x) for x in _gold_bot_subscribers)
    if RATING_RESULT_NOTIFY_CID:
        targets.add(str(RATING_RESULT_NOTIFY_CID))
    if not targets:
        return False
    for cid in sorted(targets):
        try:
            send_tg(GOLD_BOT_TOKEN, cid, text)
        except Exception as e:
            print(f"[rating] gold bot send error to {cid}: {e}")
    return True

def _notify_rating_result(token: str, alert_id: str, state: dict, creator_name: str):
    tag = (_fired_msg_ids.get(alert_id) or {}).get("__tag__", alert_id)
    body = [
        "✅ <b>نتیجه امتیازدهی</b>", str(tag), state["label_fa"],
        f"مجموع امتیاز: <b>{state['total']}</b> از {(state['voters_count'] if state.get('manual') else state['max_voters']) * 5}", "",
    ]
    for v in state["votes"]:
        why = (v.get("reason") or "").strip()
        body.append(f"• {v.get('voter_name','—')}: {v.get('score','?')}/5" + (f" — {why}" if why else ""))
    body += ["", f"👤 سازنده: {creator_name or '—'}", f"@{RATING_RESULT_NOTIFY_USERNAME}"]
    text = "\n".join(body)
    # Setup A / B → تو ربات اصلی ریپلای و نمایش داده نمی‌شه؛ فقط با ربات طلا می‌ره
    if state.get("label") in ("setup_a", "setup_b"):
        # برای ربات طلا کل اطلاعات آلارم (نماد، تگ، سازنده، قیمت، کامنت، زمان) + نتیجه‌ی نظرسنجی
        _mm_g = _fired_msg_ids.get(alert_id) or _load_fired_msg_map(alert_id) or {}
        _alarm_txt = _strip_rating_section(_mm_g.get("__text__", "") or "")
        _gold_text = (_alarm_txt + "\n\n────────────\n" + text) if _alarm_txt else text
        if _send_result_to_gold_bot(_gold_text):
            return
    msg_map = _fired_msg_ids.get(alert_id, {}) or {}
    if state.get("label") == "risky":
        # فقط هشتک آلارم + «ستاپ پرریسک تشخیص داده شد»؛ ۵ دقیقه بعد پاک می‌شه تا ربات خلوت بمونه
        short = f"{tag}\nستاپ پرریسک تشخیص داده شد\n\nاین پیام ۵ دقیقه دیگه پاک می‌شه"
        sent_to = set()
        for tc, tm in list(msg_map.items()):
            if str(tc).startswith("__"):
                continue
            try:
                r = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": tc, "text": short, "parse_mode": "HTML", "reply_to_message_id": int(tm)},
                    timeout=8, headers=H)
                mid_r = (r.json().get("result") or {}).get("message_id")
                if mid_r:
                    _delete_msg_after(token, tc, mid_r, delay=300)
                sent_to.add(str(tc))
            except Exception as e:
                print(f"[rating] risky notify fail {tc}: {e}")
        if RATING_RESULT_NOTIFY_CID and str(RATING_RESULT_NOTIFY_CID) not in sent_to:
            try:
                mid_a = send_tg_keyboard(token, str(RATING_RESULT_NOTIFY_CID), short, [], track=False)
                if mid_a:
                    _delete_msg_after(token, str(RATING_RESULT_NOTIFY_CID), mid_a, delay=300)
            except Exception as e:
                print(f"[rating] risky ali notify: {e}")
        return
    for tc, tm in list(msg_map.items()):
        if str(tc).startswith("__"):
            continue
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": tc, "text": text, "parse_mode": "HTML", "reply_to_message_id": int(tm)},
                timeout=8, headers=H)
        except Exception as e:
            print(f"[rating] notify fail {tc}: {e}")
    if RATING_RESULT_NOTIFY_CID:
        try:
            send_tg(token, RATING_RESULT_NOTIFY_CID, "📣 نتیجه امتیاز:\n\n" + text)
        except Exception as e:
            print(f"[rating] ali notify: {e}")

def _rate_cancel_poll(alert_id: str):
    """آلارم false شد → رأی‌گیری بسته و کلاً لغو می‌شه: دکمه‌ها برداشته، نتیجه برای کسی نمی‌ره، پرامپت دلیل‌ها پاک."""
    try:
        with _rating_lock_for(alert_id):
            msg_map = _load_fired_msg_map(alert_id)
            _txt0 = msg_map.get("__text__", "") or ""
            if not msg_map.get("__text_rated__") and ("__creator__" not in msg_map or "آلارم شخصی" in _txt0):
                return  # این آلارم نظرسنجی نداشته (قدیمی یا شخصی)
            if msg_map.get("__rating_cancelled__") == "1":
                return
            msg_map["__rating_closed__"] = "1"
            msg_map["__rating_cancelled__"] = "1"
            _fired_msg_ids[alert_id] = msg_map
            try:
                _sb_save_fired_msgs(alert_id, msg_map)
            except Exception as e:
                print(f"[rating] cancel save map exc: {e}")
        # پرامپت‌های دلیلِ باز این آلارم
        token = _get_token_and_cids()[0]
        for k, v in list(_pending_rate.items()):
            if isinstance(v, dict) and v.get("alert_id") == alert_id:
                _pending_rate.pop(k, None)
                if v.get("prompt_mid"):
                    _delete_msg_after(token, str(k), v["prompt_mid"], delay=0)
        base = _strip_rating_section(msg_map.get("__text__", "") or msg_map.get("__text_rated__", ""))
        if not base:
            return
        creator_cx = _resolve_creator_name(msg_map.get("__creator__", ""))
        state_cx = _compute_rating_state(_sb_load_ratings(alert_id), creator_cx, force_closed=True)
        new_text = base + _build_rating_section(state_cx, cancelled=True)  # رأی‌های ثبت‌شده تا اینجا می‌مونن
        for tc, tm in list(msg_map.items()):
            if str(tc).startswith("__"):
                continue
            try:
                payload = {"chat_id": str(tc), "message_id": int(tm), "text": new_text,
                           "parse_mode": "HTML", "reply_markup": {"inline_keyboard": []}}
                r = requests.post(f"https://api.telegram.org/bot{token}/editMessageText",
                                  json=payload, timeout=10, headers=H)
                print(f"[rating] cancel edit chat={tc} -> {r.status_code}")
            except Exception as e:
                print(f"[rating] cancel edit exc {tc}: {e}")
    except Exception as e:
        print(f"[rating] cancel exc: {e}")

def _rate_try_notify(token: str, alert_id: str, creator_name: str = ""):
    """نتیجه‌ی نهایی رو فقط وقتی بفرست که رأی‌گیری بسته شده و دیگه کسی در حال نوشتن دلیل نیست (فقط یک‌بار)"""
    try:
        _mm = _load_fired_msg_map(alert_id)
        if _mm.get("__rating_cancelled__") == "1":
            return  # آلارم false شده — نتیجه‌ای فرستاده نمی‌شه
        creator_name = _resolve_creator_name(creator_name or _mm.get("__creator__", ""))
        state = _compute_rating_state(_sb_load_ratings(alert_id), creator_name,
                                      force_closed=(_mm.get("__rating_manual__") in ("1", "auto")),
                                      force_kind=("auto" if _mm.get("__rating_manual__") == "auto" else "manual"))
        if not state["closed"] or state["voters_count"] == 0:
            return  # بسته نشده، یا هیچ رایی نیست → نتیجه‌ای برای فرستادن نیست
        if state["label"] != "risky" and _rate_has_open_reasons(alert_id):
            return  # A/B منتظر دلیل آخرین رأی‌دهنده می‌مونن؛ پرریسک فوری می‌ره
        with _rating_result_notified_lock:
            if alert_id in _rating_result_notified:
                return
            _rating_result_notified.add(alert_id)
        _notify_rating_result(token, alert_id, state, creator_name)
    except Exception as e:
        print(f"[rating] try_notify exc: {e}")

def _send_instant_rated(tok, tgts, msg, aid, atag, creator) -> dict:
    """آلارم فوری با نظرسنجی — پیام با بخش امتیاز + دکمه‌های ۱ تا ۵ ارسال و map برای ادیت/رأی ذخیره می‌شه"""
    creator = _resolve_creator_name(creator)
    st0 = _compute_rating_state([], creator)
    full = msg + _build_rating_section(st0)
    kb = _rating_keyboard(aid, st0)
    cid_to_mid = {}
    for tc in tgts:
        mid = send_tg_keyboard(tok, str(tc), full, kb, track=False)
        if mid:
            cid_to_mid[str(tc)] = mid
    if cid_to_mid:
        cid_to_mid["__tag__"] = atag
        cid_to_mid["__text__"] = msg
        cid_to_mid["__creator__"] = creator
        cid_to_mid["__rating_deadline__"] = str(int(time.time() + _rate_auto_close_seconds(_extract_symbol_from_tag(atag), True)))
        _fired_msg_ids[aid] = cid_to_mid
        try:
            _sb_save_fired_msgs(aid, cid_to_mid)
        except Exception as e:
            print(f"[instant] fired_msgs save exc: {e}")
    return cid_to_mid

# نام کسی که در حالت فعال «ترجیح داده نمی‌شه» — این خودش همیشه ثابت می‌مونه،
# اما این‌که فعال باشه یا نه از پنل وب قابل تغییره (نگاه کن به _deprioritize_masoud_active)
DEPRIORITIZED_MEMBER = "مسعود"
# بازه ساعتی تهران که این عضو تقریباً هیچ‌وقت نباید بگیره مگه بقیه مشغول باشن
DEPRIORITIZED_BLOCK_START = 8
DEPRIORITIZED_BLOCK_END = 12

# آیا اولویت پایین مسعود فعاله؟ در startup از Supabase لود می‌شه و از پنل وب
# قابل تغییره. پیش‌فرض True تا رفتار فعلی حفظ بشه.
_deprioritize_masoud_active: bool = True
_deprioritize_masoud_lock = threading.Lock()

def _get_deprioritize_masoud() -> bool:
    with _deprioritize_masoud_lock:
        return _deprioritize_masoud_active

def _set_deprioritize_masoud(value: bool):
    global _deprioritize_masoud_active
    with _deprioritize_masoud_lock:
        _deprioritize_masoud_active = value
    _sb_save_deprioritize_masoud(value)

# اسم اعضایی که موقتاً از چرخه‌ی تقسیم آلارم کنار گذاشته شدن (مرخصی/غیره).
# پیش‌فرض خالی (همه در دسترس) تا رفتار فعلی حفظ بشه.
_unavailable_members: set = set()
_unavailable_members_lock = threading.Lock()

def _get_unavailable_members() -> set:
    with _unavailable_members_lock:
        return set(_unavailable_members)

def _set_member_availability(name: str, available: bool):
    global _unavailable_members
    with _unavailable_members_lock:
        if available:
            _unavailable_members.discard(name)
        else:
            _unavailable_members.add(name)
        snapshot = sorted(_unavailable_members)
    _sb_save_unavailable_members(snapshot)

# اسم اعضایی که آلارم فوریشون ذخیره و آرشیو میشه ولی به تلگرام ارسال نمیشه.
# پیش‌فرض خالی (همه ارسال میشن) تا رفتار فعلی حفظ بشه.
_muted_instant_senders: set = set()
_muted_instant_lock = threading.Lock()

def _is_instant_sender_muted(name: str) -> bool:
    if not name:
        return False
    with _muted_instant_lock:
        return name.strip().lower() in _muted_instant_senders

def _set_instant_sender_muted(name: str, muted: bool):
    global _muted_instant_senders
    key = name.strip().lower()
    with _muted_instant_lock:
        if muted:
            _muted_instant_senders.add(key)
        else:
            _muted_instant_senders.discard(key)
        snapshot = sorted(_muted_instant_senders)
    _sb_save_muted_instant_senders(snapshot)

# =====================================================================
# 🌐 پنل ادمین وب — /admin-panel
# =====================================================================
ADMIN_PANEL_PASSWORD = os.environ.get("ADMIN_PANEL_PASSWORD", "")
# session tokenهای معتبر — در حافظه، با انقضای ۱۲ ساعته
_admin_sessions: dict = {}  # token → expiry_timestamp
_admin_sessions_lock = threading.Lock()
ADMIN_SESSION_TTL_SECONDS = 12 * 3600

def _create_admin_session() -> str:
    token = secrets.token_urlsafe(32)
    with _admin_sessions_lock:
        _admin_sessions[token] = time.time() + ADMIN_SESSION_TTL_SECONDS
    return token

def _validate_admin_session(token: str) -> bool:
    if not token:
        return False
    with _admin_sessions_lock:
        expiry = _admin_sessions.get(token)
        if expiry is None:
            return False
        if time.time() > expiry:
            del _admin_sessions[token]
            return False
        return True

def _require_admin_session():
    """چک هدر Authorization برای endpointهای پنل وب — برمی‌گردونه None اگه معتبر بود، وگرنه یه Flask response خطا"""
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.replace("Bearer ", "").strip()
    if not _validate_admin_session(token):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    return None

def _is_weekend_tehran():
    """شنبه (5) و یکشنبه (6) — فقط برای نمایش/گزارش، دیگه تاثیری در تقسیم نداره"""
    return datetime.now(TEHRAN).weekday() in (5, 6)

# { member_name: count_of_active_assignments }  — in-memory cache
# این شمارش فقط برای false-handling و آمار لحظه‌ای استفاده می‌شه، نه برای تصمیم تقسیم
_active_assign_count: dict = {}

# { member_name: count_of_alarms_received_this_week } — معیار اصلی تقسیم عادلانه.
# هر آلارمی که به کسی داده بشه (چه فعال بمونه چه بعداً false بشه) اینجا شمرده می‌شه
# و تا شنبه‌ی هفته‌ی بعد صفر نمی‌شه. این یعنی کسی که همین الان false کرده، دیگه
# «صفر» حساب نمی‌شه صرفاً چون آلارمش تموم شده — باید صبر کنه تا نوبت واقعی‌ش بشه،
# و اگه یه روز آلارم کمی گیرش اومد، بقیه‌ی هفته جبران می‌شه.
_daily_assign_count: dict = {}
_daily_assign_date: str = ""  # کلید هفته (تاریخ شنبه‌ی همون هفته) که شمارش هفتگی براش معتبره
_daily_assign_lock = threading.Lock()

def _tehran_today_str() -> str:
    return datetime.now(TEHRAN).strftime("%Y-%m-%d")

def _tehran_week_start_str() -> str:
    """تاریخ شنبه‌یِ همین هفته (شروع هفته‌ی ایرانی) به‌عنوان کلید شمارش هفتگی"""
    now = datetime.now(TEHRAN)
    days_since_saturday = (now.weekday() - 5) % 7  # weekday(): دوشنبه=0 ... شنبه=5
    week_start = now - timedelta(days=days_since_saturday)
    return week_start.strftime("%Y-%m-%d")

def _ensure_daily_reset():
    """شمارشِ عادلانه دیگه روزانه نیست، هفتگیه: اگه هفته عوض شده (رسیدیم به شنبه‌ی جدید)
    شمارش صفر می‌شه. تا وقتی هفته عوض نشده، شمارش تجمیعی می‌مونه — همین باعث می‌شه
    کسی که یه روز سبک (آلارم کم) گیرش اومده، بقیه‌ی هفته جبران بشه و در کل هفته
    تقریباً به تعداد مساوی با بقیه آلارم بگیره."""
    global _daily_assign_count, _daily_assign_date
    week_key = _tehran_week_start_str()
    with _daily_assign_lock:
        if _daily_assign_date != week_key:
            _daily_assign_count = {}
            _daily_assign_date = week_key

def _bump_daily_count(name: str):
    _ensure_daily_reset()
    with _daily_assign_lock:
        _daily_assign_count[name] = _daily_assign_count.get(name, 0) + 1

def _get_daily_counts(members: list) -> dict:
    _ensure_daily_reset()
    with _daily_assign_lock:
        return {m: _daily_assign_count.get(m, 0) for m in members}

def _rebuild_daily_assign_count():
    """بعد از ری‌استارت، شمارش هفتگی رو از Supabase بازسازی کن — بدون این
    کار، بعد از هر دیپلوی عدالتِ هفتگی از صفر شروع می‌شد و بی‌معنی می‌شد."""
    global _daily_assign_count, _daily_assign_date
    rows = _sb_load_today_assignments()
    counts = {}
    for row in rows:
        name = row.get("assigned_to", "")
        if name:
            counts[name] = counts.get(name, 0) + 1
    with _daily_assign_lock:
        _daily_assign_count = counts
        _daily_assign_date = _tehran_week_start_str()
    print(f"[assign] شمارش هفتگی بازسازی شد: {counts}")

# جلوگیری از double-handover: startup و scheduler هر کدوم فقط یه بار اجرا کنن
# جلوگیری از race condition در /False — اگه یه آلارم داره false میشه، دیگران صبر کنن
_false_in_progress: set = set()
_false_in_progress_lock = threading.Lock()

# جلوگیری از دابل-ack — اگه دکمه «دیدم» دوبار کلیک بشه (دابل‌تپ یا کندی شبکه)
# قبل از این‌که پیام ادیت و دکمه پاک بشه، این از ثبت تکراری جلوگیری می‌کنه
_ack_done: set = set()
_ack_lock = threading.Lock()

# ─── Supabase helpers ────────────────────────────────────────────────

def _sb_save_assignment(alarm_id: str, alarm_tag: str, assignee: str, shift: str, fired_at: str,
                        symbol: str = "", target_price: float = 0, created_by: str = ""):
    """ذخیره/آپدیت assignment در Supabase"""
    if not SUPABASE_KEY: return
    try:
        payload = {
            "id": alarm_id,
            "alarm_tag": alarm_tag,
            "assigned_to": assignee,
            "shift": shift,
            "is_active": True,
            "fired_at": fired_at,
            "false_at": None,
            "false_by": None,
            "symbol": symbol,
            "target_price": target_price,
            "created_by": created_by
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=8)
        if r.status_code not in (200, 201, 204):
            print(f"[assign] save error: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[assign] save exc: {e}")

def _sb_false_assignment(alarm_id: str, false_by: str, reason: str = ""):
    """
    وقتی /False زده میشه — assignment رو غیرفعال کن.
    اگه قبلاً false شده بود، یه ردیف history جدید append کن (به جای overwrite).
    """
    threading.Thread(target=_rate_cancel_poll, args=(alarm_id,), daemon=True).start()  # false → رأی‌گیری لغو
    if not SUPABASE_KEY: return
    try:
        # خوندن وضعیت فعلی برای گرفتن history قبلی
        r_get = requests.get(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{alarm_id}&select=is_active,false_history",
            headers=_sb_h(), timeout=8)
        prev_history = []
        already_false = False
        if r_get.status_code == 200:
            rows = r_get.json()
            if rows:
                already_false = (rows[0].get("is_active") == False)
                prev_history = rows[0].get("false_history") or []
                if isinstance(prev_history, str):
                    try: prev_history = json.loads(prev_history)
                    except: prev_history = []

        new_entry = {"by": false_by, "at": now_teh(), "reason": reason}
        prev_history.append(new_entry)

        payload = {
            "is_active": False,
            "false_at": now_teh(),
            "false_by": false_by,
            "false_history": prev_history
        }
        if reason:
            payload["false_reason"] = reason
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{alarm_id}",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json=payload, timeout=8)
        if r.status_code not in (200, 204):
            print(f"[assign] false error: {r.status_code} {r.text[:80]}")
        return already_false  # True اگه قبلاً false شده بود (یعنی این آپدیته)
    except Exception as e:
        print(f"[assign] false exc: {e}")
        return False

def _sb_load_active_assignments():
    """لود همه assignment‌های فعال از Supabase"""
    if not SUPABASE_KEY: return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?is_active=eq.true&select=*",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[assign] load exc: {e}")
    return []

def _sb_load_today_assignments():
    """
    لود همه assignment‌هایی که از شنبه‌ی همین هفته (نیمه‌شب تهران) تا الان
    fired_at داشتن — چه الان فعال باشن چه false شده باشن. برای بازسازی
    شمارش عادلانه‌ی هفتگی بعد از ری‌استارت لازمه.
    """
    if not SUPABASE_KEY: return []
    try:
        now = datetime.now(TEHRAN)
        days_since_saturday = (now.weekday() - 5) % 7
        week_start = (now - timedelta(days=days_since_saturday)).replace(hour=0, minute=0, second=0, microsecond=0)
        week_start_str = week_start.strftime("%Y-%m-%d %H:%M:%S")
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?fired_at=gte.{week_start_str}&select=assigned_to,fired_at",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[assign] load week exc: {e}")
    return []

def _rebuild_active_assign_count(rows):
    """بازسازی count از Supabase rows — فقط assigned آلارم‌ها"""
    global _active_assign_count
    _active_assign_count = {}
    for row in rows:
        name = row.get("assigned_to", "")
        if name:
            _active_assign_count[name] = _active_assign_count.get(name, 0) + 1
    print(f"[assign] active counts: {_active_assign_count}")

# ─── انتخاب مسئول (رندوم عادلانه، بدون شیفت) ─────────────────────────

def _pick_assignee(members: list) -> str:
    """
    اولویت اصلی: کسی که همین الان هیچ آلارم فعالی نداره (idle) — یعنی به
    کسی که یه آلارم دستشه، آلارم دوم نمی‌دیم مگر این‌که همه مشغول باشن و
    مجبور باشیم. بین idle ها، کسی که این هفته (از شنبه، نیمه‌شب تهران)
    کمترین آلارم دریافت کرده انتخاب می‌شه — تا هم کسی دوتا نگیره وقتی یکی
    دیگه بی‌کاره، هم در کل هفته شمارش‌ها تقریباً برابر بمونه. اگه یه روز
    کم‌کار بود، بقیه‌ی هفته (وقتی idle باشه) جبران می‌شه.

    اگه همه مشغول باشن (هیچ‌کس idle نیست)، آلارم بی‌صاحب نمی‌مونه —
    بین کسایی که کمترین شمارش هفتگی رو دارن (حتی اگه مشغولن) تقسیم می‌شه.

    استثنا: عضو DEPRIORITIZED_MEMBER تا حد امکان کنار گذاشته می‌شه —
    فقط وقتی انتخاب می‌شه که واقعاً نوبتش شده. تو بازه‌ی
    DEPRIORITIZED_BLOCK_START تا DEPRIORITIZED_BLOCK_END فقط زمانی بهش
    می‌دیم که همه‌ی بقیه‌ی اعضا خودشون یه آلارم فعال داشته باشن (مجبوریم).
    """
    import random
    if not members:
        return ""

    daily_counts = _get_daily_counts(members)
    active_counts = {m: _active_assign_count.get(m, 0) for m in members}

    def _finalize(chosen: str) -> str:
        _active_assign_count[chosen] = _active_assign_count.get(chosen, 0) + 1
        _bump_daily_count(chosen)
        return chosen

    def _choose_among(pool: list) -> str:
        """
        اول بین کسایی که همین الان هیچ آلارم فعالی ندارن (idle) انتخاب کن —
        حتی اگه شمارش هفتگی‌شون از یکی که مشغوله بیشتر باشه. یعنی به کسی که
        همین الان یه آلارم دستشه، یه آلارم دومی نمی‌دیم مگر این‌که واقعاً
        مجبور باشیم (همه مشغول باشن). بین idle ها هم، کسی که شمارش هفتگی‌ش
        کمتره اولویت داره تا عدالت هفتگی هم رعایت بشه.
        """
        idle = [m for m in pool if active_counts[m] == 0]
        if idle:
            min_daily_idle = min(daily_counts[m] for m in idle)
            tied_idle = [m for m in idle if daily_counts[m] == min_daily_idle]
            return random.choice(tied_idle)
        # همه مشغولن — مجبوریم؛ بین کمترین شمارش هفتگی انتخاب کن
        min_daily = min(daily_counts[m] for m in pool)
        tied = [m for m in pool if daily_counts[m] == min_daily]
        return random.choice(tied)

    # اگه اولویت پایین مسعود از پنل خاموش شده باشه، دیگه استثنا قائل نشو —
    # دقیقاً مثل بقیه‌ی اعضا وارد چرخه‌ی عادلانه‌ی خالص می‌شه
    deprioritize_on = _get_deprioritize_masoud()

    others = [m for m in members if m != DEPRIORITIZED_MEMBER]
    has_deprioritized = deprioritize_on and (DEPRIORITIZED_MEMBER in members)

    h_now = datetime.now(TEHRAN).hour
    in_block_hours = DEPRIORITIZED_BLOCK_START <= h_now < DEPRIORITIZED_BLOCK_END

    if has_deprioritized and others:
        min_others_daily = min(daily_counts[m] for m in others)
        min_dep_daily = daily_counts[DEPRIORITIZED_MEMBER]
        # «مجبوریم» یعنی همه‌ی بقیه همین الان یه آلارم فعال دارن — آلارم
        # نباید بی‌صاحب بمونه، پس چاره‌ای جز دادن به یکی نیست
        everyone_else_busy = all(active_counts[m] >= 1 for m in others)

        if in_block_hours:
            forced = everyone_else_busy
            if not forced:
                return _finalize(_choose_among(others))
            return _finalize(_choose_among(members))
        else:
            if everyone_else_busy:
                # مجبوریم بدیم — بین کمترین شمارش روزانه انتخاب کن (شاید مسعود باشه)
                return _finalize(_choose_among(members))
            elif min_others_daily <= min_dep_daily:
                return _finalize(_choose_among(others))
            else:
                return _finalize(_choose_among(members))

    # حالت عادی (بدون عضو دپریوریتایزد در لیست) — عادلانه‌ی روزانه‌ی خالص
    return _finalize(_choose_among(members))

def _get_assignee_for_alarm(alarm_id: str, alarm_tag: str, fired_at: str,
                            symbol: str = "", target_price: float = 0, created_by: str = "") -> tuple:
    """
    مسئول آلارم رو بین کل اعضای تیم به‌صورت رندومِ عادلانه تعیین کن —
    مستقل از ساعت/شیفت. هر آلارم تیمی همیشه یه مسئول می‌گیره.
    اعضایی که از پنل ادمین موقتاً «غیرفعال» (مرخصی/در دسترس نیستن) شدن،
    از این چرخه کنار گذاشته می‌شن — مگر این‌که همه غیرفعال باشن (اون موقع
    برای این‌که آلارم بی‌صاحب نمونه، بین کل اعضا تقسیم می‌شه).
    """
    unavailable = _get_unavailable_members()
    pool = [m for m in TEAM_MEMBERS if m not in unavailable] or TEAM_MEMBERS
    assignee = _pick_assignee(pool)
    threading.Thread(
        target=_sb_save_assignment,
        args=(alarm_id, alarm_tag, assignee, "", fired_at),
        kwargs={"symbol": symbol, "target_price": target_price, "created_by": created_by},
        daemon=True
    ).start()
    return (assignee, "")

# ─── startup: بازسازی state از Supabase ─────────────────────────────

def _sb_restore_on_startup():
    """
    بعد از هر restart، شمارش آلارم‌های فعال هر عضو رو از Supabase بازسازی کن
    تا تقسیم رندومِ عادلانه بدون از دست دادن state ادامه پیدا کنه.
    دیگه هیچ شیفت/handover/scheduler‌ای وجود نداره.
    """
    global _deprioritize_masoud_active, _unavailable_members, _muted_instant_senders, _gold_bot_subscribers
    rows = _sb_load_active_assignments()
    _rebuild_active_assign_count(rows)
    _rebuild_daily_assign_count()
    with _deprioritize_masoud_lock:
        _deprioritize_masoud_active = _sb_load_deprioritize_masoud()
    with _unavailable_members_lock:
        _unavailable_members = set(_sb_load_unavailable_members())
    with _muted_instant_lock:
        _muted_instant_senders = set(n.lower() for n in _sb_load_muted_instant_senders())
    with _gold_bot_lock:
        _gold_bot_subscribers = set(str(x) for x in _sb_load_gold_bot_subscribers())
    print(f"[assign] startup: {len(rows)} آلارم active از Supabase بازسازی شد — "
          f"اولویت پایین مسعود: {'فعال' if _deprioritize_masoud_active else 'غیرفعال'} — "
          f"غیرفعال‌ها: {sorted(_unavailable_members) or 'هیچ‌کس'} — "
          f"آلارم فوری بی‌صدا: {sorted(_muted_instant_senders) or 'هیچ‌کس'} — "
          f"مشترکین ربات طلا: {len(_gold_bot_subscribers)}")



def _track_msg(chat_id: str, msg_id: int):
    """id پیام ربات رو ذخیره کن (غیر از fired alerts)"""
    cid = str(chat_id)
    if cid not in _bot_msg_ids:
        _bot_msg_ids[cid] = []
    _bot_msg_ids[cid].append(msg_id)
    # سقف حافظه
    if len(_bot_msg_ids[cid]) > _BOT_MSG_MAX:
        _bot_msg_ids[cid] = _bot_msg_ids[cid][-_BOT_MSG_MAX:]

def delete_chat_history(token: str, chat_id: str):
    """همه پیام‌های track‌شده رو پاک کن — آلارم‌های fire دست‌نخورده می‌مونن"""
    cid = str(chat_id)
    ids = _bot_msg_ids.pop(cid, [])
    deleted = 0
    for mid in ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/deleteMessage",
                json={"chat_id": cid, "message_id": mid},
                timeout=5, headers=H)
            deleted += 1
        except: pass
    return deleted

def send_tg(token, chat_id, text):
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}, timeout=10, headers=H)
        mid = r.json().get("result", {}).get("message_id")
        if mid:
            _track_msg(str(chat_id), mid)
        return r.status_code == 200
    except: return False

def broadcast(token, chat_ids, text):
    return [send_tg(token, c, text) for c in chat_ids]

def send_reply_keyboard(token, chat_id, text, rows):
    """ارسال پیام با Reply Keyboard (جای کیبورد موبایل)"""
    try:
        markup = {
            "keyboard": rows,
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "input_field_placeholder": "یه گزینه انتخاب کن..."
        }
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": str(chat_id), "text": text,
                  "parse_mode": "HTML", "reply_markup": markup},
            timeout=10, headers=H)
        mid = r.json().get("result", {}).get("message_id")
        if mid:
            _track_msg(str(chat_id), mid)
        return mid
    except: return None

def edit_reply_keyboard(token, chat_id, message_id, text, rows=None):
    """ادیت پیام با Reply Keyboard یا بدون keyboard — برای flow ثبت آلارم"""
    try:
        if rows is not None:
            markup = {"keyboard": rows, "resize_keyboard": True, "one_time_keyboard": False}
        else:
            markup = {"remove_keyboard": True}
        # editMessageText نمیتونه reply_markup از نوع ReplyKeyboard داشته باشه
        # پس اول پیام رو ادیت میکنیم، بعد اگه keyboard جدید داریم یه پیام کمکی میفرستیم
        requests.post(
            f"https://api.telegram.org/bot{token}/editMessageText",
            json={"chat_id": str(chat_id), "message_id": message_id,
                  "text": text, "parse_mode": "HTML"},
            timeout=10, headers=H)
    except: pass

def _alarm_edit(token, cid, bot_msg_id, text):
    """ادیت پیام جاری flow آلارم (فقط متن، بدون keyboard)"""
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/editMessageText",
            json={"chat_id": str(cid), "message_id": bot_msg_id,
                  "text": text, "parse_mode": "HTML"},
            timeout=10, headers=H)
    except Exception as e:
        print(f"[alarm_edit] {e}")


    """حذف Reply Keyboard و ارسال پیام"""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": str(chat_id), "text": text,
                  "parse_mode": "HTML",
                  "reply_markup": {"remove_keyboard": True}},
            timeout=10, headers=H)
        mid = r.json().get("result", {}).get("message_id")
        if mid:
            _track_msg(str(chat_id), mid)
        return mid
    except: return None

def send_tg_keyboard(token, chat_id, text, keyboard, track=True):
    """ارسال پیام با inline keyboard"""
    try:
        payload = {"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}
        if keyboard is not None:
            payload["reply_markup"] = {"inline_keyboard": keyboard}
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json=payload, timeout=10, headers=H)
        if r.status_code != 200:
            print(f"[send_tg_keyboard] fail {r.status_code} {r.text[:200]}")
            return None
        mid = r.json().get("result", {}).get("message_id")
        print(f"[send_tg_keyboard] ok chat={chat_id} mid={mid} rows={len(keyboard or [])}")
        if mid and track:
            _track_msg(str(chat_id), mid)
        return mid
    except Exception as e:
        print(f"[send_tg_keyboard] exc: {e}")
        return None

def edit_tg_keyboard(token, chat_id, message_id, text, keyboard):
    """ویرایش پیام با inline keyboard"""
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/editMessageText",
            json={"chat_id": str(chat_id), "message_id": message_id,
                  "text": text, "parse_mode": "HTML",
                  "reply_markup": {"inline_keyboard": keyboard}},
            timeout=10, headers=H)
    except: pass

def answer_callback(token, callback_id, text=""):
    """جواب به callback query"""
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text},
            timeout=10, headers=H)
    except: pass

def _delete_msg_after(token, cid, msg_id, delay=120):
    """پیام رو بعد از delay ثانیه پاک کن"""
    def _do():
        time.sleep(delay)
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/deleteMessage",
                json={"chat_id": cid, "message_id": msg_id},
                timeout=10, headers=H)
        except: pass
    threading.Thread(target=_do, daemon=True).start()

# =====================================================================
# 📡 سیستم سیگنال
# =====================================================================

SIGNAL_CHANNEL = os.environ.get("SIGNAL_CHANNEL", "")  # مثلاً @mychannel یا chat_id

def _sb_next_signal_seq():
    """شماره سیگنال بعدی — از آخرین seq در جدول +1"""
    if not SUPABASE_KEY: return int(time.time()) % 100000
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/signals?select=seq&order=seq.desc&limit=1",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            data = r.json()
            if data and data[0].get("seq") is not None:
                return int(data[0]["seq"]) + 1
        return 10001  # جدول خالیه
    except:
        return 10001

def _sb_save_signal(sig: dict):
    """ذخیره سیگنال در Supabase — فقط فیلدهای جدول"""
    if not SUPABASE_KEY: return
    # فقط فیلدهایی که در جدول signals وجود دارن
    allowed = {"id","seq","symbol","direction","entry","sl","tp1","tp2","tp3",
               "tf","risk_pips","rr","sent_by","sent_at","channel_msg_id","status","note"}
    clean = {k: v for k, v in sig.items() if k in allowed}
    # مقادیر None رو برای tp2/tp3 به null تبدیل کن
    for f in ("tp2","tp3","channel_msg_id","note"):
        if f not in clean:
            clean[f] = None
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/signals",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=clean, timeout=10)
        if r.status_code not in (200, 201, 204):
            print(f"[signal] save error: {r.status_code} {r.text[:200]}")
        else:
            print(f"[signal] saved OK: {clean.get('id')}")
    except Exception as e:
        print(f"[signal] save exc: {e}")

def _sb_update_signal(sig_id, patch: dict):
    """آپدیت یه فیلد سیگنال"""
    if not SUPABASE_KEY: return
    try:
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/signals?id=eq.{sig_id}",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json=patch, timeout=8)
    except: pass

def _sb_load_signals(limit=10):
    """آخرین سیگنال‌ها رو بخون"""
    if not SUPABASE_KEY: return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/signals?select=*&order=sent_at.desc&limit={limit}",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200: return r.json()
    except: pass
    return []

def _calc_signal(symbol: str, direction: str, entry: float, sl: float, rr: float = 1.5):
    """
    محاسبه TP بر اساس Entry، SL و ریوارد.
    direction: buy_limit / buy_stop / sell_limit / sell_stop
    برمیگردونه: (sl_calc, tp1, risk_pips)
    """
    is_buy = direction.startswith("buy")
    risk = abs(entry - sl)
    mul = get_pip_multiplier(symbol)
    risk_pips = round(risk * mul, 1)
    if is_buy:
        tp1 = round(entry + risk * rr, 5)
    else:
        tp1 = round(entry - risk * rr, 5)
    return sl, tp1, risk_pips

def _sl_from_pips(symbol: str, direction: str, entry: float, pips: float):
    """محاسبه SL از روی پیپ"""
    mul = get_pip_multiplier(symbol)
    dist = pips / mul
    is_buy = direction.startswith("buy")
    sl = round(entry - dist if is_buy else entry + dist, 5)
    return sl

def _fmt_signal_price(p, symbol=""):
    """فرمت عدد برای نمایش در سیگنال"""
    if p is None: return "—"
    v = float(p)
    su = symbol.upper()
    if any(x in su for x in ['BTC','ETH','SOL','BNB']):
        return f"{v:.1f}" if v > 1000 else f"{v:.4f}"
    if "XAU" in su or "XAG" in su: return f"{v:.2f}"
    if "JPY" in su: return f"{v:.3f}"
    return f"{v:.5f}"

def _build_signal_text(sig: dict) -> str:
    """ساخت متن سیگنال — عین فرمت درخواستی، اعداد قابل کپی"""
    sym     = sig.get("symbol","")
    d       = sig.get("direction","")
    entry   = sig.get("entry")
    sl      = sig.get("sl")
    tp1     = sig.get("tp1")
    tp2     = sig.get("tp2")
    tp3     = sig.get("tp3")
    tf      = sig.get("tf","H1")
    sig_id  = sig.get("id","")

    dir_map = {
        "buy_limit":  "✅ Buy limit",
        "buy_stop":   "✅ Buy stop",
        "sell_limit": "🔴 Sell limit",
        "sell_stop":  "🔴 Sell stop",
    }
    dir_txt = dir_map.get(d, d)

    def c(val):
        if val is None: return "<code>-</code>"
        return f"<code>{_fmt_signal_price(val, sym)}</code>"

    tp2_txt = c(tp2) if tp2 else "<code>-</code>"
    tp3_txt = c(tp3) if tp3 else "<code>-</code>"

    return (
        f"#{sig_id}\n"
        f"#{sym}\n"
        f"{dir_txt}\n"
        f"➡️ Entry: {c(entry)}\n"
        f"🛑 SL: {c(sl)}\n"
        f"🎯 TP:\n"
        f"TP1: {c(tp1)}\n"
        f"TP2: {tp2_txt}\n"
        f"TP3: {tp3_txt}\n"
        f"⏱ Timeframe: {tf}"
    )

def _build_signal_preview(sig: dict) -> str:
    """پیش‌نمایش سیگنال — عین چیزی که به کانال میره"""
    return _build_signal_text(sig)

# state ثبت سیگنال در حال ساخت
_pending_signal = {}  # cid → {"step": str, "data": dict, "bot_msg_id": int}

SIGNAL_QUICK_SYMBOLS = ["BTCUSDT", "XAUUSD", "EURUSD", "GBPUSD", "ETHUSDT"]
SIGNAL_DIRECTIONS = [
    ("✅ Buy Limit",  "buy_limit"),
    ("✅ Buy Stop",   "buy_stop"),
    ("🔴 Sell Limit", "sell_limit"),
    ("🔴 Sell Stop",  "sell_stop"),
]
SIGNAL_TF_OPTIONS = ["M5", "M15", "M30", "H1", "H4", "D1"]
SIGNAL_DEFAULT_TF = "H1"
SIGNAL_DEFAULT_RR = 1.5

def _show_signal_preview(token, cid, mid, data):
    """نمایش پیش‌نمایش سیگنال با دکمه‌های ویرایش"""
    tf  = data.get("tf", SIGNAL_DEFAULT_TF)
    note = data.get("note","")
    note_line = f"\n\n📝 <i>{note}</i>" if note else ""
    preview = f"<b>── پیش‌نمایش ──</b>\n\n{_build_signal_text(data)}{note_line}"
    kb = [
        [{"text": f"⏱ TF: {tf}", "callback_data": f"sig_tf:{cid}"},
         {"text": "🎯 TP2/TP3", "callback_data": f"sig_tp:{cid}"}],
        [{"text": "📝 یادداشت", "callback_data": f"sig_note:{cid}"},
         {"text": "🔄 ریوارد", "callback_data": f"sig_recalc:{cid}"}],
        [{"text": "📤 ارسال به گروه", "callback_data": f"sig_send:{cid}:channel"},
         {"text": "💾 ثبت در دیتا", "callback_data": f"sig_send:{cid}:dbonly"}],
        [{"text": "❌ لغو", "callback_data": f"sig_cancel:{cid}"}],
    ]
    edit_tg_keyboard(token, cid, mid, preview, kb)

def build_myalerts_msg(cid):
    """متن و keyboard لیست آلارم‌های شخصی"""
    alerts = load_alerts().get("alerts", [])
    my = [a for a in alerts if a.get("is_private") and str(a.get("private_cid","")) == cid and a.get("active")]
    if not my:
        return "📭 هیچ آلارم شخصی فعالی نداری.", []
    lines = ["🔒 <b>آلارم‌های شخصی تو:</b>"]
    keyboard = []
    for i, a in enumerate(my, 1):
        sym = a.get("symbol","")
        tgt = a.get("target_price",0)
        cond = "📈 BUY" if a.get("condition") == "below" else "📉 SELL"
        cur2 = a.get("last_price")
        cur_txt = f"<code>{fmt_price(cur2, sym)}</code>" if cur2 else "—"
        cmt = f"\n│  💬 {a['comment']}" if a.get("comment") else ""
        lines.append(
            f"┌─ {i}. <b>{sym}</b>  {cond}\n"
            f"│  🎯 هدف: <code>{tgt}</code>\n"
            f"│  💹 فعلی: {cur_txt}"
            f"{cmt}\n"
            f"└──────────────"
        )
        keyboard.append([{"text": f"🗑 حذف  {sym} @ {tgt}", "callback_data": f"del_confirm:{a['id']}"}])
    keyboard.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
    return "\n".join(lines), keyboard

def _get_token_and_cids():
    data = load_alerts()
    tg = data.get("telegram", {})
    token = BOT_TOKEN_ENV or tg.get("bot_token", "")
    cids = list(tg.get("chat_ids", []))
    leg = tg.get("chat_id", "")
    if leg and leg not in [str(x) for x in cids]:
        cids.append(leg)
    return token, cids, data


FF_NEWS_HOUR = int(os.environ.get("NEWS_HOUR", "7"))   # ساعت ارسال روزانه (تهران)
FF_NEWS_MINUTE = int(os.environ.get("NEWS_MINUTE", "0"))

def fetch_ff_news():
    """
    تقویم اقتصادی ForexFactory رو از RSS می‌گیره.
    فقط رویدادهای USD با impact بالا (⭐⭐⭐) برمی‌گردونه.
    """
    try:
        import xml.etree.ElementTree as ET
        from datetime import timezone
        r = requests.get(
            "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
            headers={**H, "User-Agent": "Mozilla/5.0"},
            timeout=10)
        if r.status_code == 200:
            events = r.json()
        else:
            # fallback: RSS
            r2 = requests.get("https://www.forexfactory.com/ffcal_week_this.xml",
                headers={**H, "User-Agent": "Mozilla/5.0"}, timeout=10)
            if r2.status_code != 200:
                return None, "❌ دریافت داده از ForexFactory ناموفق بود"
            root = ET.fromstring(r2.content)
            events = []
            for ev in root.findall("event"):
                events.append({
                    "title": ev.findtext("title",""),
                    "country": ev.findtext("country",""),
                    "date": ev.findtext("date",""),
                    "time": ev.findtext("time",""),
                    "impact": ev.findtext("impact",""),
                    "forecast": ev.findtext("forecast",""),
                    "previous": ev.findtext("previous",""),
                })

        # فیلتر: فقط رویدادهای high/medium impact — همه ارزها — امروز
        today_teh = datetime.now(TEHRAN).strftime("%Y-%m-%d")
        high_events = []
        for ev in events:
            impact = ev.get("impact","").lower()
            if impact not in ("high","medium","3","2"):
                continue
            ev_date = ev.get("date","")
            try:
                from datetime import datetime as dt
                parsed = dt.strptime(ev_date, "%Y-%m-%dT%H:%M:%S%z")
                ev_date_teh = parsed.astimezone(TEHRAN).strftime("%Y-%m-%d")
                if ev_date_teh != today_teh:
                    continue
                ev["time_teh"] = parsed.astimezone(TEHRAN).strftime("%H:%M")
            except:
                if today_teh not in ev_date:
                    continue
                ev["time_teh"] = ev.get("time","—")
            high_events.append(ev)

        # مرتب‌سازی بر اساس ساعت
        high_events.sort(key=lambda x: x.get("time_teh","99:99"))

        if not high_events:
            return [], "📭 امروز رویداد مهم فارکس نداریم."

        return high_events, None

    except Exception as e:
        return None, f"❌ خطا: {e}"


def format_ff_message(events):
    """پیام تلگرام رو فرمت می‌کنه"""
    today_str = datetime.now(TEHRAN).strftime("%A %d %B %Y")
    lines = [f"📅 <b>تقویم اقتصادی فارکس — امروز</b>\n{today_str}\n"]
    for ev in events:
        impact = ev.get("impact","").lower()
        star = "🔴" if impact in ("high","3") else "🟡"
        time_str = ev.get("time_teh") or ev.get("time","—")
        title = ev.get("title","—")
        forecast = ev.get("forecast","—") or "—"
        previous = ev.get("previous","—") or "—"
        country = ev.get("country","").upper()
        flag_map = {"USD":"🇺🇸","EUR":"🇪🇺","GBP":"🇬🇧","JPY":"🇯🇵","CAD":"🇨🇦",
                    "AUD":"🇦🇺","NZD":"🇳🇿","CHF":"🇨🇭","CNY":"🇨🇳","GER":"🇩🇪"}
        flag = flag_map.get(country, "🌐")
        lines.append(
            f"{star} {flag} <b>{title}</b>\n"
            f"   🕐 {time_str} (تهران)\n"
            f"   پیش‌بینی: <b>{forecast}</b>  |  قبلی: {previous}"
        )
    return "\n\n".join(lines)


def daily_news_scheduler():
    """هر روز سر ساعت NEWS_HOUR تهران اخبار می‌فرسته + ۱۵ دقیقه قبل از هر خبر قرمز هشدار می‌فرسته"""
    sent_today = None
    global _today_red_events, _today_red_events_date, _news_reminder_sent
    # موقع استارت، وضعیت رو از Supabase بازیابی کن (اگه اپ ری‌استارت شده باشه)
    try:
        saved = _sb_load_news_reminder_state()
        if saved.get("date") == datetime.now(TEHRAN).strftime("%Y-%m-%d"):
            _today_red_events = saved.get("events", [])
            _today_red_events_date = datetime.now(TEHRAN).date()
            _news_reminder_sent = set(tuple(k) for k in saved.get("sent", []))
            print(f"[news] وضعیت هشدار از Supabase بازیابی شد — {len(_today_red_events)} خبر قرمز")
    except Exception as e:
        print(f"[news] بازیابی وضعیت ناموفق: {e}")

    while True:
        try:
            now = datetime.now(TEHRAN)
            today = now.date()
            if (now.hour == FF_NEWS_HOUR and now.minute == FF_NEWS_MINUTE
                    and sent_today != today):
                token, cids, _ = _get_token_and_cids()
                if token and cids:
                    events, err = fetch_ff_news()
                    if err and not events:
                        msg = err
                    else:
                        msg = format_ff_message(events) if events else "📭 امروز رویداد مهم فارکس نداریم."
                    broadcast(token, cids, msg)
                    sent_today = today
                    print(f"[news] ارسال شد — {len(events or [])} رویداد")
                    # فقط اخبار قرمز (impact بالا) رو برای هشدار ۱۵ دقیقه‌ای نگه می‌داریم
                    _today_red_events = [ev for ev in (events or [])
                                          if ev.get("impact","").lower() in ("high","3")
                                          and ev.get("country","").upper() == "USD"]
                    _today_red_events_date = today
                    _news_reminder_sent = set()
                    _sb_save_news_reminder_state({
                        "date": today.strftime("%Y-%m-%d"),
                        "events": _today_red_events,
                        "sent": [],
                    })

            _check_news_reminders(now)
        except Exception as e:
            print(f"[news_scheduler] {e}")
        time.sleep(50)


_today_red_events = []
_today_red_events_date = None
_news_reminder_sent = set()

def _check_news_reminders(now):
    """۱۵ دقیقه قبل از هر خبر قرمزِ امروز، یه هشدار به تیم می‌فرسته."""
    global _news_reminder_sent
    if _today_red_events_date != now.date() or not _today_red_events:
        return
    for ev in _today_red_events:
        time_teh = ev.get("time_teh")
        if not time_teh or ":" not in time_teh:
            continue
        key = (now.date().strftime("%Y-%m-%d"), ev.get("title",""), time_teh)
        if key in _news_reminder_sent:
            continue
        try:
            hh, mm = map(int, time_teh.split(":"))
            ev_dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        except Exception:
            continue
        reminder_dt = ev_dt - timedelta(minutes=15)
        if reminder_dt <= now < reminder_dt + timedelta(seconds=60):
            token, cids, _ = _get_token_and_cids()
            if token and cids:
                msg = (
                    "🚨🔴⚠️🚨 هشدار خبر مهم 🚨⚠️🔴🚨\n\n"
                    f"📅 {ev.get('title','—')}\n"
                    f"⏰ ۱۵ دقیقه دیگر ({time_teh})\n\n"
                    "❗️ با توجه به پلن، تریدهای باز داخل حساب باید مدیریت شوند.\n\n"
                    "🔔 لطفاً هرکی این پیام رو می‌بینه به این مورد توجه کنه 🙏"
                )
                broadcast(token, cids, msg)
                print(f"[news_reminder] ارسال شد — {ev.get('title')} ساعت {time_teh}")
            _news_reminder_sent.add(key)
            _sb_save_news_reminder_state({
                "date": now.date().strftime("%Y-%m-%d"),
                "events": _today_red_events,
                "sent": [list(k) for k in _news_reminder_sent],
            })

_pending_name  = {}  # cid → True
_pending_alarm = {}  # cid → {"step": str, "data": dict}
_pending_bulk_false = {}  # cid → {"bot_msg_id": ..., "count": ...} — منتظر متن دلیل فالس‌کردن دسته‌جمعی
_pending_weekly_search = {}  # cid → {"which": str, "msg_id": int}
# آلارم_آیدی → {chat_id: message_id آخرین پیام False/آپدیت}
_false_broadcast_ids: dict = {}

# ── Reply Keyboard rows ──────────────────────────────────────
MAIN_MENU = [
    ["📈 آلارم جدید"],
    ["⭐ آلارم‌های من", "📊 وضعیت"],
    ["⚡ آلارم فوری"],
    ["📡 سیگنال جدید"],
]
MAIN_MENU_PRIVATE = [
    ["📈 آلارم جدید",  "🔒 آلارم شخصی"],
    ["⭐ آلارم‌های من", "📊 وضعیت"],
    ["⚡ آلارم فوری"],
    ["📡 سیگنال جدید"],
]
MAIN_MENU_ADMIN = [
    ["📈 آلارم جدید",  "🔒 آلارم شخصی"],
    ["⭐ آلارم‌های من", "📊 وضعیت"],
    ["⚡ آلارم فوری"],
    ["📡 سیگنال جدید", "⚙️ پنل ادمین"],
]
# منوی محدود برای اعضای جدید تا وقتی که ادمین تاییدشون نکرده
MAIN_MENU_NEW = [
    ["📊 وضعیت"],
    ["📩 درخواست فعال‌سازی آلارم"],
]
DIR_MENU = [["📈 BUY", "📉 SELL"], ["❌ انصراف"]]

def show_main_menu(token, cid, text, is_admin=False):
    if is_admin:
        rows = MAIN_MENU_ADMIN
    elif not _is_approved(cid):
        rows = MAIN_MENU_NEW
    elif _has_private_access(cid):
        rows = MAIN_MENU_PRIVATE
    else:
        rows = MAIN_MENU
    send_reply_keyboard(token, cid, text, rows)

def _get_user_custom_name(cid):
    data = load_alerts()
    for u in data.get("users", []):
        if str(u.get("chat_id","")) == str(cid):
            return u.get("custom_name","") or u.get("username","")
    return ""

def _has_private_access(cid):
    """آیا این کاربر دسترسی آلارم شخصی داره؟ فقط ادمین و کسایی که تایید شدن"""
    if str(cid) == str(YOUR_CHAT_ID):
        return True
    data = load_alerts()
    for u in data.get("users", []):
        if str(u.get("chat_id","")) == str(cid):
            return bool(u.get("private_access", False))
    return False

def _is_approved(cid):
    """آیا این کاربر اجازه‌ی استفاده کامل (ثبت آلارم و بقیه امکانات) داره؟
    کاربرهای قدیمی که فیلد approved ندارن به صورت پیش‌فرض تاییدشده حساب می‌شن،
    فقط اعضای جدید (که این فیلد رو صریحاً False دارن) باید تایید ادمین رو بگیرن."""
    if str(cid) == str(YOUR_CHAT_ID):
        return True
    data = load_alerts()
    for u in data.get("users", []):
        if str(u.get("chat_id","")) == str(cid):
            return bool(u.get("approved", True))
    return False

def _is_name_approved(name: str, pin: str = "") -> bool:
    """نسخه‌ی مبتنی بر اسم _is_approved — برای گیت کردن ثبت آلارم از سایت (وب)،
    جایی که chat_id نداریم و فقط اسمی که کاربر توی سایت انتخاب کرده در دسترسه.
    اسم با custom_name کاربرهای تلگرام مطابقت داده می‌شه.
    برای کاربرهایی که از این به بعد از طریق «درخواست فعال‌سازی» تایید می‌شن، یک کد (web_pin) هم
    باید مطابقت داشته باشه — تا صرفاً تایپ کردن اسمِ یک نفر دیگه کافی برای دور زدن تایید نباشه.
    کاربرهای قدیمی که web_pin ندارن (چون قبل از این قابلیت ثبت‌نام کردن) طبق روال قبل فقط با تطابق اسم عبور می‌کنن."""
    name = (name or "").strip().lower()
    if not name:
        return False
    data = load_alerts()
    for u in data.get("users", []):
        un = (u.get("custom_name") or "").strip().lower()
        if un and un == name:
            required_pin = u.get("web_pin")
            if required_pin:
                return str(pin or "").strip() == str(required_pin)
            return bool(u.get("approved", True))
    # اسمی که به هیچ کاربر تاییدشده‌ی تلگرام تطابق نداشته باشه (یعنی هنوز از تلگرام تاییدیه نگرفته) — اجازه نداره
    return False

def _verified_user_by_name(name: str, pin: str = ""):
    """مثل _is_name_approved ولی به‌جای True/False، خودِ رکورد کاربر رو برمی‌گردونه (یا None اگه
    هویتش تایید نشه). برای جاهایی که بعد از تایید هویت نیاز به فیلدهای دیگه‌ی کاربر
    (private_access, full_access, chat_id) داریم."""
    name_lc = (name or "").strip().lower()
    if not name_lc:
        return None
    data = load_alerts()
    for u in data.get("users", []):
        un = (u.get("custom_name") or "").strip().lower()
        if un and un == name_lc:
            required_pin = u.get("web_pin")
            if required_pin:
                if str(pin or "").strip() != str(required_pin):
                    return None
                return u
            if bool(u.get("approved", True)):
                return u
            return None
    return None


@app.route("/api/me", methods=["GET"])
def get_me():
    """وضعیت یه کاربر (بر اساس اسم+کد) — تا سایت بفهمه کدوم دکمه‌ها (مثلاً آلارم شخصی) رو نشونش بده"""
    name = request.args.get("name", "").strip()
    pin = request.args.get("pin", "").strip()
    u = _verified_user_by_name(name, pin)
    if not u:
        return jsonify({"ok": True, "verified": False, "private_access": False, "full_access": False})
    return jsonify({
        "ok": True,
        "verified": True,
        "private_access": bool(u.get("private_access")) or str(u.get("chat_id", "")) == str(YOUR_CHAT_ID),
        "full_access": bool(u.get("full_access")),
    })


def _get_sender_name(msg):
    """اسم فرستنده — اول custom_name، بعد اسم تلگرام"""
    u = msg.get("from", {})
    cid = str(msg.get("chat", {}).get("id", "") or u.get("id", ""))
    if cid:
        users = load_alerts().get("users", [])
        for usr in users:
            if str(usr.get("chat_id", "")) == cid and usr.get("custom_name"):
                return usr["custom_name"]
    fn = u.get("first_name", "")
    ln = u.get("last_name", "")
    un = u.get("username", "")
    return (fn + " " + ln).strip() or ("@" + un if un else "ناشناس")


def _build_myalerts_section(alerts_list, title, start_idx=1):
    """ساخت متن و keyboard برای نمایش لیست آلارم"""
    if not alerts_list:
        return None, []
    lines = [f"{title}"]
    kb = []
    for i, a in enumerate(alerts_list, start_idx):
        sym2  = a.get("symbol","")
        tgt2  = a.get("target_price", 0)
        cond2 = "📈 BUY" if a.get("condition") == "below" else "📉 SELL"
        cur2  = a.get("last_price")
        cur_txt = f"<code>{fmt_price(cur2, sym2)}</code>" if cur2 else "—"
        cmt2  = f"\n💬 {a['comment']}" if a.get("comment") else ""
        block = (
            f"┌─ {i}. <b>{sym2}</b>  {cond2}\n"
            f"│  🎯 هدف: <code>{fmt_price(tgt2, sym2)}</code>\n"
            f"│  💹 فعلی: {cur_txt}"
            f"{cmt2}\n"
            f"└──────────────"
        )
        lines.append(block)
        kb.append([{"text": f"🗑 {i}. {sym2} {cond2} @ {fmt_price(tgt2, sym2)}", "callback_data": f"del_confirm:{a['id']}"}])
    kb.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
    return "\n".join(lines), kb

def poll_telegram():
    last_id = 0
    _poll_round = 0
    while True:
        try:
            token, _, _ = _get_token_and_cids()
            if not token:
                time.sleep(30)
                continue
            _poll_round += 1
            if _poll_round == 1 or _poll_round % 30 == 0:
                try:
                    rw = requests.get(
                        f"https://api.telegram.org/bot{token}/deleteWebhook",
                        params={"drop_pending_updates": "false"}, timeout=15)
                    print(f"[poll] deleteWebhook: {rw.status_code} {rw.text[:120]}")
                except Exception as e:
                    print(f"[poll] deleteWebhook err: {e}")
            r = requests.get(
                f"https://api.telegram.org/bot{token}/getUpdates",
                params={
                    "offset": last_id + 1,
                    "timeout": 25,
                    "limit": 100,
                    "allowed_updates": json.dumps(["message", "callback_query", "edited_message"]),
                },
                timeout=35, headers=H)
            if r.status_code == 409:
                print(f"[poll] 409 Conflict — یه instance دیگه هم polling می‌کنه: {r.text[:200]}")
                time.sleep(5)
                continue
            if r.status_code != 200:
                print(f"[poll_telegram] getUpdates خطا داد: {r.status_code} — {r.text[:300]}")
                time.sleep(10)
                continue
            _all_updates = r.json().get("result", [])
            for upd in _all_updates:
                last_id = upd["update_id"]
                if "callback_query" in upd:
                    print(f"[poll] CALLBACK data={upd['callback_query'].get('data')!r} from={upd['callback_query'].get('from',{}).get('id')}")
                elif "message" in upd:
                    t = (upd["message"].get("text") or "")[:40]
                    if t:
                        print(f"[poll] MSG text={t!r}")
            for upd in _all_updates:
                threading.Thread(target=_do_update, args=(upd, token), daemon=True).start()
        except Exception as e:
            print(f"[poll] {e}")
            time.sleep(5)

def _do_update(upd, token):
  try:
                # ── callback query (دکمه‌های inline) ─────────────────
                cbq = upd.get("callback_query", {})
                if cbq:
                    cbq_id = cbq.get("id","")
                    cbq_data = cbq.get("data","") or ""
                    cbq_cid = str(cbq.get("from",{}).get("id","") or cbq.get("message",{}).get("chat",{}).get("id",""))
                    cbq_msg_id = cbq.get("message",{}).get("message_id")
                    token_cbq, _, _ = _get_token_and_cids()
                    print(f"[cbq] data={cbq_data!r} from={cbq_cid} msg={cbq_msg_id}")

                    if cbq_data.startswith("rate:"):
                        parts_rt = cbq_data.split(":")
                        rt_aid = parts_rt[1] if len(parts_rt) > 1 else ""
                        try:
                            rt_score = int(parts_rt[2]) if len(parts_rt) > 2 else -1
                        except ValueError:
                            rt_score = -1
                        print(f"[rating] click aid={rt_aid} score={rt_score} cid={cbq_cid}")
                        if not rt_aid or rt_score not in (0, 1, 2, 3, 4, 5):
                            answer_callback(token_cbq, cbq_id, "امتیاز نامعتبر")
                        else:
                            clicker_id_rt = str(cbq.get("from", {}).get("id", ""))
                            voter_name_rt = TEAM_ID_TO_NAME.get(clicker_id_rt) or _get_user_custom_name(clicker_id_rt) or _get_user_custom_name(cbq_cid) or ("user_" + clicker_id_rt)
                            if voter_name_rt not in TEAM_MEMBERS:
                                mapped = next((m for m in TEAM_MEMBERS if _norm_name(m) == _norm_name(voter_name_rt)), None)
                                if mapped:
                                    voter_name_rt = mapped
                            print(f"[rating] voter resolved name={voter_name_rt!r} id={clicker_id_rt} in_team={voter_name_rt in TEAM_MEMBERS}")
                            # قفل per-alert: ۳ نفر همزمان → یکی‌یکی پردازش می‌شن، بدون گم‌شدن رای
                            with _rating_lock_for(rt_aid):
                                msg_map_rt = _load_fired_msg_map(rt_aid)
                                creator_rt = msg_map_rt.get("__creator__", "")
                                if not creator_rt and SUPABASE_KEY:
                                    try:
                                        r_cr = requests.get(
                                            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.{rt_aid}&select=created_by",
                                            headers=_sb_h(), timeout=6)
                                        if r_cr.status_code == 200 and r_cr.json():
                                            creator_rt = r_cr.json()[0].get("created_by") or ""
                                    except Exception:
                                        pass
                                creator_rt = _resolve_creator_name(creator_rt)
                                if msg_map_rt.get("__rating_cancelled__") == "1":
                                    answer_callback(token_cbq, cbq_id, "آلارم false شده — رأی‌گیری لغو شد")
                                elif voter_name_rt in TEAM_MEMBERS and _norm_name(voter_name_rt) == _norm_name(creator_rt):
                                    answer_callback(token_cbq, cbq_id, "سازنده نمی‌تونه رای بده")
                                elif msg_map_rt.get("__rating_closed__") == "1":
                                    answer_callback(token_cbq, cbq_id, "رای‌گیری تموم شده")
                                else:
                                    existing_votes = _sb_load_ratings(rt_aid)
                                    my_prev = next((v for v in existing_votes if _norm_name(v.get("voter_name","")) == _norm_name(voter_name_rt) or str(v.get("voter_cid")) == clicker_id_rt), None)
                                    if my_prev is not None and int(my_prev.get("score") or 0) == rt_score and (rt_aid, clicker_id_rt) not in _rate_editing:
                                        answer_callback(token_cbq, cbq_id, "همین رای رو قبلاً دادی")
                                    else:
                                        st_chk = _compute_rating_state(existing_votes, creator_rt)
                                        if st_chk["closed"]:
                                            answer_callback(token_cbq, cbq_id, "رای‌گیری تموم شده")
                                        else:
                                            _prev_reason = ((my_prev.get("reason") or "") if my_prev else "")
                                            if not _sb_save_rating(rt_aid, clicker_id_rt, voter_name_rt, rt_score, _prev_reason, creator_rt):
                                                answer_callback(token_cbq, cbq_id, "❌ ثبت نشد، دوباره بزن")
                                            else:
                                                answer_callback(token_cbq, cbq_id, ("رای به %s تغییر کرد" if my_prev else "امتیاز %s ثبت شد") % rt_score)
                                                _rate_editing.discard((rt_aid, clicker_id_rt))
                                                _extra = (cbq_cid, cbq_msg_id)
                                                _fb_text = (cbq.get("message") or {}).get("text") or (cbq.get("message") or {}).get("caption") or ""
                                                print(f"[rating] before refresh map_keys={list((_fired_msg_ids.get(rt_aid) or {}).keys())} fb_len={len(_fb_text)}")
                                                state_after = _refresh_rating_messages(
                                                    token_cbq, rt_aid, creator_rt,
                                                    extra_chat_msg=_extra, fallback_text=_fb_text)
                                                # اگه این نفر هنوز پرامپت دلیلِ باز برای آلارم دیگه‌ای داشت، پاکش کن
                                                _old_pr = _pending_rate.pop(clicker_id_rt, None)
                                                if isinstance(_old_pr, dict):
                                                    if _old_pr.get("prompt_mid"):
                                                        _delete_msg_after(token_cbq, cbq_cid, _old_pr["prompt_mid"], delay=0)
                                                    if _old_pr.get("alert_id"):
                                                        threading.Thread(target=_rate_try_notify, args=(token_cbq, _old_pr["alert_id"], _old_pr.get("creator", "")), daemon=True).start()
                                                # پرامپت دلیل (با دکمه رد کردن) — «ثبت شد» بعد از نوشتن دلیل میاد
                                                _prompt_txt = "📝 <b>دلیل امتیاز %s/5</b> رو بنویس (اختیاری)" % rt_score
                                                if _prev_reason.strip():
                                                    _prompt_txt += "\nکامنت فعلی: <i>%s</i>\n(رد کنی، همین می‌مونه)" % _prev_reason.strip()
                                                _prompt_mid = send_tg_keyboard(
                                                    token_cbq, cbq_cid, _prompt_txt,
                                                    [[{"text": "⏭ رد کردن", "callback_data": "rate_skip:%s" % rt_aid}]])
                                                _pending_rate[clicker_id_rt] = {
                                                    "alert_id": rt_aid, "score": rt_score,
                                                    "voter_name": voter_name_rt, "creator": creator_rt,
                                                    "already_saved": True,
                                                    "ts": time.time(), "prompt_mid": _prompt_mid,
                                                    "closer": bool(state_after and state_after.get("closed")),
                                                }
                                                _pending_rate[cbq_cid] = _pending_rate[clicker_id_rt]
                                                # نتیجه‌ی نهایی بعد از دلیل (یا رد کردنِ) آخرین رأی‌دهنده میاد؛ تایمر برای وقتیه که اون هم کاری نکنه
                                                if state_after and state_after.get("closed"):
                                                    _tm_rt = threading.Timer(RATE_NOTIFY_WAIT + 3, _rate_try_notify, args=(token_cbq, rt_aid, creator_rt))
                                                    _tm_rt.daemon = True
                                                    _tm_rt.start()

                    elif cbq_data.startswith("rate_edit:") or cbq_data.startswith("rate_edit_cancel:"):
                        # ورود به حالت ویرایش (دکمه‌های امتیاز دوباره باز می‌شن) / انصراف از ویرایش
                        ed_cancel = cbq_data.startswith("rate_edit_cancel:")
                        ed_aid = cbq_data.split(":", 1)[1] if ":" in cbq_data else ""
                        ed_clicker = str(cbq.get("from", {}).get("id", ""))
                        if not ed_aid:
                            answer_callback(token_cbq, cbq_id, "نامعتبر")
                        else:
                            mm_ed = _load_fired_msg_map(ed_aid)
                            votes_ed = _sb_load_ratings(ed_aid)
                            st_ed = _compute_rating_state(votes_ed, _resolve_creator_name(mm_ed.get("__creator__", "")))
                            if mm_ed.get("__rating_cancelled__") == "1":
                                answer_callback(token_cbq, cbq_id, "آلارم false شده — رأی‌گیری لغو شد")
                            elif mm_ed.get("__rating_closed__") == "1":
                                answer_callback(token_cbq, cbq_id, "رای‌گیری تموم شده")
                            elif ed_cancel:
                                _rate_editing.discard((ed_aid, ed_clicker))
                                answer_callback(token_cbq, cbq_id, "انصراف داده شد")
                                _edit_markup(token_cbq, cbq_cid, cbq_msg_id, _rating_keyboard_for(ed_aid, st_ed, cbq_cid))
                            elif not any(str(v.get("voter_cid")) == ed_clicker for v in votes_ed):
                                answer_callback(token_cbq, cbq_id, "اول باید رای بدی")
                            else:
                                _rate_editing.add((ed_aid, ed_clicker))
                                answer_callback(token_cbq, cbq_id, "✏️ رای یا کامنت جدیدت رو بزن")
                                _edit_markup(token_cbq, cbq_cid, cbq_msg_id, _rating_keyboard_for(ed_aid, st_ed, cbq_cid))

                    elif cbq_data.startswith("rate_comment:"):
                        # ویرایش فقط کامنت: فقط کسی که رای داده و تا وقتی رای‌گیری بازه
                        re_aid = cbq_data.split(":", 1)[1] if ":" in cbq_data else ""
                        re_clicker = str(cbq.get("from", {}).get("id", ""))
                        re_name = TEAM_ID_TO_NAME.get(re_clicker) or _get_user_custom_name(re_clicker) or _get_user_custom_name(cbq_cid) or ("user_" + re_clicker)
                        if not re_aid:
                            answer_callback(token_cbq, cbq_id, "نامعتبر")
                        else:
                            mm_re = _load_fired_msg_map(re_aid)
                            if mm_re.get("__rating_cancelled__") == "1":
                                answer_callback(token_cbq, cbq_id, "آلارم false شده — رأی‌گیری لغو شد")
                            elif mm_re.get("__rating_closed__") == "1":
                                answer_callback(token_cbq, cbq_id, "رای‌گیری تموم شده")
                            else:
                                votes_re = _sb_load_ratings(re_aid)
                                mine_re = next((v for v in votes_re if _norm_name(v.get("voter_name", "")) == _norm_name(re_name) or str(v.get("voter_cid")) == re_clicker), None)
                                if mine_re is None:
                                    answer_callback(token_cbq, cbq_id, "اول باید رای بدی")
                                else:
                                    answer_callback(token_cbq, cbq_id, "✏️ کامنت جدیدت رو بنویس")
                                    _old_re = _pending_rate.pop(re_clicker, None)
                                    if isinstance(_old_re, dict) and _old_re.get("prompt_mid"):
                                        _delete_msg_after(token_cbq, cbq_cid, _old_re["prompt_mid"], delay=0)
                                    cur_re = (mine_re.get("reason") or "").strip()
                                    txt_re = "✏️ <b>ویرایش کامنت</b> (رای فعلی: %s/5)" % int(mine_re.get("score") or 0)
                                    if cur_re:
                                        txt_re += "\nکامنت فعلی: <i>%s</i>" % cur_re
                                    txt_re += "\n\nکامنت جدید رو بنویس («-» بفرستی کامنت پاک می‌شه)"
                                    mid_re = send_tg_keyboard(token_cbq, cbq_cid, txt_re,
                                                              [[{"text": "⏭ رد کردن", "callback_data": "rate_skip:%s" % re_aid}]])
                                    _pending_rate[re_clicker] = {
                                        "alert_id": re_aid, "score": int(mine_re.get("score") or 0),
                                        "voter_name": mine_re.get("voter_name") or re_name,
                                        "creator": _resolve_creator_name(mm_re.get("__creator__", "")),
                                        "already_saved": True, "ts": time.time(), "prompt_mid": mid_re, "closer": False,
                                    }
                                    _pending_rate[cbq_cid] = _pending_rate[re_clicker]
                                    _rate_editing.discard((re_aid, re_clicker))
                                    _edit_markup(token_cbq, cbq_cid, cbq_msg_id,
                                                 _rating_keyboard_for(re_aid, _compute_rating_state(votes_re, _resolve_creator_name(mm_re.get("__creator__", ""))), cbq_cid))

                    elif cbq_data.startswith("rate_skip:"):
                        rs_aid = cbq_data.split(":", 1)[1] if ":" in cbq_data else ""
                        answer_callback(token_cbq, cbq_id, "رد شد")
                        rs_creator = ""
                        _rs_pr = _pending_rate.get(cbq_cid)
                        if isinstance(_rs_pr, dict) and _rs_pr.get("alert_id") == rs_aid:
                            rs_creator = _rs_pr.get("creator", "")
                            _pending_rate.pop(cbq_cid, None)
                        if cbq_msg_id:
                            _delete_msg_after(token_cbq, cbq_cid, cbq_msg_id, delay=0)
                        if rs_aid:
                            threading.Thread(target=_rate_try_notify, args=(token_cbq, rs_aid, rs_creator), daemon=True).start()

                    elif cbq_data.startswith("rate_close:") or cbq_data.startswith("rate_close_no:"):
                        # مرحله‌ی تایید: فقط کیبوردِ همین پیام عوض می‌شه (برای بقیه‌ی اعضا چیزی تغییر نمی‌کنه)
                        rq_aid = cbq_data.split(":", 1)[1] if ":" in cbq_data else ""
                        rq_clicker = str(cbq.get("from", {}).get("id", ""))
                        rq_ask = cbq_data.startswith("rate_close:")
                        if not rq_aid:
                            answer_callback(token_cbq, cbq_id, "نامعتبر")
                        elif rq_clicker not in TEAM_ID_TO_NAME:
                            answer_callback(token_cbq, cbq_id, "فقط اعضای تیم می‌تونن ببندن")
                        else:
                            mm_rq = _load_fired_msg_map(rq_aid)
                            creator_rq = _resolve_creator_name(mm_rq.get("__creator__", ""))
                            st_rq = _compute_rating_state(_sb_load_ratings(rq_aid), creator_rq)
                            if mm_rq.get("__rating_cancelled__") == "1":
                                answer_callback(token_cbq, cbq_id, "آلارم false شده — رأی‌گیری لغو شد")
                            elif mm_rq.get("__rating_closed__") == "1":
                                answer_callback(token_cbq, cbq_id, "رای‌گیری تموم شده")
                            elif rq_ask and st_rq["voters_count"] == 0:
                                answer_callback(token_cbq, cbq_id, "هنوز هیچ رایی ثبت نشده")
                            else:
                                if rq_ask:
                                    answer_callback(token_cbq, cbq_id, "⚠️ مطمئنی؟ با رای‌های فعلی بسته و محاسبه می‌شه")
                                    kb_rq = [[
                                        {"text": "✅ بله، ببند و محاسبه کن", "callback_data": f"rate_close_yes:{rq_aid}"},
                                        {"text": "↩️ انصراف", "callback_data": f"rate_close_no:{rq_aid}"},
                                    ]]
                                else:
                                    answer_callback(token_cbq, cbq_id, "انصراف داده شد")
                                    kb_rq = _rating_keyboard_for(rq_aid, st_rq, cbq_cid)
                                try:
                                    requests.post(f"https://api.telegram.org/bot{token_cbq}/editMessageReplyMarkup",
                                                  json={"chat_id": str(cbq_cid), "message_id": cbq_msg_id,
                                                        "reply_markup": {"inline_keyboard": kb_rq}},
                                                  timeout=10, headers=H)
                                except Exception as e:
                                    print(f"[rating] close confirm kb exc: {e}")

                    elif cbq_data.startswith("rate_close_yes:"):
                        rc_aid = cbq_data.split(":", 1)[1] if ":" in cbq_data else ""
                        rc_clicker = str(cbq.get("from", {}).get("id", ""))
                        if not rc_aid:
                            answer_callback(token_cbq, cbq_id, "نامعتبر")
                        elif rc_clicker not in TEAM_ID_TO_NAME:
                            answer_callback(token_cbq, cbq_id, "فقط اعضای تیم می‌تونن ببندن")
                        else:
                            with _rating_lock_for(rc_aid):
                                mm_rc = _load_fired_msg_map(rc_aid)
                                creator_rc = _resolve_creator_name(mm_rc.get("__creator__", ""))
                                if mm_rc.get("__rating_cancelled__") == "1":
                                    answer_callback(token_cbq, cbq_id, "آلارم false شده — رأی‌گیری لغو شد")
                                elif mm_rc.get("__rating_closed__") == "1":
                                    answer_callback(token_cbq, cbq_id, "رای‌گیری تموم شده")
                                elif _compute_rating_state(_sb_load_ratings(rc_aid), creator_rc)["voters_count"] == 0:
                                    answer_callback(token_cbq, cbq_id, "هنوز هیچ رایی ثبت نشده")
                                else:
                                    mm_rc["__rating_closed__"] = "1"
                                    mm_rc["__rating_manual__"] = "1"
                                    _fired_msg_ids[rc_aid] = mm_rc
                                    try:
                                        _sb_save_fired_msgs(rc_aid, mm_rc)
                                    except Exception as e:
                                        print(f"[rating] manual close save exc: {e}")
                                    answer_callback(token_cbq, cbq_id, "🔒 رای‌گیری بسته شد")
                                    _fb_rc = (cbq.get("message") or {}).get("text") or (cbq.get("message") or {}).get("caption") or ""
                                    _refresh_rating_messages(token_cbq, rc_aid, creator_rc,
                                                             extra_chat_msg=(cbq_cid, cbq_msg_id), fallback_text=_fb_rc)
                                    threading.Thread(target=_rate_try_notify, args=(token_cbq, rc_aid, creator_rc), daemon=True).start()

                    elif cbq_data.startswith("del_confirm:"):
                        # مرحله اول: نمایش تأیید حذف
                        aid = cbq_data.split(":",1)[1]
                        d_conf = load_alerts()
                        a_conf = next((a for a in d_conf["alerts"] if a["id"] == aid), None)
                        answer_callback(token_cbq, cbq_id)
                        if a_conf:
                            sym_c = a_conf.get("symbol","")
                            tgt_c = a_conf.get("target_price", 0)
                            cond_c = "📈 BUY" if a_conf.get("condition") == "below" else "📉 SELL"
                            confirm_text = (
                                f"⚠️ <b>آیا مطمئنی؟</b>\n\n"
                                f"میخوای این آلارم رو حذف کنی:\n"
                                f"<b>{sym_c}</b>  {cond_c}  @ <code>{fmt_price(tgt_c, sym_c)}</code>"
                            )
                            confirm_kb = [
                                [{"text": "✅ بله، حذف کن", "callback_data": f"del_alert:{aid}"}],
                                [{"text": "❌ نه، برگرد", "callback_data": f"del_cancel:{aid}"}],
                            ]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, confirm_text, confirm_kb)
                        else:
                            answer_callback(token_cbq, cbq_id, "⚠️ آلارم پیدا نشد")

                    elif cbq_data.startswith("del_cancel:"):
                        # برگشت به لیست آلارم‌ها بعد از انصراف از حذف
                        answer_callback(token_cbq, cbq_id, "❌ حذف لغو شد")
                        d_can = load_alerts()
                        all_a_can = d_can.get("alerts", [])
                        custom_name_can = _get_user_custom_name(cbq_cid)
                        pub_can  = [a for a in all_a_can if a.get("active") and not a.get("is_private") and a.get("created_by","") == custom_name_can]
                        priv_can = [a for a in all_a_can if a.get("active") and a.get("is_private") and (
                            str(a.get("private_cid","")) == cbq_cid or str(a.get("notify_only","")) == cbq_cid
                        )]
                        combined_can = pub_can + priv_can
                        if pub_can and not priv_can:
                            txt_can, kb_can = _build_myalerts_section(pub_can, f"🌐 <b>آلارم‌های تیمی</b>  ({len(pub_can)} مورد)")
                        elif priv_can and not pub_can:
                            txt_can, kb_can = _build_myalerts_section(priv_can, f"🔒 <b>آلارم‌های شخصی</b>  ({len(priv_can)} مورد)")
                        elif combined_can:
                            txt_can, kb_can = _build_myalerts_section(pub_can, f"📋 <b>همه آلارم‌های من</b>  ({len(combined_can)} مورد)", start_idx=1)
                            priv_txt_can, priv_kb_can = _build_myalerts_section(priv_can, "\n🔒 <b>شخصی</b>", start_idx=len(pub_can)+1)
                            if priv_txt_can:
                                txt_can = txt_can + "\n" + priv_txt_can
                                kb_can = kb_can[:-1] + priv_kb_can
                        else:
                            txt_can, kb_can = "📭 هیچ آلارم فعالی نداری.", [[{"text":"✕ بستن","callback_data":"close_myalerts"}]]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_can, kb_can)

                    elif cbq_data.startswith("del_alert:"):
                        # مرحله دوم: حذف واقعی بعد از تأیید
                        aid = cbq_data.split(":",1)[1]
                        d = load_alerts()
                        a_del = next((a for a in d["alerts"] if a["id"] == aid), None)
                        before = len(d["alerts"])
                        d["alerts"] = [a for a in d["alerts"] if a["id"] != aid]
                        if len(d["alerts"]) < before:
                            _cache_alerts = d
                            answer_callback(token_cbq, cbq_id, "✅ آلارم حذف شد")
                            threading.Thread(target=_sb_delete_alert, args=(aid,), daemon=True).start()
                        else:
                            answer_callback(token_cbq, cbq_id, "⚠️ آلارم پیدا نشد")
                        # بازسازی لیست در همان پیام — بدون sleep، بدون پیام جدید
                        d2 = load_alerts()
                        all_a2 = d2.get("alerts", [])
                        custom_name2 = _get_user_custom_name(cbq_cid)
                        pub2  = [a for a in all_a2 if a.get("active") and not a.get("is_private") and a.get("created_by","") == custom_name2]
                        priv2 = [a for a in all_a2 if a.get("active") and a.get("is_private") and (
                            str(a.get("private_cid","")) == cbq_cid or str(a.get("notify_only","")) == cbq_cid
                        )]
                        combined2 = pub2 + priv2
                        if not combined2:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ آلارم فعالی نداری.", [])
                        elif pub2 and not priv2:
                            txt2, kb2 = _build_myalerts_section(pub2, f"🌐 <b>آلارم‌های تیمی</b>  ({len(pub2)} مورد)")
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt2, kb2)
                        elif priv2 and not pub2:
                            txt2, kb2 = _build_myalerts_section(priv2, f"🔒 <b>آلارم‌های شخصی</b>  ({len(priv2)} مورد)")
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt2, kb2)
                        else:
                            txt2, kb2 = _build_myalerts_section(pub2, f"📋 <b>همه آلارم‌های من</b>  ({len(combined2)} مورد)", start_idx=1)
                            # اضافه کردن آلارم‌های شخصی به لیست ترکیبی
                            txt2_lines = txt2.split("\n")
                            priv_txt, priv_kb = _build_myalerts_section(priv2, f"\n🔒 <b>شخصی</b>", start_idx=len(pub2)+1)
                            if priv_txt:
                                txt2 = txt2 + "\n" + priv_txt
                                kb2 = kb2[:-1] + priv_kb  # بستن رو از pub حذف کن، priv_kb خودش داره
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt2, kb2)

                    elif cbq_data.startswith("ack_trigger:"):
                        # ack_trigger:alarm_id:expected_user_id — فقط مسئول تریگر می‌تونه بزنه
                        parts_ack = cbq_data.split(":", 2)
                        ack_aid = parts_ack[1] if len(parts_ack) > 1 else ""
                        ack_expected_id = parts_ack[2] if len(parts_ack) > 2 else ""
                        clicker_id = str(cbq.get("from", {}).get("id", ""))
                        if not ack_expected_id or clicker_id != ack_expected_id:
                            answer_callback(token_cbq, cbq_id, "⛔ این دکمه فقط برای مسئول تریگره")
                        else:
                            # جلوگیری از دابل-ack — اگه دابل‌تپ شده یا قبلاً پردازش شده، دومی رو نادیده بگیر
                            with _ack_lock:
                                _already_acked = ack_aid in _ack_done
                                if not _already_acked:
                                    _ack_done.add(ack_aid)
                            if _already_acked:
                                answer_callback(token_cbq, cbq_id, "✅ قبلاً ثبت شده")
                            else:
                                answer_callback(token_cbq, cbq_id, "✅ ثبت شد")
                                ack_name = TEAM_ID_TO_NAME.get(clicker_id, "")
                                ack_time_label = now_pretty()

                                def _do_ack(aid=ack_aid, name=ack_name, tlabel=ack_time_label):
                                    # لایه‌ی دوم ایمنی — حتی اگه سرور بین دو کلیک ری‌استارت شده باشه
                                    # (که _ack_done در حافظه رو ریست می‌کنه)، از خود دیتابیس هم چک کن
                                    if SUPABASE_KEY:
                                        try:
                                            r_chk_ack = requests.get(
                                                f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{aid}&select=ack_at",
                                                headers=_sb_h(), timeout=8)
                                            if r_chk_ack.status_code == 200:
                                                chk_rows_ack = r_chk_ack.json()
                                                if chk_rows_ack and chk_rows_ack[0].get("ack_at"):
                                                    print(f"[ack] {aid} از قبل تو DB ack شده بود — skip")
                                                    return
                                        except Exception as e:
                                            print(f"[ack] pre-check exc: {e}")
                                        try:
                                            requests.patch(
                                                f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{aid}",
                                                headers={**_sb_h(), "Prefer": "return=minimal"},
                                                json={"ack_at": now_teh(), "ack_by": name},
                                                timeout=8)
                                        except Exception as e:
                                            print(f"[ack] save exc: {e}")
                                    # ادیت پیام برای همه‌ی چت‌هایی که این آلارم توشون ارسال شده
                                    tok_ack, _, _ = _get_token_and_cids()
                                    msg_map_ack = _fired_msg_ids.get(aid, {})
                                    # اگه تو حافظه نبود (ری‌استارت / لود ناقص) مستقیم از Supabase بخون
                                    if not msg_map_ack.get("__text__") and SUPABASE_KEY:
                                        try:
                                            r_fm = requests.get(
                                                f"{SUPABASE_URL}/rest/v1/fired_msgs?id=eq.{aid}&select=msg_map",
                                                headers=_sb_h(), timeout=8)
                                            if r_fm.status_code == 200 and r_fm.json():
                                                raw_fm = r_fm.json()[0].get("msg_map") or "{}"
                                                msg_map_ack = json.loads(raw_fm) if isinstance(raw_fm, str) else (raw_fm or {})
                                        except Exception as e:
                                            print(f"[ack] fired_msgs fetch exc: {e}")
                                    base_text = msg_map_ack.get("__text__", "")
                                    if not base_text:
                                        print(f"[ack] {aid} — map پیام‌ها پیدا نشد، ادیت انجام نشد")
                                        return
                                    ack_line = f"\n✅ <b>{name}</b> قبول کرد در ساعت {tlabel}"
                                    new_text = base_text + ack_line
                                    # هر آلارم یه نماد ثابت داره — از تگ متن استخراج می‌کنیم نه از هر پیام
                                    sym_ack = _extract_symbol_from_tag(msg_map_ack.get("__tag__", ""))
                                    for tc_ack, tm_ack in msg_map_ack.items():
                                        if tc_ack in ("__tag__", "__text__"):
                                            continue
                                        new_kb = {"inline_keyboard": []}
                                        try:
                                            r_ed = requests.post(
                                                f"https://api.telegram.org/bot{tok_ack}/editMessageText",
                                                json={"chat_id": tc_ack, "message_id": tm_ack,
                                                      "text": new_text, "parse_mode": "HTML",
                                                      "reply_markup": new_kb},
                                                timeout=8, headers=H)
                                            if r_ed.status_code != 200:
                                                print(f"[ack] edit fail {tc_ack}: {r_ed.status_code} {r_ed.text[:150]}")
                                        except Exception as e:
                                            print(f"[ack] edit exc: {e}")
                                    # آپدیت متن ذخیره‌شده تا اگه دوباره چیزی edit بشه این تغییر از دست نره
                                    msg_map_ack["__text__"] = new_text
                                    _fired_msg_ids[aid] = msg_map_ack
                                    threading.Thread(target=_sb_save_fired_msgs, args=(aid, msg_map_ack), daemon=True).start()

                                threading.Thread(target=_do_ack, daemon=True).start()

                    elif cbq_data.startswith("myalerts:"):
                        # myalerts:pub:CID / myalerts:priv:CID / myalerts:all:CID
                        parts_mya = cbq_data.split(":", 2)
                        mya_type = parts_mya[1] if len(parts_mya) > 1 else "all"
                        mya_cid  = parts_mya[2] if len(parts_mya) > 2 else cbq_cid
                        answer_callback(token_cbq, cbq_id)
                        d_mya = load_alerts()
                        all_a_mya = d_mya.get("alerts", [])
                        custom_name_mya = _get_user_custom_name(mya_cid)
                        pub_mya  = [a for a in all_a_mya if a.get("active") and not a.get("is_private") and a.get("created_by","") == custom_name_mya]
                        # برای آلارم شخصی: هم private_cid هم notify_only چک کن
                        priv_mya = [a for a in all_a_mya if a.get("active") and a.get("is_private") and (
                            str(a.get("private_cid","")) == mya_cid or str(a.get("notify_only","")) == mya_cid
                        )]
                        if mya_type == "pub":
                            txt_mya, kb_mya = _build_myalerts_section(pub_mya, f"🌐 <b>آلارم‌های تیمی</b>  ({len(pub_mya)} مورد)")
                            if not txt_mya:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ آلارم تیمی فعالی نداری.", [[{"text":"✕ بستن","callback_data":"close_myalerts"}]])
                            else:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya, kb_mya)
                        elif mya_type == "priv":
                            txt_mya, kb_mya = _build_myalerts_section(priv_mya, f"🔒 <b>آلارم‌های شخصی</b>  ({len(priv_mya)} مورد)")
                            if not txt_mya:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ آلارم شخصی فعالی نداری.", [[{"text":"✕ بستن","callback_data":"close_myalerts"}]])
                            else:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya, kb_mya)
                        else:  # all
                            combined_mya = pub_mya + priv_mya
                            if not combined_mya:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ آلارم فعالی نداری.", [[{"text":"✕ بستن","callback_data":"close_myalerts"}]])
                            elif pub_mya and not priv_mya:
                                txt_mya_all, kb_mya_all = _build_myalerts_section(pub_mya, f"🌐 <b>آلارم‌های تیمی</b>  ({len(pub_mya)} مورد)")
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya_all, kb_mya_all)
                            elif priv_mya and not pub_mya:
                                txt_mya_all, kb_mya_all = _build_myalerts_section(priv_mya, f"🔒 <b>آلارم‌های شخصی</b>  ({len(priv_mya)} مورد)")
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya_all, kb_mya_all)
                            else:
                                txt_mya_all, kb_mya_all = _build_myalerts_section(pub_mya, f"📋 <b>همه آلارم‌های من</b>  ({len(combined_mya)} مورد)\n\n🌐 <b>تیمی</b>", start_idx=1)
                                priv_txt_all, priv_kb_all = _build_myalerts_section(priv_mya, "\n🔒 <b>شخصی</b>", start_idx=len(pub_mya)+1)
                                if priv_txt_all:
                                    txt_mya_all = txt_mya_all + "\n" + priv_txt_all
                                    kb_mya_all = kb_mya_all[:-1] + priv_kb_all
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya_all, kb_mya_all)

                    elif cbq_data == "admin:news" and cbq_cid == YOUR_CHAT_ID:
                        answer_callback(token_cbq, cbq_id, "در حال دریافت اخبار...")
                        def _bg_news(tok=token_cbq, c=cbq_cid):
                            result = fetch_ff_news()
                            msg_text = result[1] if isinstance(result, tuple) else str(result)
                            send_tg(tok, c, msg_text)
                        threading.Thread(target=_bg_news, daemon=True).start()

                    elif cbq_data == "admin:broadcast" and cbq_cid == YOUR_CHAT_ID:
                        answer_callback(token_cbq, cbq_id)
                        _pending_alarm[cbq_cid] = {"step": "broadcast_text", "data": {}}
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=10, headers=H)
                        except: pass
                        send_tg(token_cbq, cbq_cid, "✉️ متن پیام رو بنویس:")

                    elif cbq_data.startswith("req_private:"):
                        # کاربر درخواست فعال‌سازی آلارم شخصی داده
                        req_cid = cbq_data.split(":")[1]
                        answer_callback(token_cbq, cbq_id, "✅ درخواست ارسال شد")
                        # پیام تایید به کاربر
                        send_tg(token_cbq, req_cid,
                            "📩 <b>درخواست شما ثبت شد</b>\n\n"
                            "درخواست فعال‌سازی آلارم شخصی شما ارسال شد و در دست بررسی است.\n"
                            "پس از تایید، یک پیام دریافت خواهید کرد. 🙏")
                        # پیام به ادمین
                        d_req = load_alerts()
                        req_user = next((u for u in d_req.get("users",[]) if str(u.get("chat_id","")) == req_cid), {})
                        req_name = req_user.get("custom_name","") or req_user.get("username","") or req_cid
                        admin_notif = (
                            f"📩 <b>درخواست آلارم شخصی</b>\n\n"
                            f"👤 نام: <b>{req_name}</b>\n"
                            f"🆔 Chat ID: <code>{req_cid}</code>\n"
                            f"⏰ {now_pretty()} (تهران)"
                        )
                        approve_kb = [
                            [{"text": "✅ تایید و فعال‌سازی", "callback_data": f"approve_private:{req_cid}"}],
                            [{"text": "❌ رد درخواست",       "callback_data": f"reject_private:{req_cid}"}],
                        ]
                        send_tg_keyboard(token_cbq, YOUR_CHAT_ID, admin_notif, approve_kb)

                    elif cbq_data.startswith("approve_private:"):
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            target_cid = cbq_data.split(":")[1]
                            answer_callback(token_cbq, cbq_id, "✅ فعال شد")
                            # آپدیت private_access توی users
                            d_apr = load_alerts()
                            found = False
                            for u in d_apr.get("users", []):
                                if str(u.get("chat_id","")) == target_cid:
                                    u["private_access"] = True
                                    found = True
                                    break
                            if not found:
                                d_apr.setdefault("users",[]).append({"chat_id": target_cid, "username": "", "joined_at": now_pretty(), "custom_name": "", "private_access": True})
                            save_alerts(d_apr)
                            # ویرایش پیام ادمین
                            try:
                                requests.post(f"https://api.telegram.org/bot{token_cbq}/editMessageReplyMarkup",
                                    json={"chat_id": cbq_cid, "message_id": cbq_msg_id, "reply_markup": {"inline_keyboard": []}},
                                    timeout=10, headers=H)
                            except: pass
                            send_tg(token_cbq, cbq_cid, f"✅ آلارم شخصی برای <code>{target_cid}</code> فعال شد.")
                            # پیام به کاربر
                            send_tg(token_cbq, target_cid,
                                "🎉 <b>آلارم شخصی شما فعال شد!</b>\n\n"
                                "یکبار /start بزنید تا منو به‌روز شود. 🔒")

                    elif cbq_data.startswith("reject_private:"):
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            target_cid = cbq_data.split(":")[1]
                            answer_callback(token_cbq, cbq_id, "❌ رد شد")
                            try:
                                requests.post(f"https://api.telegram.org/bot{token_cbq}/editMessageReplyMarkup",
                                    json={"chat_id": cbq_cid, "message_id": cbq_msg_id, "reply_markup": {"inline_keyboard": []}},
                                    timeout=10, headers=H)
                            except: pass
                            send_tg(token_cbq, cbq_cid, f"❌ درخواست <code>{target_cid}</code> رد شد.")
                            send_tg(token_cbq, target_cid,
                                "❌ <b>درخواست شما رد شد.</b>\n\n"
                                "برای اطلاعات بیشتر با ادمین تماس بگیرید.")

                    elif cbq_data.startswith("approve_signup:"):
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            target_cid_su = cbq_data.split(":")[1]
                            answer_callback(token_cbq, cbq_id, "✅ فعال شد")
                            web_pin_su = f"{secrets.randbelow(1000000):06d}"
                            d_apr_su = load_alerts()
                            found_su = False
                            for u in d_apr_su.get("users", []):
                                if str(u.get("chat_id","")) == target_cid_su:
                                    u["approved"] = True
                                    u["web_pin"] = web_pin_su
                                    found_su = True
                                    break
                            if not found_su:
                                d_apr_su.setdefault("users",[]).append({"chat_id": target_cid_su, "username": "", "joined_at": now_pretty(), "custom_name": "", "approved": True, "web_pin": web_pin_su})
                            save_alerts(d_apr_su)
                            try:
                                requests.post(f"https://api.telegram.org/bot{token_cbq}/editMessageReplyMarkup",
                                    json={"chat_id": cbq_cid, "message_id": cbq_msg_id, "reply_markup": {"inline_keyboard": []}},
                                    timeout=10, headers=H)
                            except: pass
                            send_tg(token_cbq, cbq_cid, f"✅ عضو <code>{target_cid_su}</code> تایید و فعال شد.")
                            send_tg(token_cbq, target_cid_su,
                                "🎉 <b>درخواست شما تایید شد!</b>\n\n"
                                "یکبار /start بزنید تا منو به‌روز شود. ✅\n\n"
                                f"🔑 کد فعال‌سازی سایت شما: <code>{web_pin_su}</code>\n"
                                "برای ثبت آلارم از طریق سایت، این کد رو یک‌بار توی سایت (دکمه «🔑 کد فعال‌سازی» زیر نام کاربری) وارد کن.")

                    elif cbq_data.startswith("reject_signup:"):
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            target_cid_su = cbq_data.split(":")[1]
                            answer_callback(token_cbq, cbq_id, "❌ رد شد")
                            try:
                                requests.post(f"https://api.telegram.org/bot{token_cbq}/editMessageReplyMarkup",
                                    json={"chat_id": cbq_cid, "message_id": cbq_msg_id, "reply_markup": {"inline_keyboard": []}},
                                    timeout=10, headers=H)
                            except: pass
                            send_tg(token_cbq, cbq_cid, f"❌ درخواست <code>{target_cid_su}</code> رد شد.")
                            send_tg(token_cbq, target_cid_su,
                                "❌ <b>درخواست شما رد شد.</b>\n\n"
                                "برای اطلاعات بیشتر با ادمین تماس بگیرید.")

                    elif cbq_data.startswith("admin:users"):
                        # لیست کاربران برای ادمین
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            answer_callback(token_cbq, cbq_id)
                            d_usr = load_alerts()
                            all_users = d_usr.get("users", [])
                            if not all_users:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ کاربری ثبت نشده.", [[{"text": "✕ بستن", "callback_data": "close_myalerts"}]])
                            else:
                                lines_u = ["👥 <b>لیست کاربران</b>\n"]
                                kb_u = []
                                for u in all_users:
                                    ucid = str(u.get("chat_id",""))
                                    uname_u = u.get("custom_name","") or u.get("username","") or ucid
                                    priv_icon = "🔒" if u.get("private_access") else "👤"
                                    lines_u.append(f"{priv_icon} <b>{uname_u}</b>  <code>{ucid}</code>")
                                    kb_u.append([{"text": f"🗑 حذف {uname_u}", "callback_data": f"admin:deluser:{ucid}"}])
                                kb_u.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_u), kb_u)

                    elif cbq_data.startswith("admin:deluser:"):
                        # مرحله اول: تایید حذف
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            del_cid = cbq_data.split(":")[2]
                            answer_callback(token_cbq, cbq_id)
                            d_confirm = load_alerts()
                            u_confirm = next((u for u in d_confirm.get("users",[]) if str(u.get("chat_id","")) == del_cid), {})
                            uname_confirm = u_confirm.get("custom_name","") or u_confirm.get("username","") or del_cid
                            confirm_kb = [
                                [{"text": "✅ بله، حذف کن", "callback_data": f"admin:confirmdelete:{del_cid}"}],
                                [{"text": "❌ نه، برگشت",   "callback_data": "admin:users"}],
                            ]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"⚠️ <b>تایید حذف کاربر</b>\n\n"
                                f"👤 <b>{uname_confirm}</b>\n"
                                f"🆔 <code>{del_cid}</code>\n\n"
                                f"مطمئنی می‌خوای این کاربر رو حذف کنی؟",
                                confirm_kb)

                    elif cbq_data.startswith("admin:confirmdelete:"):
                        # مرحله دوم: انجام حذف
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            del_cid = cbq_data.split(":")[2]
                            answer_callback(token_cbq, cbq_id, "🗑 حذف شد")
                            d_del = load_alerts()
                            d_del["users"] = [u for u in d_del.get("users",[]) if str(u.get("chat_id","")) != del_cid]
                            d_del["telegram"]["chat_ids"] = [x for x in d_del["telegram"].get("chat_ids",[]) if str(x) != del_cid]
                            save_alerts(d_del)
                            all_users2 = d_del.get("users", [])
                            if not all_users2:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 لیست کاربران خالی شد.", [[{"text": "✕ بستن", "callback_data": "close_myalerts"}]])
                            else:
                                lines_u2 = ["👥 <b>لیست کاربران</b>\n"]
                                kb_u2 = []
                                for u in all_users2:
                                    ucid = str(u.get("chat_id",""))
                                    uname_u2 = u.get("custom_name","") or u.get("username","") or ucid
                                    priv_icon2 = "🔒" if u.get("private_access") else "👤"
                                    lines_u2.append(f"{priv_icon2} <b>{uname_u2}</b>  <code>{ucid}</code>")
                                    kb_u2.append([{"text": f"🗑 حذف {uname_u2}", "callback_data": f"admin:deluser:{ucid}"}])
                                kb_u2.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_u2), kb_u2)

                    elif cbq_data == "admin:users":
                        # برگشت به لیست کاربران (از صفحه تایید)
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            answer_callback(token_cbq, cbq_id)
                            d_back = load_alerts()
                            all_users_back = d_back.get("users", [])
                            if not all_users_back:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ کاربری ثبت نشده.", [[{"text": "✕ بستن", "callback_data": "close_myalerts"}]])
                            else:
                                lines_back = ["👥 <b>لیست کاربران</b>\n"]
                                kb_back = []
                                for u in all_users_back:
                                    ucid = str(u.get("chat_id",""))
                                    uname_back = u.get("custom_name","") or u.get("username","") or ucid
                                    priv_icon_back = "🔒" if u.get("private_access") else "👤"
                                    lines_back.append(f"{priv_icon_back} <b>{uname_back}</b>  <code>{ucid}</code>")
                                    kb_back.append([{"text": f"🗑 حذف {uname_back}", "callback_data": f"admin:deluser:{ucid}"}])
                                kb_back.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_back), kb_back)

                    elif cbq_data.startswith("admin_sig:"):
                        # فقط ادمین
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                            return
                        parts_as2 = cbq_data.split(":", 2)
                        as2_action = parts_as2[1]
                        as2_arg    = parts_as2[2] if len(parts_as2) > 2 else ""

                        if as2_action == "list":
                            answer_callback(token_cbq, cbq_id, "⏳ بارگذاری...")
                            page = int(as2_arg) if as2_arg.isdigit() else 1
                            per_page = 5
                            all_sigs = _sb_load_signals(limit=50)
                            total = len(all_sigs)
                            start_i = (page - 1) * per_page
                            page_sigs = all_sigs[start_i: start_i + per_page]
                            if not page_sigs:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    "🗑 <b>مدیریت سیگنال‌ها</b>\n\n📭 سیگنالی وجود ندارد.",
                                    [[{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}]])
                                return
                            lines_asl = [f"🗑 <b>مدیریت سیگنال‌ها</b>  ({total} سیگنال)\n"]
                            kb_asl = []
                            for s in page_sigs:
                                sid2   = s.get("id","")
                                sym2   = s.get("symbol","")
                                dir2   = s.get("direction","")
                                entry2 = s.get("entry")
                                tf2    = s.get("tf","")
                                by2    = s.get("sent_by","")
                                at2    = (s.get("sent_at") or "")[:16]
                                ch2    = s.get("channel_msg_id")
                                origin2 = "📤" if ch2 else "💾"
                                dir_short2 = {"buy_limit":"BL↗","buy_stop":"BS↗",
                                              "sell_limit":"SL↘","sell_stop":"SS↘"}.get(dir2, dir2)
                                lines_asl.append(
                                    f"{origin2} <b>{sid2}</b>  #{sym2}  {dir_short2}\n"
                                    f"   ➡️ <code>{_fmt_signal_price(entry2, sym2)}</code>  "
                                    f"⏱{tf2}  👤{by2}  🕐{at2}"
                                )
                                kb_asl.append([{"text": f"🗑 حذف {sid2} — {sym2} {dir_short2}",
                                                "callback_data": f"admin_sig:confirm:{sid2}:{page}"}])
                            # pagination
                            nav_row = []
                            if page > 1:
                                nav_row.append({"text": "◀️ قبل", "callback_data": f"admin_sig:list:{page-1}"})
                            if start_i + per_page < total:
                                nav_row.append({"text": "بعد ▶️", "callback_data": f"admin_sig:list:{page+1}"})
                            if nav_row:
                                kb_asl.append(nav_row)
                            kb_asl.append([{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}])
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                "\n".join(lines_asl), kb_asl)

                        elif as2_action == "confirm":
                            # as2_arg = "S10001:page"
                            conf_parts = as2_arg.split(":", 1)
                            conf_sid   = conf_parts[0]
                            conf_page  = conf_parts[1] if len(conf_parts) > 1 else "1"
                            answer_callback(token_cbq, cbq_id)
                            # اطلاعات سیگنال برای نمایش در تأیید
                            all_sigs_c = _sb_load_signals(limit=50)
                            sig_c = next((s for s in all_sigs_c if s.get("id") == conf_sid), None)
                            if not sig_c:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    "⚠️ سیگنال پیدا نشد.",
                                    [[{"text": "↩️ برگشت به لیست", "callback_data": f"admin_sig:list:{conf_page}"}]])
                                return
                            sym_c   = sig_c.get("symbol","")
                            dir_c   = sig_c.get("direction","")
                            entry_c = sig_c.get("entry")
                            by_c    = sig_c.get("sent_by","")
                            at_c    = (sig_c.get("sent_at") or "")[:16]
                            ch_c    = sig_c.get("channel_msg_id")
                            ch_warn = "\n\n⚠️ این سیگنال <b>به گروه ارسال شده</b> — حذف از DB فقط رکورد رو پاک میکنه، پیام کانال دست‌نخورده میمونه." if ch_c else ""
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"⚠️ <b>تأیید حذف سیگنال</b>\n\n"
                                f"🆔 <b>{conf_sid}</b>  #{sym_c}  {dir_c}\n"
                                f"➡️ Entry: <code>{_fmt_signal_price(entry_c, sym_c)}</code>\n"
                                f"👤 {by_c}  🕐 {at_c}"
                                f"{ch_warn}\n\n"
                                f"مطمئنی؟",
                                [[{"text": "✅ بله، حذف کن",    "callback_data": f"admin_sig:delete:{conf_sid}:{conf_page}"}],
                                 [{"text": "❌ نه، برگشت",       "callback_data": f"admin_sig:list:{conf_page}"}]])

                        elif as2_action == "delete":
                            del_parts  = as2_arg.split(":", 1)
                            del_sid    = del_parts[0]
                            del_page   = del_parts[1] if len(del_parts) > 1 else "1"
                            answer_callback(token_cbq, cbq_id, "🗑 در حال حذف...")
                            # حذف از Supabase
                            try:
                                r_del = requests.delete(
                                    f"{SUPABASE_URL}/rest/v1/signals?id=eq.{del_sid}",
                                    headers={**_sb_h(), "Prefer": "return=minimal"},
                                    timeout=10)
                                ok_del = r_del.status_code in (200, 204)
                            except Exception as e:
                                print(f"[admin_sig] delete error: {e}")
                                ok_del = False
                            if not ok_del:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    f"❌ خطا در حذف سیگنال <b>{del_sid}</b>.",
                                    [[{"text": "↩️ برگشت", "callback_data": f"admin_sig:list:{del_page}"}]])
                                return
                            # بعد از حذف، لیست آپدیت‌شده رو نشون بده
                            page_back = int(del_page) if del_page.isdigit() else 1
                            per_page2 = 5
                            all_sigs2 = _sb_load_signals(limit=50)
                            total2    = len(all_sigs2)
                            # اگه صفحه الان خالی شد، یه صفحه برگرد
                            start_i2 = (page_back - 1) * per_page2
                            if start_i2 >= total2 and page_back > 1:
                                page_back -= 1
                                start_i2 = (page_back - 1) * per_page2
                            page_sigs2 = all_sigs2[start_i2: start_i2 + per_page2]
                            if not page_sigs2:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    f"✅ سیگنال <b>{del_sid}</b> حذف شد.\n\n📭 سیگنال دیگری وجود ندارد.",
                                    [[{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}]])
                                return
                            lines_af = [f"✅ <b>{del_sid}</b> حذف شد.\n\n🗑 <b>مدیریت سیگنال‌ها</b>  ({total2} سیگنال)\n"]
                            kb_af = []
                            for s in page_sigs2:
                                sid_f   = s.get("id",""); sym_f2  = s.get("symbol","")
                                dir_f   = s.get("direction",""); entry_f = s.get("entry")
                                tf_f    = s.get("tf",""); by_f    = s.get("sent_by","")
                                at_f    = (s.get("sent_at") or "")[:16]
                                ch_f    = s.get("channel_msg_id")
                                orig_f  = "📤" if ch_f else "💾"
                                ds_f    = {"buy_limit":"BL↗","buy_stop":"BS↗",
                                           "sell_limit":"SL↘","sell_stop":"SS↘"}.get(dir_f, dir_f)
                                lines_af.append(
                                    f"{orig_f} <b>{sid_f}</b>  #{sym_f2}  {ds_f}\n"
                                    f"   ➡️ <code>{_fmt_signal_price(entry_f, sym_f2)}</code>  "
                                    f"⏱{tf_f}  👤{by_f}  🕐{at_f}"
                                )
                                kb_af.append([{"text": f"🗑 حذف {sid_f} — {sym_f2} {ds_f}",
                                               "callback_data": f"admin_sig:confirm:{sid_f}:{page_back}"}])
                            nav_f = []
                            if page_back > 1:
                                nav_f.append({"text": "◀️ قبل", "callback_data": f"admin_sig:list:{page_back-1}"})
                            if start_i2 + per_page2 < total2:
                                nav_f.append({"text": "بعد ▶️", "callback_data": f"admin_sig:list:{page_back+1}"})
                            if nav_f: kb_af.append(nav_f)
                            kb_af.append([{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}])
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_af), kb_af)

                        elif as2_action == "back":
                            answer_callback(token_cbq, cbq_id)
                            admin_kb_b = [
                                [{"text": "📰 اخبار فارکس",      "callback_data": "admin:news"}],
                                [{"text": "✉️ پیام به گروه",     "callback_data": "admin:broadcast"}],
                                [{"text": "👥 لیست کاربران",      "callback_data": "admin:users"}],
                                [{"text": "🗑 مدیریت سیگنال‌ها", "callback_data": "admin_sig:list:1"}],
                                # (غیرفعال — تقسیم آلارم کنسل شده) [{"text": "📋 مسئولین آلارم",        "callback_data": "admin:shift:1"}],
                                [{"text": "❌ فالس همه آلارم‌های فعال", "callback_data": "admin:bulkfalse:0"}],
                                [{"text": "✕ بستن",               "callback_data": "close_myalerts"}],
                            ]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                "⚙️ <b>پنل ادمین</b>\n\nیه گزینه انتخاب کن:", admin_kb_b)

                    # ─── فالس دسته‌جمعی همه‌ی آلارم‌های فعال ──────────────────
                    elif cbq_data.startswith("admin:bulkfalse"):
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            bf_parts = cbq_data.split(":")
                            bf_step = bf_parts[2] if len(bf_parts) > 2 else "0"

                            # مرحله ۰: نمایش تعداد و درخواست تایید
                            if bf_step == "0":
                                answer_callback(token_cbq, cbq_id, "⏳ شمارش...")
                                bf_rows = _sb_load_active_assignments()
                                bf_count = len(bf_rows)
                                if bf_count == 0:
                                    edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                        "📭 <b>هیچ آلارم فعالی نیست.</b>",
                                        [[{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}]])
                                else:
                                    lines_bf = [f"⚠️ <b>فالس دسته‌جمعی</b>\n\n{bf_count} آلارم فعال پیدا شد:\n"]
                                    for row_bf in bf_rows[:20]:
                                        lines_bf.append(f"• {row_bf.get('alarm_tag','')}  👤 {row_bf.get('assigned_to','—')}")
                                    if bf_count > 20:
                                        lines_bf.append(f"... و {bf_count-20} مورد دیگه")
                                    lines_bf.append(f"\nهمه‌ی این {bf_count} آلارم false می‌شن. مطمئنی؟")
                                    edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                        "\n".join(lines_bf),
                                        [[{"text": f"✅ بله، همه رو false کن ({bf_count})", "callback_data": "admin:bulkfalse:reason"}],
                                         [{"text": "↩️ انصراف", "callback_data": "admin_sig:back"}]])

                            # مرحله ۱: درخواست متن دلیل
                            elif bf_step == "reason":
                                answer_callback(token_cbq, cbq_id)
                                bf_rows2 = _sb_load_active_assignments()
                                _pending_bulk_false[cbq_cid] = {"bot_msg_id": cbq_msg_id, "count": len(bf_rows2)}
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    f"📝 <b>دلیل فالس کردن {len(bf_rows2)} آلارم رو بنویس</b>\n\n"
                                    f"(مثلاً: آخر هفته / تعطیلی / تست)\n\n"
                                    f"یا دکمه‌ی زیر رو بزن تا بدون دلیل ثبت بشه.",
                                    [[{"text": "⏭ بدون دلیل، فقط false کن", "callback_data": "admin:bulkfalse:go:__no_reason__"}],
                                     [{"text": "❌ انصراف", "callback_data": "admin_sig:back"}]])

                            # مرحله ۲: اجرای نهایی (یا از متن تایپ‌شده، یا از دکمه‌ی بدون دلیل)
                            elif bf_step == "go":
                                bf_reason = bf_parts[3] if len(bf_parts) > 3 else ""
                                if bf_reason == "__no_reason__":
                                    bf_reason = ""
                                answer_callback(token_cbq, cbq_id, "⏳ در حال فالس کردن همه...")
                                _pending_bulk_false.pop(cbq_cid, None)

                                def _do_bulk_false(reason=bf_reason, tok=token_cbq, c=cbq_cid, mid=cbq_msg_id):
                                    rows_bf = _sb_load_active_assignments()
                                    admin_name_bf = _get_user_custom_name(c) or "ادمین"
                                    done_count = 0
                                    for row_bf in rows_bf:
                                        aid_bf = row_bf.get("id")
                                        tag_bf = row_bf.get("alarm_tag", "")
                                        # همون تابع false تکی رو صدا بزن — یکسان با /False دستی
                                        _sb_false_assignment(aid_bf, admin_name_bf, reason)
                                        done_count += 1
                                        # ریپلای به همه‌ی گیرنده‌های این آلارم
                                        msg_map_bf = _fired_msg_ids.get(aid_bf, {})
                                        reason_line_bf = f"\n📝 علت: {reason}" if reason else ""
                                        reply_bf = (f"❌ <b>فالس (دسته‌جمعی توسط ادمین)</b>\n\n"
                                                    f"{tag_bf}{reason_line_bf}")
                                        for tc_bf, tm_bf in msg_map_bf.items():
                                            if tc_bf in ("__tag__", "__text__"): continue
                                            try:
                                                requests.post(
                                                    f"https://api.telegram.org/bot{tok}/sendMessage",
                                                    json={"chat_id": tc_bf, "text": reply_bf,
                                                          "parse_mode": "HTML", "reply_to_message_id": tm_bf},
                                                    timeout=8, headers=H)
                                            except: pass
                                    # بعد از فالس کردن همه، شمارش عادلانه رو یک‌بار نهایی بازسازی کن
                                    _rebuild_active_assign_count(_sb_load_active_assignments())
                                    edit_tg_keyboard(tok, c, mid,
                                        f"✅ <b>{done_count} آلارم false شدن</b>" + (f"\n📝 علت: {reason}" if reason else ""),
                                        [[{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}]])
                                threading.Thread(target=_do_bulk_false, daemon=True).start()

                    elif cbq_data.startswith("clean_chat:"):
                        answer_callback(token_cbq, cbq_id, "🧹 در حال پاک‌سازی...")
                        target_cid = cbq_data.split(":", 1)[1]
                        if cbq_msg_id:
                            _track_msg(cbq_cid, cbq_msg_id)
                        def _do_clean(tok=token_cbq, c=cbq_cid, tc=target_cid):
                            cnt = delete_chat_history(tok, tc)
                            send_tg_keyboard(tok, c,
                                f"✅ <b>{cnt} پیام پاک شد.</b>",
                                [[{"text": "✕ بستن", "callback_data": "close_myalerts"}]])
                        threading.Thread(target=_do_clean, daemon=True).start()

                    elif cbq_data.startswith("edit_name:"):
                        # شروع flow ویرایش اسم از طریق استاتوس
                        en_cid = cbq_data.split(":", 1)[1]
                        answer_callback(token_cbq, cbq_id)
                        d_en = load_alerts()
                        cur_name_en = next(
                            (u.get("custom_name","") for u in d_en.get("users",[]) if str(u.get("chat_id","")) == en_cid),
                            "")
                        cur_info_en = f"\nاسم فعلی: <b>{cur_name_en}</b>" if cur_name_en else ""
                        edit_tg_keyboard(token_cbq, en_cid, cbq_msg_id,
                            f"✏️ <b>ویرایش اسم</b>{cur_info_en}\n\nاسم جدیدت رو بنویس:",
                            [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{en_cid}"}]])
                        _pending_alarm[en_cid] = {"step": "edit_name_input", "data": {}, "bot_msg_id": cbq_msg_id}

                    elif cbq_data == "close_myalerts":
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=10, headers=H)
                        except: pass

                    elif cbq_data.startswith("flow_cancel:"):
                        # لغو هر flow در حال اجرا (آلارم عادی یا SOS)
                        f_cid = cbq_data.split(":", 1)[1]
                        _pending_alarm.pop(f_cid, None)
                        _pending_signal.pop(f_cid, None)
                        answer_callback(token_cbq, cbq_id, "لغو شد")
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            "❌ <b>عملیات لغو شد.</b>", [])

                    # ── signal callbacks ──────────────────────────────
                    elif cbq_data.startswith("sig_cancel:"):
                        sc_cid = cbq_data.split(":",1)[1]
                        _pending_signal.pop(sc_cid, None)
                        answer_callback(token_cbq, cbq_id, "لغو شد")
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            "❌ <b>ساخت سیگنال لغو شد.</b>", [])

                    elif cbq_data.startswith("sig_sym:"):
                        # انتخاب نماد از shortcut
                        parts_ss = cbq_data.split(":", 2)
                        ss_cid = parts_ss[1]; ss_sym = parts_ss[2]
                        ps = _pending_signal.get(ss_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["data"]["symbol"] = ss_sym
                        ps["step"] = "sig_direction"
                        dir_kb = [[{"text": lbl, "callback_data": f"sig_dir:{ss_cid}:{val}"} for lbl,val in SIGNAL_DIRECTIONS[:2]],
                                  [{"text": lbl, "callback_data": f"sig_dir:{ss_cid}:{val}"} for lbl,val in SIGNAL_DIRECTIONS[2:]],
                                  [{"text": "❌ انصراف", "callback_data": f"sig_cancel:{ss_cid}"}]]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"📡 <b>{ss_sym}</b>\n\nنوع سفارش:", dir_kb)

                    elif cbq_data.startswith("sig_dir:"):
                        # انتخاب جهت
                        parts_sd2 = cbq_data.split(":", 2)
                        sd2_cid = parts_sd2[1]; sd2_dir = parts_sd2[2]
                        ps = _pending_signal.get(sd2_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        dir_lbl_map = {"buy_limit":"✅ Buy Limit","buy_stop":"✅ Buy Stop",
                                       "sell_limit":"🔴 Sell Limit","sell_stop":"🔴 Sell Stop"}
                        ps["data"]["direction"] = sd2_dir
                        ps["data"]["dir_lbl"] = dir_lbl_map.get(sd2_dir, sd2_dir)
                        ps["step"] = "sig_sl_mode"
                        sym_sd2 = ps["data"].get("symbol","")
                        # انتخاب نوع SL
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"📡 <b>{sym_sd2}</b>  {ps['data']['dir_lbl']}\n\nاستاپ رو چطور میدی؟",
                            [[{"text": "🔢 عدد مستقیم", "callback_data": f"sig_slmode:{sd2_cid}:price"},
                              {"text": "📏 پیپ", "callback_data": f"sig_slmode:{sd2_cid}:pip"}],
                             [{"text": "❌ انصراف", "callback_data": f"sig_cancel:{sd2_cid}"}]])

                    elif cbq_data.startswith("sig_slmode:"):
                        parts_sm = cbq_data.split(":", 2)
                        sm_cid = parts_sm[1]; sm_mode = parts_sm[2]
                        ps = _pending_signal.get(sm_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["data"]["sl_mode"] = sm_mode
                        ps["step"] = "sig_entry_sl"
                        sym_sm = ps["data"].get("symbol","")
                        dir_lbl_sm = ps["data"].get("dir_lbl","")
                        mode_hint = "قیمت SL" if sm_mode == "price" else "پیپ SL"
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"📡 <b>{sym_sm}</b>  {dir_lbl_sm}\n\n"
                            f"بنویس:  <code>Entry  {mode_hint}</code>\n"
                            f"مثال:   <code>73370  {'72550' if sm_mode == 'price' else '82'}</code>",
                            [[{"text": "❌ انصراف", "callback_data": f"sig_cancel:{sm_cid}"}]])

                    elif cbq_data.startswith("sig_tf:"):
                        # انتخاب تایم‌فریم
                        stf_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(stf_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        cur_tf = ps["data"].get("tf", SIGNAL_DEFAULT_TF)
                        tf_kb = [[{"text": f"{'✅ ' if tf==cur_tf else ''}{tf}",
                                   "callback_data": f"sig_settf:{stf_cid}:{tf}"}
                                  for tf in SIGNAL_TF_OPTIONS[:3]],
                                 [{"text": f"{'✅ ' if tf==cur_tf else ''}{tf}",
                                   "callback_data": f"sig_settf:{stf_cid}:{tf}"}
                                  for tf in SIGNAL_TF_OPTIONS[3:]],
                                 [{"text": "↩️ بازگشت", "callback_data": f"sig_back:{stf_cid}"}]]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"⏱ تایم‌فریم رو انتخاب کن (فعلی: <b>{cur_tf}</b>):", tf_kb)

                    elif cbq_data.startswith("sig_settf:"):
                        parts_stf = cbq_data.split(":", 2)
                        stf_cid2 = parts_stf[1]; stf_val = parts_stf[2]
                        ps = _pending_signal.get(stf_cid2)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id, f"✅ {stf_val}")
                        ps["data"]["tf"] = stf_val
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token_cbq, cbq_cid, cbq_msg_id, ps["data"])

                    elif cbq_data.startswith("sig_tp:"):
                        stp_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(stp_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["data"]["_editing_tp"] = "tp2"
                        ps["step"] = "sig_tp_edit"
                        cur_tp2 = ps["data"].get("tp2")
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"🎯 <b>TP2</b> رو بنویس (اختیاری):\n"
                            + (f"فعلی: <code>{_fmt_signal_price(cur_tp2, ps['data'].get('symbol',''))}</code>\n" if cur_tp2 else "")
                            + "برای حذف بنویس: <code>0</code>",
                            [[{"text": "⏭ رد کن", "callback_data": f"sig_skip_tp:{stp_cid}:tp2"},
                              {"text": "↩️ بازگشت", "callback_data": f"sig_back:{stp_cid}"}]])

                    elif cbq_data.startswith("sig_skip_tp:"):
                        parts_skp = cbq_data.split(":", 2)
                        skp_cid = parts_skp[1]; skp_which = parts_skp[2]
                        ps = _pending_signal.get(skp_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        # اگه tp2 رد شد، tp3 رو هم رد کن
                        ps["data"]["tp2"] = None
                        ps["data"]["tp3"] = None
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token_cbq, cbq_cid, cbq_msg_id, ps["data"])

                    elif cbq_data.startswith("sig_note:"):
                        sn_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(sn_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["step"] = "sig_note"
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            "📝 یادداشت بنویس (اختیاری):",
                            [[{"text": "⏭ رد کن", "callback_data": f"sig_back:{sn_cid}"},
                              {"text": "❌ انصراف", "callback_data": f"sig_cancel:{sn_cid}"}]])

                    elif cbq_data.startswith("sig_recalc:"):
                        # محاسبه مجدد با RR متفاوت
                        src_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(src_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        rr_kb = [[{"text": f"{'✅ ' if ps['data'].get('rr')==v else ''}{v}R",
                                   "callback_data": f"sig_setrr:{src_cid}:{v}"}
                                  for v in [1.0, 1.5, 2.0]],
                                 [{"text": f"{'✅ ' if ps['data'].get('rr')==v else ''}{v}R",
                                   "callback_data": f"sig_setrr:{src_cid}:{v}"}
                                  for v in [2.5, 3.0]],
                                 [{"text": "↩️ بازگشت", "callback_data": f"sig_back:{src_cid}"}]]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"🔄 ریوارد رو انتخاب کن (فعلی: <b>{ps['data'].get('rr', SIGNAL_DEFAULT_RR)}R</b>):", rr_kb)

                    elif cbq_data.startswith("sig_setrr:"):
                        parts_rr = cbq_data.split(":", 2)
                        rr_cid = parts_rr[1]; rr_val = float(parts_rr[2])
                        ps = _pending_signal.get(rr_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id, f"✅ ریوارد {rr_val}R")
                        d_rr = ps["data"]
                        # محاسبه مجدد
                        _, tp1_new, _ = _calc_signal(d_rr["symbol"], d_rr["direction"],
                                                      d_rr["entry"], d_rr["sl"], rr_val)
                        d_rr["rr"] = rr_val
                        d_rr["tp1"] = tp1_new
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token_cbq, cbq_cid, cbq_msg_id, d_rr)

                    elif cbq_data.startswith("sig_back:"):
                        sb_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(sb_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token_cbq, cbq_cid, cbq_msg_id, ps["data"])

                    elif cbq_data.startswith("signals_view:"):
                        # نمایش لیست سیگنال‌ها
                        parts_sv = cbq_data.split(":", 2)
                        sv_cid  = parts_sv[1]
                        sv_mode = parts_sv[2]  # mine | all
                        answer_callback(token_cbq, cbq_id, "⏳ در حال بارگذاری...")
                        sigs = _sb_load_signals(limit=20)
                        if sv_mode == "mine":
                            my_name = _get_user_custom_name(sv_cid) or sv_cid
                            sigs = [s for s in sigs if s.get("sent_by","") == my_name]
                            title = f"📡 <b>سیگنال‌های من</b>"
                        else:
                            title = f"📊 <b>همه سیگنال‌ها</b>"
                        if not sigs:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"{title}\n\n📭 سیگنالی یافت نشد.",
                                [[{"text": "↩️ بازگشت", "callback_data": f"signals_close:{sv_cid}"}]])
                            return
                        lines = [title, ""]
                        for s in sigs:
                            sym_sv   = s.get("symbol","")
                            sid_sv   = s.get("id","")
                            dir_sv   = s.get("direction","")
                            entry_sv = s.get("entry")
                            sl_sv    = s.get("sl")
                            tp1_sv   = s.get("tp1")
                            tf_sv    = s.get("tf","")
                            sent_by_sv = s.get("sent_by","")
                            sent_at_sv = s.get("sent_at","")[:16] if s.get("sent_at") else ""
                            ch_mid_sv  = s.get("channel_msg_id")
                            # آیکون ارسال به کانال یا فقط DB
                            origin = "📤" if ch_mid_sv else "💾"
                            dir_short = {"buy_limit":"BL↗","buy_stop":"BS↗",
                                         "sell_limit":"SL↘","sell_stop":"SS↘"}.get(dir_sv, dir_sv)
                            lines.append(
                                f"{origin} <b>{sid_sv}</b>  #{sym_sv}  <i>{dir_short}</i>\n"
                                f"   ➡️ <code>{_fmt_signal_price(entry_sv,sym_sv)}</code>  "
                                f"🛑 <code>{_fmt_signal_price(sl_sv,sym_sv)}</code>  "
                                f"🎯 <code>{_fmt_signal_price(tp1_sv,sym_sv)}</code>\n"
                                f"   ⏱ {tf_sv}  👤 {sent_by_sv}  🕐 {sent_at_sv}"
                            )
                            lines.append("──────────────────")
                        legend = "\n📤 = ارسال به گروه   💾 = فقط ثبت دیتا"
                        kb_sv = [
                            [{"text": "📡 سیگنال‌های من", "callback_data": f"signals_view:{sv_cid}:mine"},
                             {"text": "📊 همه سیگنال‌ها", "callback_data": f"signals_view:{sv_cid}:all"}],
                            [{"text": "✕ بستن", "callback_data": f"signals_close:{sv_cid}"}]
                        ]
                        full_text = "\n".join(lines) + legend
                        # تلگرام max 4096 کاراکتر
                        if len(full_text) > 4000:
                            full_text = full_text[:3980] + "\n\n<i>... (برای دیدن بیشتر فیلتر کن)</i>"
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, full_text, kb_sv)

                    elif cbq_data.startswith("signals_close:"):
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=8, headers=H)
                        except: pass

                    elif cbq_data.startswith("trigger_list:"):
                        answer_callback(token_cbq, cbq_id, "⏳ در حال بارگذاری...")
                        tl_cid = cbq_data.split(":", 1)[1]
                        rows_tl_all = _sb_load_active_assignments()
                        # فقط آلارم‌های تیمی — فقط is_private رو برای همون IDهای لازم چک کن (نه کل جدول)
                        tl_ids = sorted({str(r.get("id","")) for r in rows_tl_all if r.get("id")})
                        private_ids = set()
                        if SUPABASE_KEY and tl_ids:
                            try:
                                r_tl = requests.get(
                                    f"{SUPABASE_URL}/rest/v1/alerts?id=in.({','.join(tl_ids)})&select=id,is_private",
                                    headers=_sb_h(), timeout=10)
                                if r_tl.status_code == 200:
                                    private_ids = {str(x.get("id","")) for x in r_tl.json() if x.get("is_private")}
                            except Exception as e:
                                print(f"[trigger_list] targeted fetch error: {e}")
                        rows_tl = [r for r in rows_tl_all if str(r.get("id","")) not in private_ids]
                        my_name_tl = _get_user_custom_name(tl_cid) or ""
                        if not rows_tl:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                "🎯 <b>لیست تریگر</b>\n\n📭 هیچ آلارم فعالی در لیست تریگر نیست.",
                                [[{"text": "↩️ بازگشت", "callback_data": f"trigger_list_close:{tl_cid}"}]])
                        else:
                            # گروه‌بندی بر اساس مسئول
                            by_member = {}
                            unassigned = []
                            for row_tl in rows_tl:
                                m = row_tl.get("assigned_to", "")
                                tag = row_tl.get("alarm_tag", "—")
                                shift_tl = row_tl.get("shift", "")
                                if m:
                                    by_member.setdefault(m, []).append((tag, shift_tl))
                                else:
                                    unassigned.append((tag, shift_tl))
                            lines_tl = ["🎯 <b>لیست تریگر فعال</b>\n"]
                            for member, items in sorted(by_member.items()):
                                marker = " 👈" if member == my_name_tl else ""
                                lines_tl.append(f"👤 <b>{member}</b>{marker}")
                                for tag_tl, sh_tl in items:
                                    lines_tl.append(f"   • {tag_tl}")
                                lines_tl.append("")
                            if unassigned:
                                lines_tl.append("⏳ <b>بدون مسئول</b>")
                                for tag_tl, sh_tl in unassigned:
                                    lines_tl.append(f"   • {tag_tl}")
                            full_tl = "\n".join(lines_tl)
                            if len(full_tl) > 4000:
                                full_tl = full_tl[:3980] + "\n\n<i>...</i>"
                            kb_tl = [[{"text": "🔄 بروزرسانی", "callback_data": f"trigger_list:{tl_cid}"},
                                      {"text": "✕ بستن",       "callback_data": f"trigger_list_close:{tl_cid}"}]]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, full_tl, kb_tl)

                    elif cbq_data.startswith("trigger_list_close:"):
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=8, headers=H)
                        except: pass

                    elif cbq_data.startswith("weekly_menu:"):
                        answer_callback(token_cbq, cbq_id, "")
                        wm_cid = cbq_data.split(":", 1)[1]
                        kb_menu = [
                            [{"text": "📅 این هفته", "callback_data": f"weekly_report:{wm_cid}:this:0"},
                             {"text": "📅 هفته قبل", "callback_data": f"weekly_report:{wm_cid}:last:0"}],
                        ]
                        if APP_BASE_URL:
                            kb_menu.append([{"text": "🌐 نسخه وب", "url": f"{APP_BASE_URL}/report/weekly"}])
                        kb_menu.append([{"text": "✕ بستن", "callback_data": f"weekly_report_close:{wm_cid}"}])
                        send_tg_keyboard(token_cbq, cbq_cid, "📊 <b>آنالیز هفتگی تیم</b>\n\nیک گزینه رو انتخاب کن:", kb_menu)

                    elif cbq_data.startswith("weekly_report:"):
                        answer_callback(token_cbq, cbq_id, "⏳ در حال بارگذاری...")
                        wr_parts = cbq_data.split(":")
                        cbq_cid_wr = wr_parts[1]
                        wr_which   = wr_parts[2] if len(wr_parts) > 2 else "this"
                        wr_page    = int(wr_parts[3]) if len(wr_parts) > 3 else 0
                        PER_PAGE   = 5
                        now_dt_wr = datetime.now(TEHRAN)
                        days_since_sat = (now_dt_wr.weekday() - 5) % 7
                        this_week_start = (now_dt_wr - timedelta(days=days_since_sat)).replace(
                            hour=0, minute=0, second=0, microsecond=0)
                        if wr_which == "last":
                            week_start = this_week_start - timedelta(days=7)
                            week_end   = this_week_start
                        else:
                            week_start = this_week_start
                            week_end   = None
                        week_start_str = week_start.strftime("%Y-%m-%dT%H:%M:%S")
                        week_label = f"{week_start.strftime('%d/%m')} — {(week_end - timedelta(days=1)).strftime('%d/%m') if week_end else 'الان'}"
                        rows_wr = []
                        if SUPABASE_KEY:
                            try:
                                url_wr = (f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                                          f"?fired_at=gte.{week_start_str}&select=*&order=fired_at.asc")
                                if week_end:
                                    url_wr += f"&fired_at=lt.{week_end.strftime('%Y-%m-%dT%H:%M:%S')}"
                                r_wr = requests.get(url_wr, headers=_sb_h(), timeout=10)
                                if r_wr.status_code == 200:
                                    rows_wr = r_wr.json()
                            except Exception as e:
                                print(f"[weekly] load exc: {e}")
                        # لود فقط alertهایی که تو این بازه‌ی هفته هستن (نه کل جدول)
                        wr_ids = sorted({str(r.get("id","")) for r in rows_wr if r.get("id")})
                        alerts_by_id_wr = {}
                        if SUPABASE_KEY and wr_ids:
                            try:
                                r_wr2 = requests.get(
                                    f"{SUPABASE_URL}/rest/v1/alerts?id=in.({','.join(wr_ids)})&select=*",
                                    headers=_sb_h(), timeout=10)
                                if r_wr2.status_code == 200:
                                    for row_wr in r_wr2.json():
                                        alerts_by_id_wr[str(row_wr.get("id",""))] = row_wr
                            except Exception as e:
                                print(f"[weekly] targeted alerts fetch error: {e}")
                        all_alerts_wr = list(alerts_by_id_wr.values())
                        private_ids_wr = {str(a["id"]) for a in all_alerts_wr if a.get("is_private")}
                        rows_wr = [r for r in rows_wr if str(r.get("id","")) not in private_ids_wr]
                        # صفحه‌بندی
                        total = len(rows_wr)
                        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
                        wr_page = max(0, min(wr_page, total_pages - 1))
                        page_rows = rows_wr[wr_page * PER_PAGE:(wr_page + 1) * PER_PAGE]

                        # فقط صفحه‌بندی و بستن
                        kb_wr_nav = []
                        if total_pages > 1:
                            page_row = []
                            if wr_page > 0:
                                page_row.append({"text": "‹ قبلی", "callback_data": f"weekly_report:{cbq_cid_wr}:{wr_which}:{wr_page-1}"})
                            page_row.append({"text": f"{wr_page+1}/{total_pages}", "callback_data": "noop"})
                            if wr_page < total_pages - 1:
                                page_row.append({"text": "بعدی ›", "callback_data": f"weekly_report:{cbq_cid_wr}:{wr_which}:{wr_page+1}"})
                            kb_wr_nav.append(page_row)
                        kb_wr_nav.append([{"text": "🔍 جستجو", "callback_data": f"weekly_search:{cbq_cid_wr}:{wr_which}"},
                                          {"text": "✕ بستن", "callback_data": f"weekly_report_close:{cbq_cid_wr}"}])

                        if not rows_wr:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"📋 <b>گزارش هفتگی تیم</b>\n<i>{week_label}</i>\n\n📭 هیچ آلارم تیمی ثبت نشده.",
                                kb_wr_nav)
                        else:
                            lines_wr = [
                                f"📋 <b>گزارش هفتگی تیم</b>",
                                f"<i>{week_label} — {total} آلارم | صفحه {wr_page+1}/{total_pages}</i>",
                                ""
                            ]
                            for row_wr in page_rows:
                                aid_wr       = str(row_wr.get("id", ""))
                                tag_wr       = row_wr.get("alarm_tag", "—")
                                assignee_wr  = row_wr.get("assigned_to", "") or "—"
                                fired_wr     = row_wr.get("fired_at", "")[:16]
                                false_by_wr  = row_wr.get("false_by", "") or ""
                                false_at_wr  = row_wr.get("false_at", "")[:16] if row_wr.get("false_at") else ""
                                false_rsn_wr = row_wr.get("false_reason", "") or ""
                                is_active_wr = row_wr.get("is_active", True)
                                alert_wr   = alerts_by_id_wr.get(aid_wr, {})
                                sym_wr     = alert_wr.get("symbol", "") or row_wr.get("symbol", "") or ""
                                tgt_raw    = alert_wr.get("target_price", 0) or row_wr.get("target_price", 0) or 0
                                target_wr  = fmt_price(float(tgt_raw), sym_wr) if tgt_raw else "—"
                                creator_wr = alert_wr.get("created_by", "") or row_wr.get("created_by", "") or "—"
                                created_wr = str(alert_wr.get("created_at", ""))[:16]
                                cond_wr    = alert_wr.get("condition", "") or row_wr.get("condition", "")
                                dir_wr     = "📈 ناحیه سل" if cond_wr == "above" else ("📉 ناحیه بای" if cond_wr == "below" else "")
                                lines_wr.append(f"🔖 <b>{tag_wr}</b>  |  #{sym_wr}" + (f"  |  {dir_wr}" if dir_wr else ""))
                                if created_wr:
                                    lines_wr.append(f"📅 ثبت: {created_wr}")
                                lines_wr.append(f"🎯 هدف: <code>{target_wr}</code>")
                                lines_wr.append(f"👤 سازنده: {creator_wr}")
                                lines_wr.append(f"⏰ فایر شد: {fired_wr}")
                                lines_wr.append(f"🙋 مسئول: {assignee_wr}")
                                if is_active_wr:
                                    lines_wr.append(f"✅ وضعیت: فعال")
                                else:
                                    hist_wr = row_wr.get("false_history") or []
                                    if isinstance(hist_wr, str):
                                        try: hist_wr = json.loads(hist_wr)
                                        except: hist_wr = []
                                    if hist_wr:
                                        lines_wr.append("❌ <b>تاریخچه False:</b>")
                                        for i_h, h_wr in enumerate(hist_wr, 1):
                                            h_at  = str(h_wr.get("at",""))[:16]
                                            h_by  = h_wr.get("by","")
                                            h_rsn = h_wr.get("reason","")
                                            h_line = f"  {i_h}. {h_by}  |  {h_at}"
                                            if h_rsn:
                                                h_line += f"\n     📝 {h_rsn}"
                                            lines_wr.append(h_line)
                                    else:
                                        lines_wr.append(f"❌ وضعیت: False — {false_by_wr}  |  {false_at_wr}")
                                        if false_rsn_wr:
                                            lines_wr.append(f"📝 علت: {false_rsn_wr}")
                                lines_wr.append("──────────────")
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_wr), kb_wr_nav)

                    elif cbq_data.startswith("weekly_report_close:"):
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=8, headers=H)
                        except: pass

                    elif cbq_data.startswith("weekly_search:"):
                        answer_callback(token_cbq, cbq_id, "")
                        ws_parts = cbq_data.split(":")
                        ws_cid   = ws_parts[1]
                        ws_which = ws_parts[2] if len(ws_parts) > 2 else "this"
                        _pending_weekly_search[ws_cid] = {"which": ws_which, "msg_id": cbq_msg_id}
                        send_tg(token_cbq, ws_cid, "🔍 اسم مسئول، نماد یا تگ رو بفرست (مثلاً: مسعود):")

                    elif cbq_data.startswith("resend_active:"):
                        answer_callback(token_cbq, cbq_id, "⏳ در حال ارسال...")
                        ra_cid = cbq_data.split(":", 1)[1]
                        rows_ra = _sb_load_active_assignments()
                        # فقط تیمی
                        all_alerts_ra = load_alerts().get("alarms", [])
                        private_ids_ra = {str(a["id"]) for a in all_alerts_ra if a.get("is_private")}
                        rows_ra = [r for r in rows_ra if str(r.get("id","")) not in private_ids_ra]
                        if not rows_ra:
                            send_tg(token_cbq, ra_cid, "📭 هیچ آلارم فعالی در لیست تریگر نیست.")
                        else:
                            send_tg(token_cbq, ra_cid,
                                f"🔔 <b>{len(rows_ra)} آلارم فعال</b> — ریپلای زیر:")
                            for row_ra in rows_ra:
                                aid_ra = str(row_ra.get("id", ""))
                                tag_ra = row_ra.get("alarm_tag", "—")
                                assignee_ra = row_ra.get("assigned_to", "") or "⏳ منتظر تقسیم"
                                # پیدا کردن message_id اصلی برای این کاربر
                                cid_map_ra = _fired_msg_ids.get(aid_ra, {})
                                orig_mid = cid_map_ra.get(ra_cid)
                                orig_text = cid_map_ra.get("__text__", "")
                                if orig_mid:
                                    # ریپلای روی همون پیام اصلی
                                    try:
                                        requests.post(
                                            f"https://api.telegram.org/bot{token_cbq}/sendMessage",
                                            json={"chat_id": ra_cid,
                                                  "text": f"🔔 <b>{tag_ra}</b>\n👤 مسئول: {assignee_ra}",
                                                  "parse_mode": "HTML",
                                                  "reply_to_message_id": orig_mid},
                                            timeout=8, headers=H)
                                    except: pass
                                else:
                                    # پیام اصلی پیدا نشد — متن خلاصه بفرست
                                    send_tg(token_cbq, ra_cid,
                                        f"🔔 <b>{tag_ra}</b>\n👤 مسئول: {assignee_ra}")

                    elif cbq_data.startswith("today_alarms:"):
                        answer_callback(token_cbq, cbq_id, "⏳ در حال بارگذاری...")
                        ta_parts = cbq_data.split(":")
                        ta_cid   = ta_parts[1]
                        ta_mode  = ta_parts[2] if len(ta_parts) > 2 else "active"
                        # ابتدای هفته — شنبه تهران
                        now_dt_ta = datetime.now(TEHRAN)
                        days_since_sat = (now_dt_ta.weekday() - 5) % 7
                        week_start_ta = (now_dt_ta - timedelta(days=days_since_sat)).replace(
                            hour=0, minute=0, second=0, microsecond=0)
                        week_start_str_ta = week_start_ta.strftime("%Y-%m-%dT%H:%M:%S")
                        rows_ta = []
                        if SUPABASE_KEY:
                            try:
                                if ta_mode == "active":
                                    url_ta = (
                                        f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                                        f"?fired_at=gte.{week_start_str_ta}&is_active=eq.true"
                                        f"&select=*&order=fired_at.asc"
                                    )
                                else:
                                    url_ta = (
                                        f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                                        f"?fired_at=gte.{week_start_str_ta}"
                                        f"&select=*&order=fired_at.asc"
                                    )
                                r_ta = requests.get(url_ta, headers=_sb_h(), timeout=10)
                                if r_ta.status_code == 200:
                                    rows_ta = r_ta.json()
                            except Exception as e:
                                print(f"[weekly_list] load exc: {e}")
                        # فقط تیمی
                        all_alerts_ta = load_alerts().get("alarms", [])
                        private_ids_ta = {str(a["id"]) for a in all_alerts_ta if a.get("is_private")}
                        rows_ta = [r for r in rows_ta if str(r.get("id","")) not in private_ids_ta]
                        mode_label = "فعال" if ta_mode == "active" else "همه"
                        week_label = week_start_ta.strftime("%d/%m")
                        if not rows_ta:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"📋 <b>هفتگی ({mode_label}) از {week_label}</b>\n\n📭 آلارمی پیدا نشد.",
                                [[{"text": "✅ فعال", "callback_data": f"today_alarms:{ta_cid}:active"},
                                  {"text": "📊 همه",  "callback_data": f"today_alarms:{ta_cid}:all"}],
                                 [{"text": "✕ بستن", "callback_data": f"today_alarms_close:{ta_cid}"}]])
                        else:
                            lines_ta = [f"📋 <b>هفتگی ({mode_label}) از {week_label} — {len(rows_ta)} آلارم</b>", ""]
                            alerts_by_id_ta = {str(a["id"]): a for a in all_alerts_ta}
                            for row_ta in rows_ta:
                                aid_ta      = str(row_ta.get("id",""))
                                tag_ta      = row_ta.get("alarm_tag", "—")
                                assignee_ta = row_ta.get("assigned_to", "") or "⏳ منتظر تقسیم"
                                fired_ta    = row_ta.get("fired_at", "")[:16]
                                is_act_ta   = row_ta.get("is_active", True)
                                alert_ta    = alerts_by_id_ta.get(aid_ta, {})
                                sym_ta      = alert_ta.get("symbol", "")
                                target_ta   = alert_ta.get("target_price", "") or alert_ta.get("price", "")
                                status_icon = "✅" if is_act_ta else "❌"
                                lines_ta.append(f"{status_icon} <b>{tag_ta}</b>  {sym_ta}")
                                if target_ta:
                                    lines_ta.append(f"   🎯 هدف: <code>{target_ta}</code>")
                                lines_ta.append(f"   ⏰ {fired_ta}  |  👤 {assignee_ta}")
                                lines_ta.append("")
                            full_ta = "\n".join(lines_ta)
                            if len(full_ta) > 4000:
                                full_ta = full_ta[:3980] + "\n<i>...</i>"
                            kb_ta = [
                                [{"text": "✅ فعال", "callback_data": f"today_alarms:{ta_cid}:active"},
                                 {"text": "📊 همه",  "callback_data": f"today_alarms:{ta_cid}:all"}],
                                [{"text": "🔄 بروزرسانی", "callback_data": f"today_alarms:{ta_cid}:{ta_mode}"},
                                 {"text": "✕ بستن",        "callback_data": f"today_alarms_close:{ta_cid}"}]
                            ]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, full_ta, kb_ta)

                    elif cbq_data.startswith("today_alarms_close:"):
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=8, headers=H)
                        except: pass

                    elif cbq_data.startswith("sig_send:"):
                        parts_snd = cbq_data.split(":", 2)
                        send_cid  = parts_snd[1]
                        send_mode = parts_snd[2] if len(parts_snd) > 2 else "channel"
                        ps = _pending_signal.get(send_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id, "⏳ در حال ثبت...")
                        d_send = ps["data"]
                        seq = _sb_next_signal_seq()
                        sig_id = f"S{seq:05d}"
                        d_send["id"] = sig_id
                        d_send["seq"] = seq
                        d_send["sent_by"] = _get_user_custom_name(send_cid) or send_cid
                        d_send["sent_at"] = now_teh()
                        d_send["status"]  = "active"
                        d_send["channel_msg_id"] = None
                        # ارسال به کانال فقط در حالت channel
                        channel_mid = None
                        if send_mode == "channel" and SIGNAL_CHANNEL:
                            try:
                                r_ch = requests.post(
                                    f"https://api.telegram.org/bot{token_cbq}/sendMessage",
                                    json={"chat_id": SIGNAL_CHANNEL, "text": _build_signal_text(d_send),
                                          "parse_mode": "HTML"},
                                    timeout=10, headers=H)
                                if r_ch.status_code == 200:
                                    channel_mid = r_ch.json().get("result",{}).get("message_id")
                                    d_send["channel_msg_id"] = channel_mid
                            except Exception as e:
                                print(f"[signal] channel send error: {e}")
                        # همیشه در Supabase ذخیره میشه
                        threading.Thread(target=_sb_save_signal, args=(d_send,), daemon=True).start()
                        del _pending_signal[send_cid]
                        sig_text_conf = _build_signal_text(d_send)
                        if send_mode == "channel":
                            status_line = f"📤 ارسال شد به گروه" if channel_mid else "⚠️ کانال تنظیم نشده — فقط ذخیره شد"
                        else:
                            status_line = "💾 فقط در دیتابیس ثبت شد"
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"✅ <b>سیگنال {sig_id} ثبت شد</b>  {status_line}\n\n{sig_text_conf}", [])

                    elif cbq_data.startswith("alarm_dir:"):
                        # alarm_dir:cid:buy|sell
                        parts_ad = cbq_data.split(":", 2)
                        ad_cid = parts_ad[1] if len(parts_ad) > 1 else cbq_cid
                        ad_raw = parts_ad[2] if len(parts_ad) > 2 else "sell"
                        pend_ad = _pending_alarm.get(ad_cid)
                        if not pend_ad or pend_ad.get("step") != "alarm_dir":
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد — دوباره شروع کن")
                            return
                        answer_callback(token_cbq, cbq_id)
                        dw_ad = pend_ad["data"]
                        if ad_raw == "buy":
                            dw_ad["condition"] = "below"
                            dir_lbl_ad = "📈 BUY"
                        else:
                            dw_ad["condition"] = "above"
                            dir_lbl_ad = "📉 SELL"
                        pend_ad["step"] = "alarm_price"
                        ptype_lbl_ad = "🔒 شخصی" if dw_ad.get("ptype") == "private" else "🌐 تیمی"
                        edit_tg_keyboard(token_cbq, ad_cid, cbq_msg_id,
                            f"🔔 <b>{dw_ad['symbol']}</b>  {dir_lbl_ad}  ({ptype_lbl_ad})\n\nقیمت هدف رو بنویس:\nمثال: <code>1.08500</code>  یا  <code>2350</code>",
                            [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{ad_cid}"}]])

                    elif cbq_data.startswith("alarm_submit:"):
                        # «ثبت بدون یادداشت» — بعدش هنوز باید مدت اعتبار انتخاب بشه
                        as_cid = cbq_data.split(":", 1)[1]
                        pend_as = _pending_alarm.get(as_cid)
                        if not pend_as or pend_as.get("step") != "alarm_comment":
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد — دوباره شروع کن")
                            return
                        answer_callback(token_cbq, cbq_id, "")
                        pend_as["data"]["comment"] = ""
                        pend_as["step"] = "alarm_expiry"
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"🔔 <b>{pend_as['data'].get('symbol','')}</b>\n\n⏳ این آلارم چند روز معتبر باشه؟ (اگه تا اون موقع فایر نشه، خودکار حذف می‌شه)",
                            [
                                [{"text": "۱ روز", "callback_data": f"alarm_expiry:{as_cid}:1"},
                                 {"text": "۳ روز", "callback_data": f"alarm_expiry:{as_cid}:3"},
                                 {"text": "۷ روز", "callback_data": f"alarm_expiry:{as_cid}:7"}],
                                [{"text": "♾ بدون انقضا", "callback_data": f"alarm_expiry:{as_cid}:0"}],
                                [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{as_cid}"}]
                            ])

                    elif cbq_data.startswith("alarm_expiry:"):
                        # مرحله‌ی آخر — انتخاب مدت اعتبار، و ثبت نهایی آلارم
                        parts_ae = cbq_data.split(":", 2)
                        ae_cid = parts_ae[1] if len(parts_ae) > 1 else cbq_cid
                        ae_days = int(parts_ae[2]) if len(parts_ae) > 2 and parts_ae[2].isdigit() else 0
                        pend_ae = _pending_alarm.get(ae_cid)
                        if not pend_ae or pend_ae.get("step") != "alarm_expiry":
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد — دوباره شروع کن")
                            return
                        answer_callback(token_cbq, cbq_id, "⏳ در حال ثبت...")
                        dw_ae = pend_ae["data"]
                        ae_uname = cbq.get("from",{}).get("username","") or cbq.get("from",{}).get("first_name","")
                        is_private_ae = dw_ae.get("ptype","public") == "private"
                        sender_ae = _get_user_custom_name(ae_cid) or ae_uname
                        sym_ae = dw_ae["symbol"]
                        atype_ae = dw_ae["atype"]
                        comment_ae = dw_ae.get("comment","")
                        expires_at_ae = None
                        if ae_days > 0:
                            expires_at_ae = (datetime.now(TEHRAN) + timedelta(days=ae_days)).strftime("%Y-%m-%d %H:%M:%S")
                        new_alert_ae = {
                            "id": str(int(time.time()*1000)),
                            "symbol": sym_ae, "type": atype_ae,
                            "target_price": dw_ae["target_price"], "condition": dw_ae["condition"],
                            "comment": comment_ae, "created_by": sender_ae,
                            "active": True, "last_price": None, "last_checked": None,
                            "created_at": now_teh(),
                            "expires_at": expires_at_ae,
                            "is_private": is_private_ae,
                            "private_cid": ae_cid if is_private_ae else None,
                            "notify_only": ae_cid if is_private_ae else (YOUR_CHAT_ID if not BROADCAST_MODE else None)
                        }
                        d_ae = load_alerts()
                        d_ae["alerts"].append(new_alert_ae)
                        _sb_upsert_alert(new_alert_ae)
                        _cache_alerts = d_ae
                        del _pending_alarm[ae_cid]
                        dir_lbl_ae = "📈 BUY" if new_alert_ae["condition"] == "below" else "📉 SELL"
                        priv_lbl_ae = "  🔒 شخصی" if is_private_ae else "  🌐 تیمی"
                        expiry_lbl_ae = f"\n⏳ اعتبار: {ae_days} روز (تا {expires_at_ae[:10]})" if expires_at_ae else "\n♾ بدون انقضا"
                        confirm_ae = (
                            f"✅ <b>آلارم ثبت شد!</b>\n\n"
                            f"💰 <b>{sym_ae}</b>  {dir_lbl_ae}{priv_lbl_ae}\n"
                            f"🎯 هدف: <code>{fmt_price(new_alert_ae['target_price'], sym_ae)}</code>\n"
                            + (f"💬 {comment_ae}\n" if comment_ae else "")
                            + expiry_lbl_ae
                            + f"\n\n⏰ {now_teh()} (تهران)"
                        )
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, confirm_ae, [])
                        def _bg_ae(alert=new_alert_ae, s=sym_ae, t=atype_ae):
                            try:
                                cur = get_price(s, t)
                                if cur:
                                    alert["last_price"] = cur
                                    alert["last_checked"] = now_teh()
                                    _sb_upsert_alert(alert)
                            except: pass
                        threading.Thread(target=_bg_ae, daemon=True).start()

                    elif cbq_data.startswith("sos_dir:"):
                        # sos_dir:cid:buy|sell
                        parts_sd = cbq_data.split(":", 2)
                        sd_cid = parts_sd[1] if len(parts_sd) > 1 else cbq_cid
                        sd_raw = parts_sd[2] if len(parts_sd) > 2 else "sell"
                        pend = _pending_alarm.get(sd_cid)
                        if not pend or pend.get("step") != "sos_dir":
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد — دوباره شروع کن")
                            return
                        answer_callback(token_cbq, cbq_id)
                        dw_sd = pend["data"]
                        if sd_raw == "buy":
                            dw_sd["condition"] = "below"
                            dw_sd["dir_lbl"] = "📈 BUY"
                        else:
                            dw_sd["condition"] = "above"
                            dw_sd["dir_lbl"] = "📉 SELL"
                        pend["step"] = "sos_comment"
                        sym_sd = dw_sd.get("symbol", "")
                        dir_lbl_sd = dw_sd["dir_lbl"]
                        edit_tg_keyboard(token_cbq, sd_cid, cbq_msg_id,
                            f"⚡ <b>{sym_sd}</b>  {dir_lbl_sd}\n\nیادداشت اختیاری بنویس\nیا دکمه «بدون یادداشت» رو بزن:",
                            [
                                [{"text": "✅ ارسال بدون یادداشت", "callback_data": f"sos_nocomment:{sd_cid}"}],
                                [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{sd_cid}"}]
                            ])

                    elif cbq_data.startswith("sos_nocomment:"):
                        # کاربر بدون یادداشت ارسال کرد
                        sc_cid = cbq_data.split(":", 1)[1]
                        pend_sc = _pending_alarm.get(sc_cid)
                        if not pend_sc or pend_sc.get("step") != "sos_comment":
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد — دوباره شروع کن")
                            return
                        answer_callback(token_cbq, cbq_id, "⏳ در حال ارسال...")
                        dw_sc = pend_sc["data"]
                        sym_sc = dw_sc["symbol"]
                        condition_sc = dw_sc["condition"]
                        dir_lbl_sc = dw_sc.get("dir_lbl", "📈 BUY" if condition_sc == "below" else "📉 SELL")
                        atype_sc = "forex" if any(x in sym_sc for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                        sender_sc = _get_user_custom_name(sc_cid) or cbq_cid
                        alarm_num_tag_sc = _make_alarm_tag(sym_sc)
                        sender_tag_sc = "#" + re.sub(r"[^\w]","_", sender_sc).strip("_")
                        arrow_sc = "📈 ناحیه سل" if condition_sc == "above" else "📉 ناحیه بای"
                        try: cur_sc = get_price(sym_sc, atype_sc)
                        except: cur_sc = None
                        out_sc = (
                            f"🚨 <b>آلارم فوری!</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"💰 <b>#{sym_sc}</b>  {arrow_sc}\n"
                            f"🔖 {alarm_num_tag_sc}\n"
                            f"👤 {sender_tag_sc}\n"
                            f"📊 قیمت: <b>{fmt_price(cur_sc, sym_sc) if cur_sc else '—'}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"⏰ {now_pretty()} (تهران)"
                        )
                        _, all_cids_sc, _ = _get_token_and_cids()
                        targets_sc = all_cids_sc if BROADCAST_MODE else [YOUR_CHAT_ID]
                        del _pending_alarm[sc_cid]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"✅ <b>آلارم فوری ارسال شد!</b>\n\n"
                            f"💰 <b>{sym_sc}</b>  {dir_lbl_sc}\n"
                            f"⏰ {now_pretty()} (تهران)", [])
                        sos_aid = str(int(time.time() * 1000))
                        def _bg_sos(tok=token_cbq, tgts=targets_sc, msg=out_sc, s=sym_sc, aid=sos_aid,
                                    atag=alarm_num_tag_sc, sndr=sender_sc, cond=condition_sc, atp=atype_sc, cur=cur_sc):
                            if s.upper() not in TEMP_MUTED_SYMBOLS and not _is_instant_sender_muted(sndr):
                                _send_instant_rated(tok, tgts, msg, aid, atag, sndr)
                            # ذخیره توی archive
                            sos_arch_entry = {"id": aid, "symbol": s, "type": atp,
                                "condition": cond, "comment": "", "created_by": sndr,
                                "active": False, "fired_at": now_teh(), "fired_price": cur,
                                "instant": True, "created_at": now_teh(), "tag": atag}
                            d_arc = load_alerts()
                            d_arc.setdefault("archive", []).append(sos_arch_entry)
                            save_alerts(d_arc)
                            threading.Thread(target=_sb_upsert_alert, args=(sos_arch_entry,), daemon=True).start()
                        threading.Thread(target=_bg_sos, daemon=True).start()

                msg = upd.get("message", {})
                raw_txt = msg.get("text", "") or ""
                # normalize: /cmd@botname → /cmd
                txt = raw_txt.split("@")[0] if raw_txt.startswith("/") else raw_txt
                ch = msg.get("chat", {})
                cid = str(ch.get("id", ""))
                uname = ch.get("username", "") or ch.get("first_name", "")

                # track پیام کاربر برای پاک‌سازی
                user_msg_id = msg.get("message_id")
                if cid and user_msg_id:
                    _track_msg(cid, user_msg_id)

                # ── /start ──────────────────────────────────────────
                if txt.startswith("/start") and cid:
                    data = load_alerts()
                    users = data.get("users", [])
                    existing_user = next((u for u in users if str(u.get("chat_id","")) == cid), None)
                    existing_name = existing_user.get("custom_name","").strip() if existing_user else ""
                    # ثبت کاربر اگه جدیده
                    if not existing_user:
                        users.append({"chat_id": cid, "username": uname, "joined_at": now_teh(), "custom_name": "", "private_access": False, "approved": False})
                        data["users"] = users
                        ids = data.get("telegram", {}).get("chat_ids", [])
                        if cid not in [str(x) for x in ids]:
                            ids.append(cid)
                        data["telegram"]["chat_ids"] = ids
                        save_alerts(data)
                    is_adm = (cid == YOUR_CHAT_ID)
                    if existing_name:
                        # کاربر قدیمی که هنوز کد فعال‌سازی سایت (web_pin) نداره —
                        # الان که وارد شده، یه بار براش می‌سازیم تا دیگه کسی نتونه با تایپ کردن اسمش تو سایت جاش بزنه
                        if existing_user and not existing_user.get("web_pin"):
                            new_pin_old = f"{secrets.randbelow(1000000):06d}"
                            for u in users:
                                if str(u.get("chat_id","")) == cid:
                                    u["web_pin"] = new_pin_old
                                    break
                            data["users"] = users
                            save_alerts(data)
                            send_tg(token, cid,
                                f"🔑 <b>کد فعال‌سازی سایت شما:</b> <code>{new_pin_old}</code>\n\n"
                                "برای جلوگیری از سوءاستفاده‌ی افراد دیگه از اسمت، این کد رو یک‌بار توی سایت "
                                "(دکمه «🔑 کد فعال‌سازی» زیر نام کاربری) وارد کن.")
                        # کاربر قبلاً اسم داده — مستقیم به منو برو
                        show_main_menu(token, cid,
                            f"👋 خوش برگشتی <b>{existing_name}</b>!\n\nاز منوی زیر انتخاب کن 👇",
                            is_adm)
                    else:
                        # اولین بار — اسم بخواه
                        _pending_name[cid] = True
                        send_tg(token, cid,
                            f"👋 سلام <b>{uname}</b>!\n\n"
                            f"لطفاً <b>اسمی که در سایت استفاده می‌کنی</b> رو بنویس:\n"
                            f"(آلارم‌های شخصیت با همین اسم شناسایی میشن)")

                # ── دریافت کوئری جستجوی گزارش هفتگی ──────────────────
                elif cid in _pending_weekly_search and not txt.startswith("/"):
                    ws_info = _pending_weekly_search.pop(cid)
                    ws_which = ws_info["which"]
                    query = txt.strip().lower()

                    now_dt_ws = datetime.now(TEHRAN)
                    days_since_sat_ws = (now_dt_ws.weekday() - 5) % 7
                    this_week_start_ws = (now_dt_ws - timedelta(days=days_since_sat_ws)).replace(
                        hour=0, minute=0, second=0, microsecond=0)
                    if ws_which == "last":
                        week_start_ws = this_week_start_ws - timedelta(days=7)
                        week_end_ws   = this_week_start_ws
                    else:
                        week_start_ws = this_week_start_ws
                        week_end_ws   = None
                    week_start_str_ws = week_start_ws.strftime("%Y-%m-%dT%H:%M:%S")
                    week_label_ws = f"{week_start_ws.strftime('%d/%m')} — {(week_end_ws - timedelta(days=1)).strftime('%d/%m') if week_end_ws else 'الان'}"

                    rows_ws = []
                    if SUPABASE_KEY:
                        try:
                            url_ws = (f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                                      f"?fired_at=gte.{week_start_str_ws}&select=*&order=fired_at.asc")
                            if week_end_ws:
                                url_ws += f"&fired_at=lt.{week_end_ws.strftime('%Y-%m-%dT%H:%M:%S')}"
                            r_ws = requests.get(url_ws, headers=_sb_h(), timeout=10)
                            if r_ws.status_code == 200:
                                rows_ws = r_ws.json()
                        except Exception as e:
                            print(f"[weekly_search] load exc: {e}")

                    # لود فقط alertهایی که تو این بازه‌ی هفته هستن (نه کل جدول)
                    ws_ids = sorted({str(r.get("id","")) for r in rows_ws if r.get("id")})
                    alerts_by_id_ws = {}
                    if SUPABASE_KEY and ws_ids:
                        try:
                            r_ws2 = requests.get(
                                f"{SUPABASE_URL}/rest/v1/alerts?id=in.({','.join(ws_ids)})&select=*",
                                headers=_sb_h(), timeout=10)
                            if r_ws2.status_code == 200:
                                for row_ws in r_ws2.json():
                                    alerts_by_id_ws[str(row_ws.get("id",""))] = row_ws
                        except Exception as e:
                            print(f"[weekly_search] targeted alerts fetch error: {e}")
                    all_alerts_ws = list(alerts_by_id_ws.values())
                    private_ids_ws = {str(a["id"]) for a in all_alerts_ws if a.get("is_private")}
                    rows_ws = [r for r in rows_ws if str(r.get("id","")) not in private_ids_ws]

                    # فیلتر بر اساس کوئری — مسئول، نماد، تگ، سازنده
                    filtered = []
                    for row_ws in rows_ws:
                        aid_ws  = str(row_ws.get("id",""))
                        alert_ws = alerts_by_id_ws.get(aid_ws, {})
                        sym_ws  = (alert_ws.get("symbol","") or row_ws.get("symbol","") or "").lower()
                        tag_ws  = (row_ws.get("alarm_tag","") or "").lower()
                        assignee_ws = (row_ws.get("assigned_to","") or "").lower()
                        creator_ws  = (alert_ws.get("created_by","") or row_ws.get("created_by","") or "").lower()
                        if query in sym_ws or query in tag_ws or query in assignee_ws or query in creator_ws:
                            filtered.append(row_ws)

                    if not filtered:
                        send_tg(token, cid, f"🔍 نتیجه‌ای برای «<b>{txt.strip()}</b>» پیدا نشد.")
                    else:
                        lines_ws = [f"🔍 <b>نتایج جستجو: {txt.strip()}</b>",
                                     f"<i>{week_label_ws} — {len(filtered)} نتیجه</i>", ""]
                        for row_ws in filtered[:10]:
                            aid_ws       = str(row_ws.get("id",""))
                            tag_ws       = row_ws.get("alarm_tag","—")
                            assignee_ws  = row_ws.get("assigned_to","") or "—"
                            fired_ws     = row_ws.get("fired_at","")[:16]
                            is_active_ws = row_ws.get("is_active", True)
                            false_by_ws  = row_ws.get("false_by","") or ""
                            false_at_ws  = row_ws.get("false_at","")[:16] if row_ws.get("false_at") else ""
                            false_rsn_ws = row_ws.get("false_reason","") or ""
                            alert_ws     = alerts_by_id_ws.get(aid_ws, {})
                            sym_ws       = alert_ws.get("symbol","") or row_ws.get("symbol","") or ""
                            tgt_raw_ws   = alert_ws.get("target_price",0) or row_ws.get("target_price",0) or 0
                            target_ws    = fmt_price(float(tgt_raw_ws), sym_ws) if tgt_raw_ws else "—"
                            creator_ws   = alert_ws.get("created_by","") or row_ws.get("created_by","") or "—"
                            created_ws   = str(alert_ws.get("created_at",""))[:16]
                            cond_ws      = alert_ws.get("condition","") or row_ws.get("condition","")
                            dir_ws       = "📈 ناحیه سل" if cond_ws == "above" else ("📉 ناحیه بای" if cond_ws == "below" else "")
                            lines_ws.append(f"🔖 <b>{tag_ws}</b>  |  #{sym_ws}" + (f"  |  {dir_ws}" if dir_ws else ""))
                            if created_ws:
                                lines_ws.append(f"📅 ثبت: {created_ws}")
                            lines_ws.append(f"🎯 هدف: <code>{target_ws}</code>")
                            lines_ws.append(f"👤 سازنده: {creator_ws}")
                            lines_ws.append(f"⏰ فایر شد: {fired_ws}")
                            lines_ws.append(f"🙋 مسئول: {assignee_ws}")
                            if is_active_ws:
                                lines_ws.append(f"✅ وضعیت: فعال")
                            else:
                                hist_ws = row_ws.get("false_history") or []
                                if isinstance(hist_ws, str):
                                    try: hist_ws = json.loads(hist_ws)
                                    except: hist_ws = []
                                if hist_ws:
                                    lines_ws.append("❌ <b>تاریخچه False:</b>")
                                    for i_hw, h_ws in enumerate(hist_ws, 1):
                                        h_at_w  = str(h_ws.get("at",""))[:16]
                                        h_by_w  = h_ws.get("by","")
                                        h_rsn_w = h_ws.get("reason","")
                                        h_line_w = f"  {i_hw}. {h_by_w}  |  {h_at_w}"
                                        if h_rsn_w:
                                            h_line_w += f"\n     📝 {h_rsn_w}"
                                        lines_ws.append(h_line_w)
                                else:
                                    lines_ws.append(f"❌ وضعیت: False — {false_by_ws}  |  {false_at_ws}")
                                    if false_rsn_ws:
                                        lines_ws.append(f"📝 علت: {false_rsn_ws}")
                            lines_ws.append("──────────────")
                        if len(filtered) > 10:
                            lines_ws.append(f"<i>... و {len(filtered)-10} مورد دیگر</i>")
                        full_ws = "\n".join(lines_ws)
                        if len(full_ws) > 4000:
                            full_ws = full_ws[:3980] + "\n\n<i>...</i>"
                        send_tg(token, cid, full_ws)

                # ── دریافت اسم custom بعد از /start یا /setname ──────
                elif cid in _pending_name and not txt.startswith("/"):
                    custom_name = txt.strip()
                    if len(custom_name) < 2:
                        send_tg(token, cid, "⚠️ اسم باید حداقل ۲ حرف باشه. دوباره بنویس:")
                    else:
                        data = load_alerts()
                        users = data.get("users", [])
                        found = False
                        for usr in users:
                            if str(usr.get("chat_id", "")) == cid:
                                usr["custom_name"] = custom_name
                                found = True
                                break
                        if not found:
                            users.append({"chat_id": cid, "username": uname, "joined_at": now_teh(), "custom_name": custom_name, "approved": False})
                            data["users"] = users
                            ids = data.get("telegram", {}).get("chat_ids", [])
                            if cid not in [str(x) for x in ids]:
                                ids.append(cid)
                            data["telegram"]["chat_ids"] = ids
                        data["users"] = users
                        save_alerts(data)
                        del _pending_name[cid]
                        is_adm = (cid == YOUR_CHAT_ID)
                        show_main_menu(token, cid,
                            f"✅ خوش اومدی <b>{custom_name}</b>!\n\n"
                            f"آلارم‌هات با اسم <b>{custom_name}</b> شناسایی میشن.\n"
                            f"از منوی زیر انتخاب کن 👇",
                            is_adm)

                # ── /setname — تغییر اسم ────────────────────────────
                elif txt.startswith("/setname"):
                    _pending_name[cid] = True
                    data = load_alerts()
                    users = data.get("users", [])
                    cur = next((u.get("custom_name","") for u in users if str(u.get("chat_id",""))==cid), "")
                    cur_info = f"\nاسم فعلی: <b>{cur}</b>" if cur else ""
                    send_tg(token, cid, f"✏️ اسم جدیدت رو بنویس:{cur_info}")

                # ── /del — حذف پیام fired از همه چت‌ها ────────────
                # فرمت‌ها: ریپلای روی پیام + /del
                #          یا /del XAUUSD7
                elif txt.strip().startswith("/del"):
                    del_parts = txt.strip().split(maxsplit=1)
                    del_tag = del_parts[1].upper().lstrip("#") if len(del_parts) > 1 else None
                    replied = msg.get("reply_to_message", {})
                    replied_mid = replied.get("message_id")

                    target_aid = None

                    if del_tag:
                        # جستجو با هشتگ — مثلاً /del XAUUSD7
                        tag_search = f"#{del_tag}"
                        for aid, cid_map in _fired_msg_ids.items():
                            if cid_map.get("__tag__") == tag_search:
                                target_aid = aid
                                break
                        if not target_aid:
                            send_tg(token, cid, f"⚠️ آلارم <b>{tag_search}</b> پیدا نشد یا قبلاً پاک شده.")
                    elif replied_mid:
                        # جستجو با ریپلای
                        for aid, cid_map in _fired_msg_ids.items():
                            if str(cid_map.get(cid)) == str(replied_mid):
                                target_aid = aid
                                break
                        if not target_aid:
                            send_tg(token, cid, "⚠️ این پیام توی لیست آلارم‌های ذخیره‌شده نیست یا قبلاً پاک شده.")
                    else:
                        send_tg(token, cid, "⚠️ روی پیام آلارم ریپلای بزن یا بنویس /del XAUUSD7")

                    if target_aid:
                        cid_map = _fired_msg_ids.pop(target_aid, {})
                        deleted_count = 0
                        for tc, tm in cid_map.items():
                            if tc in ("__tag__", "__text__"): continue
                            try:
                                r_del = requests.post(
                                    f"https://api.telegram.org/bot{token}/deleteMessage",
                                    json={"chat_id": tc, "message_id": tm},
                                    timeout=5, headers=H)
                                if r_del.status_code == 200:
                                    deleted_count += 1
                            except: pass
                        # حذف پیام fired از Supabase
                        threading.Thread(target=_sb_delete_fired_msgs, args=(target_aid,), daemon=True).start()
                        # حذف خود آلارم از Supabase — تا دیگه fire نشه
                        threading.Thread(target=_sb_delete_alert, args=(target_aid,), daemon=True).start()
                        _cache_alerts = None
                        send_tg(token, cid, f"🗑 پیام آلارم از <b>{deleted_count}</b> چت پاک شد.")

                # ── /false — آلارم منقضی/ترید شده، از لیست تریگر خارج بشه ──
                elif txt.strip().lower() in ("/false",) or txt.strip().lower().startswith("/false"):
                    # کامنت بعد از /False — مثلاً: /False شکسته شد ناحیه
                    # یا /False XAUUSD5 برای جستجو با هشتگ
                    false_parts = txt.strip().split(maxsplit=1)
                    false_arg = false_parts[1].strip() if len(false_parts) > 1 else ""
                    false_reason = ""
                    false_tag_search = None
                    # چک کن اول arg یه هشتگ/تگ نماد هست؟ (مثلاً XAUUSD5 یا #XAUUSD5)
                    if false_arg:
                        candidate = false_arg.upper().lstrip("#").split()[0]
                        # اگه شبیه تگ نماد باشه (حروف + عدد، بدون فاصله)
                        import re as _re2
                        if _re2.match(r'^[A-Z0-9]+$', candidate) and any(c.isdigit() for c in candidate):
                            false_tag_search = f"#{candidate}"
                            # بقیه arg رو reason بدون
                            rest = false_arg[len(candidate):].strip().lstrip("#").strip()
                            false_reason = rest
                        else:
                            false_reason = false_arg
                    false_tag = None
                    replied = msg.get("reply_to_message", {})
                    replied_mid = replied.get("message_id")
                    target_aid_false = None
                    # ۱. جستجو با هشتگ (اگه داده شده)
                    if false_tag_search:
                        for aid_f, cid_map_f in _fired_msg_ids.items():
                            if cid_map_f.get("__tag__") == false_tag_search:
                                target_aid_false = aid_f
                                false_tag = cid_map_f.get("__tag__", "")
                                break
                        if not target_aid_false:
                            send_tg(token, cid, f"⚠️ آلارم <b>{false_tag_search}</b> پیدا نشد یا قبلاً پاک شده.\n\nشاید سرور restart شده — مستقیم روی پیام آلارم ریپلای بزن.")
                    # ۲. جستجو با ریپلای
                    elif replied_mid:
                        for aid_f, cid_map_f in _fired_msg_ids.items():
                            if str(cid_map_f.get(cid)) == str(replied_mid):
                                target_aid_false = aid_f
                                false_tag = cid_map_f.get("__tag__", "")
                                break
                    if target_aid_false:
                        sender_name_false = _get_user_custom_name(cid) or uname

                        # جلوگیری از race condition — اگه الان داره false میشه، ignore کن
                        with _false_in_progress_lock:
                            _fp_busy = target_aid_false in _false_in_progress
                            if not _fp_busy:
                                _false_in_progress.add(target_aid_false)

                        if _fp_busy:
                            send_tg(token, cid, "⏳ این آلارم الان داره پردازش میشه، چند ثانیه صبر کن.")
                        else:
                            try:
                                # چک کن این آلارم قبلاً false شده یا نه (sync — قبل از ارسال پیام)
                                already_false = False
                                if SUPABASE_KEY:
                                    try:
                                        r_chk = requests.get(
                                            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{target_aid_false}&select=is_active",
                                            headers=_sb_h(), timeout=8)
                                        if r_chk.status_code == 200:
                                            chk_rows = r_chk.json()
                                            if chk_rows:
                                                already_false = (chk_rows[0].get("is_active") == False)
                                    except Exception as e:
                                        print(f"[false] check exc: {e}")

                                def _false_then_rebuild(aid=target_aid_false, sender=sender_name_false, reason=false_reason):
                                    # اول false رو تو Supabase ذخیره کن، بعد شمارش رو بازسازی کن —
                                    # اگه این دوتا موازی اجرا بشن، ممکنه rebuild قبل از ثبت is_active=False
                                    # از Supabase بخونه و شمارش طرف false‌کننده درست کم نشه (race condition)
                                    _sb_false_assignment(aid, sender, reason)
                                    _rebuild_active_assign_count(_sb_load_active_assignments())
                                threading.Thread(target=_false_then_rebuild, daemon=True).start()
                                tag_txt = f" <b>{false_tag}</b>" if false_tag else ""
                                reason_line = f"\n📝 علت: {false_reason}" if false_reason else ""
                                now_label = now_pretty()

                                false_cid_map = _fired_msg_ids.get(target_aid_false, {})
                                prev_broadcast_map = _false_broadcast_ids.get(target_aid_false, {})
                                if not prev_broadcast_map and already_false and SUPABASE_KEY:
                                    try:
                                        r_bm = requests.get(
                                            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{target_aid_false}&select=false_broadcast_map",
                                            headers=_sb_h(), timeout=8)
                                        if r_bm.status_code == 200:
                                            bm_rows = r_bm.json()
                                            if bm_rows and bm_rows[0].get("false_broadcast_map"):
                                                prev_broadcast_map = bm_rows[0]["false_broadcast_map"]
                                                if isinstance(prev_broadcast_map, str):
                                                    prev_broadcast_map = json.loads(prev_broadcast_map)
                                    except Exception as e:
                                        print(f"[false] load broadcast_map exc: {e}")
                                new_broadcast_map = {}

                                if already_false and prev_broadcast_map:
                                    # ── آپدیت — ریپلای روی پیام False قبلی، متن قبلی حذف نمیشه
                                    update_msg = (
                                        f"🔄 <b>آپدیت</b> — {now_label}\n"
                                        f"👤 توسط: <b>{sender_name_false}</b>"
                                        f"{reason_line}"
                                    )
                                    for tc, tm in false_cid_map.items():
                                        if tc in ("__tag__", "__text__"): continue
                                        reply_target = prev_broadcast_map.get(tc, tm)
                                        try:
                                            r_send = requests.post(
                                                f"https://api.telegram.org/bot{token}/sendMessage",
                                                json={"chat_id": tc, "text": update_msg,
                                                      "parse_mode": "HTML", "reply_to_message_id": reply_target},
                                                timeout=8, headers=H)
                                            if r_send.status_code == 200:
                                                new_mid = r_send.json().get("result", {}).get("message_id")
                                                if new_mid:
                                                    new_broadcast_map[tc] = new_mid
                                        except: pass
                                else:
                                    # ── اولین بار false میشه
                                    false_broadcast = (
                                        f"❌ آلارم{tag_txt} از لیست تریگر خارج شد\n"
                                        f"👤 توسط: <b>{sender_name_false}</b>"
                                        f"{reason_line}"
                                    )
                                    for tc, tm in false_cid_map.items():
                                        if tc in ("__tag__", "__text__"): continue
                                        try:
                                            r_send = requests.post(
                                                f"https://api.telegram.org/bot{token}/sendMessage",
                                                json={"chat_id": tc, "text": false_broadcast,
                                                      "parse_mode": "HTML", "reply_to_message_id": tm},
                                                timeout=8, headers=H)
                                            if r_send.status_code == 200:
                                                new_mid = r_send.json().get("result", {}).get("message_id")
                                                if new_mid:
                                                    new_broadcast_map[tc] = new_mid
                                        except: pass

                                if new_broadcast_map:
                                    _false_broadcast_ids[target_aid_false] = new_broadcast_map
                                    threading.Thread(
                                        target=lambda aid=target_aid_false, m=new_broadcast_map: requests.patch(
                                            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{aid}",
                                            headers={**_sb_h(), "Prefer": "return=minimal"},
                                            json={"false_broadcast_map": m}, timeout=8) if SUPABASE_KEY else None,
                                        daemon=True).start()
                            finally:
                                # همیشه lock رو آزاد کن
                                with _false_in_progress_lock:
                                    _false_in_progress.discard(target_aid_false)
                    else:
                        send_tg(token, cid, "⚠️ روی پیام آلارم ریپلای بزن و /False بنویس.\n\n💡 اگه آلارم قبل از restart سرور آمده، با /False XAUUSD5 (هشتگ آلارم) امتحان کن.")

                # ── /check — ثبت در ژورنال روی پیام ─────────────
                elif txt.strip().startswith("/check"):
                    check_parts = txt.strip().split(maxsplit=1)
                    check_note = check_parts[1].strip() if len(check_parts) > 1 else ""
                    replied = msg.get("reply_to_message", {})
                    replied_mid = replied.get("message_id")
                    replied_text = replied.get("text") or replied.get("caption") or ""
                    if not replied_mid:
                        send_tg(token, cid, "⚠️ باید روی پیام آلارم ریپلای بزنی و /check بنویسی.")
                    else:
                        target_aid = None
                        for aid, cid_map in _fired_msg_ids.items():
                            if str(cid_map.get(cid)) == str(replied_mid):
                                target_aid = aid
                                break
                        if not target_aid:
                            send_tg(token, cid, "⚠️ این پیام توی لیست آلارم‌های ذخیره‌شده نیست یا قبلاً ثبت شده.")
                        else:
                            cid_map = _fired_msg_ids.get(target_aid, {})
                            orig_text = cid_map.get("__text__") or replied_text
                            note_line = f"\n🗒 {check_note}" if check_note else ""
                            journal_line = f"\n──────────────\n📋 بررسی شد{note_line}\n🕐 {now_pretty()}"
                            new_text = orig_text + journal_line
                            edited_count = 0
                            for tc, tm in cid_map.items():
                                if tc in ("__tag__", "__text__"): continue
                                try:
                                    r_edit = requests.post(
                                        f"https://api.telegram.org/bot{token}/editMessageText",
                                        json={"chat_id": tc, "message_id": tm,
                                              "text": new_text, "parse_mode": "HTML"},
                                        timeout=8, headers=H)
                                    if r_edit.status_code == 200:
                                        edited_count += 1
                                except: pass
                            if edited_count:
                                send_tg(token, cid, f"✅ در <b>{edited_count}</b> چت ثبت شد.")
                            else:
                                send_tg(token, cid, "⚠️ نشد ویرایش کرد.")
                elif txt == "📩 درخواست فعال‌سازی آلارم":
                    if _is_approved(cid):
                        show_main_menu(token, cid, "✅ شما قبلاً تایید شدید و به همه امکانات دسترسی دارید.", cid == YOUR_CHAT_ID)
                    else:
                        send_tg(token, cid,
                            "📩 <b>درخواست شما ثبت شد</b>\n\n"
                            "درخواست فعال‌سازی شما برای ادمین ارسال شد و در دست بررسی است.\n"
                            "پس از تایید، یک پیام دریافت خواهید کرد. 🙏")
                        d_su = load_alerts()
                        su_user = next((u for u in d_su.get("users",[]) if str(u.get("chat_id","")) == cid), {})
                        su_name = su_user.get("custom_name","") or su_user.get("username","") or cid
                        admin_notif_su = (
                            f"📩 <b>درخواست فعال‌سازی عضو جدید</b>\n\n"
                            f"👤 نام: <b>{su_name}</b>\n"
                            f"🆔 Chat ID: <code>{cid}</code>\n"
                            f"⏰ {now_pretty()} (تهران)"
                        )
                        approve_kb_su = [
                            [{"text": "✅ تایید و فعال‌سازی", "callback_data": f"approve_signup:{cid}"}],
                            [{"text": "❌ رد درخواست",       "callback_data": f"reject_signup:{cid}"}],
                        ]
                        send_tg_keyboard(token, YOUR_CHAT_ID, admin_notif_su, approve_kb_su)

                elif txt in ("📊 وضعیت",) or (txt.startswith("/status") and txt not in ("/statuspage",)):
                    d2 = load_alerts()
                    all_active2 = [a for a in d2.get("alerts",[]) if a.get("active")]
                    team_active2 = [a for a in all_active2 if not a.get("is_private")]
                    private_active2 = [a for a in all_active2 if a.get("is_private")]
                    is_open = is_forex_market_open()
                    is_adm = (cid == YOUR_CHAT_ID)
                    has_priv = _has_private_access(cid)
                    status_text = (
                        f"📊 <b>وضعیت سیستم</b>\n\n"
                        f"{'🟢' if is_open else '🔴'} فارکس: {'باز' if is_open else 'بسته'}\n"
                        f"📈 آلارم فعال (کل): <b>{len(all_active2)}</b>\n"
                        f"🌐 تیمی: <b>{len(team_active2)}</b> | 🔒 شخصی: <b>{len(private_active2)}</b>\n"
                        f"🔒 آلارم شخصی: {'✅ فعال' if has_priv else '❌ غیرفعال'}\n"
                        f"⏱ {now_pretty()} (تهران)"
                    )
                    status_kb = []
                    if not has_priv:
                        status_kb.append([{"text": "📩 درخواست فعال‌سازی آلارم شخصی", "callback_data": f"req_private:{cid}"}])
                    status_kb.append([{"text": "✏️ ویرایش اسم", "callback_data": f"edit_name:{cid}"}])
                    status_kb.append([{"text": "📡 سیگنال‌های من", "callback_data": f"signals_view:{cid}:mine"},
                                      {"text": "📊 همه سیگنال‌ها", "callback_data": f"signals_view:{cid}:all"}])
                    status_kb.append([{"text": "🎯 لیست تریگر", "callback_data": f"trigger_list:{cid}"}])
                    status_kb.append([{"text": "📊 آنالیز هفتگی", "callback_data": f"weekly_menu:{cid}"}])
                    status_kb.append([{"text": "🔔 نمایش آلارم‌های فعال", "callback_data": f"resend_active:{cid}"}])
                    send_tg_keyboard(token, cid, status_text, status_kb)

                elif txt == "⭐ آلارم‌های من":
                    is_adm = (cid == YOUR_CHAT_ID)
                    has_priv = _has_private_access(cid)
                    btns = [{"text": "🌐 آلارم‌های تیمی", "callback_data": f"myalerts:pub:{cid}"}]
                    if has_priv:
                        btns.append({"text": "🔒 آلارم‌های شخصی", "callback_data": f"myalerts:priv:{cid}"})
                    btns = [[b] if isinstance(b, dict) else b for b in btns]
                    btns.append([{"text": "📋 همه آلارم‌های من", "callback_data": f"myalerts:all:{cid}"}])
                    btns.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                    send_tg_keyboard(token, cid, "⭐ <b>آلارم‌های من</b>\n\nکدوم رو می‌خوای ببینی؟", btns)

                elif txt == "📡 سیگنال جدید":
                    # مرحله ۱ — نماد
                    quick_btns = [[{"text": s, "callback_data": f"sig_sym:{cid}:{s}"} for s in SIGNAL_QUICK_SYMBOLS[:3]],
                                  [{"text": s, "callback_data": f"sig_sym:{cid}:{s}"} for s in SIGNAL_QUICK_SYMBOLS[3:]],
                                  [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]]
                    mid_sig = send_tg_keyboard(token, cid,
                        "📡 <b>سیگنال جدید</b>\n\nنماد رو انتخاب کن یا بنویس:",
                        quick_btns)
                    _pending_signal[cid] = {"step": "sig_symbol", "data": {}, "bot_msg_id": mid_sig}

                elif txt == "⚙️ پنل ادمین" and cid == YOUR_CHAT_ID:
                    admin_kb = [
                        [{"text": "📰 اخبار فارکس",       "callback_data": "admin:news"}],
                        [{"text": "✉️ پیام به گروه",      "callback_data": "admin:broadcast"}],
                        [{"text": "👥 لیست کاربران",       "callback_data": "admin:users"}],
                        [{"text": "🗑 مدیریت سیگنال‌ها",  "callback_data": "admin_sig:list:1"}],
                        # (غیرفعال — تقسیم آلارم کنسل شده) [{"text": "📋 مسئولین آلارم",         "callback_data": "admin:shift:1"}],
                        [{"text": "❌ فالس همه آلارم‌های فعال", "callback_data": "admin:bulkfalse:0"}],
                        [{"text": "✕ بستن",                "callback_data": "close_myalerts"}],
                    ]
                    send_tg_keyboard(token, cid,
                        "⚙️ <b>پنل ادمین</b>\n\nیه گزینه انتخاب کن:", admin_kb)


                elif txt == "❌ انصراف":
                    pend_cancel = _pending_alarm.pop(cid, None)
                    if pend_cancel and pend_cancel.get("bot_msg_id"):
                        edit_tg_keyboard(token, cid, pend_cancel["bot_msg_id"],
                            "❌ <b>عملیات لغو شد.</b>", [])

                elif txt == "📈 آلارم جدید" and (cid == YOUR_CHAT_ID or (BROADCAST_MODE and _is_approved(cid))):
                    kb_new = [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]]
                    mid_new = send_tg_keyboard(token, cid,
                        "🔔 <b>آلارم جدید</b>\n\nاسم نماد رو بنویس:\n<code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                        kb_new)
                    _pending_alarm[cid] = {"step": "alarm_symbol", "data": {"ptype": "public"}, "bot_msg_id": mid_new}

                elif txt == "🔒 آلارم شخصی":
                    if not _has_private_access(cid):
                        is_adm = (cid == YOUR_CHAT_ID)
                        show_main_menu(token, cid, "⚠️ این قابلیت برای شما فعال نیست.\nاز بخش 📊 وضعیت می‌توانید درخواست دسترسی بدهید.", is_adm)
                    else:
                        kb_priv = [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]]
                        mid_priv = send_tg_keyboard(token, cid,
                            "🔒 <b>آلارم شخصی</b>\n\nاسم نماد رو بنویس:\n<code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                            kb_priv)
                        _pending_alarm[cid] = {"step": "alarm_symbol", "data": {"ptype": "private"}, "bot_msg_id": mid_priv}


                elif txt == "⚡ آلارم فوری" and (cid == YOUR_CHAT_ID or (BROADCAST_MODE and _is_approved(cid))):
                    kb_sos = [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]]
                    mid_sos = send_tg_keyboard(token, cid,
                        "⚡ <b>آلارم فوری</b>\n\nاسم نماد رو بنویس:\n<code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                        kb_sos)
                    _pending_alarm[cid] = {"step": "sos_symbol", "data": {}, "bot_msg_id": mid_sos}

                elif cid in _pending_bulk_false and not txt.startswith("/") and cid == YOUR_CHAT_ID:
                    bf_info = _pending_bulk_false.pop(cid)
                    bf_mid = bf_info.get("bot_msg_id")
                    bf_reason_txt = txt.strip()
                    send_tg(token, cid, f"⏳ در حال false کردن {bf_info.get('count',0)} آلارم...")

                    def _do_bulk_false_txt(reason=bf_reason_txt, tok=token, c=cid, mid=bf_mid):
                        rows_bft = _sb_load_active_assignments()
                        admin_name_bft = _get_user_custom_name(c) or "ادمین"
                        done_count_t = 0
                        for row_bft in rows_bft:
                            aid_bft = row_bft.get("id")
                            tag_bft = row_bft.get("alarm_tag", "")
                            _sb_false_assignment(aid_bft, admin_name_bft, reason)
                            done_count_t += 1
                            msg_map_bft = _fired_msg_ids.get(aid_bft, {})
                            reason_line_bft = f"\n📝 علت: {reason}" if reason else ""
                            reply_bft = (f"❌ <b>فالس (دسته‌جمعی توسط ادمین)</b>\n\n"
                                         f"{tag_bft}{reason_line_bft}")
                            for tc_bft, tm_bft in msg_map_bft.items():
                                if tc_bft in ("__tag__", "__text__"): continue
                                try:
                                    requests.post(
                                        f"https://api.telegram.org/bot{tok}/sendMessage",
                                        json={"chat_id": tc_bft, "text": reply_bft,
                                              "parse_mode": "HTML", "reply_to_message_id": tm_bft},
                                        timeout=8, headers=H)
                                except: pass
                        _rebuild_active_assign_count(_sb_load_active_assignments())
                        edit_tg_keyboard(tok, c, mid,
                            f"✅ <b>{done_count_t} آلارم false شدن</b>\n📝 علت: {reason}",
                            [[{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}]])
                    threading.Thread(target=_do_bulk_false_txt, daemon=True).start()

                elif _rate_pending_valid(cid) and not txt.startswith("/"):
                    pr = _pending_rate.pop(cid, None)
                    if pr:
                        for k in list(_pending_rate.keys()):
                            pv = _pending_rate.get(k)
                            if isinstance(pv, dict) and pv.get("alert_id") == pr.get("alert_id") and pv.get("voter_name") == pr.get("voter_name"):
                                _pending_rate.pop(k, None)
                        rt_aid = pr["alert_id"]
                        rt_score = pr["score"]
                        voter_name_rt = pr["voter_name"]
                        creator_rt = pr.get("creator", "")
                        reason_rt = "" if txt.strip() in ("-", "—", "بدون دلیل", ".") else txt.strip()[:300]
                        if _sb_save_rating(rt_aid, cid, voter_name_rt, rt_score, reason_rt, creator_rt):
                            _refresh_rating_messages(token, rt_aid, creator_rt)
                            if pr.get("prompt_mid"):
                                _delete_msg_after(token, cid, pr["prompt_mid"], delay=0)
                            if user_msg_id:
                                _delete_msg_after(token, cid, user_msg_id, delay=0)  # متن کامنت کاربر هم پاک می‌شه (دلیل تو پیام نظرسنجی دیده می‌شه)
                            threading.Thread(target=_rate_try_notify, args=(token, rt_aid, creator_rt), daemon=True).start()
                        else:
                            _pending_rate[cid] = pr  # دوباره فرصت بده
                            send_tg(token, cid, "❌ دلیل ثبت نشد، دوباره بفرست یا «رد کردن» رو بزن")

                elif cid in _pending_alarm and not txt.startswith("/"):
                    step = _pending_alarm[cid]["step"]
                    dw   = _pending_alarm[cid]["data"]
                    bot_msg_id = _pending_alarm[cid].get("bot_msg_id")
                    is_adm = (cid == YOUR_CHAT_ID)

                    # ── لغو در هر مرحله ─────────────────────────────────
                    if txt in ("↩️ برگشت", "❌ انصراف"):
                        del _pending_alarm[cid]
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id, "❌ <b>عملیات لغو شد.</b>", [])
                        # پیام کاربر رو ادیت نکن، فقط state رو پاک کردیم

                    elif step == "alarm_symbol":
                        sym_w = txt.upper().replace("/","")
                        if len(sym_w) < 2:
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    "🔔 <b>آلارم جدید</b>\n\n❌ نماد نامعتبر.\nاسم نماد رو بنویس:\n<code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                                    [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]])
                        else:
                            dw["symbol"] = sym_w
                            dw["atype"]  = "forex" if any(x in sym_w for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                            _pending_alarm[cid]["step"] = "alarm_dir"
                            ptype_lbl = "🔒 شخصی" if dw.get("ptype") == "private" else "🌐 تیمی"
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"🔔 <b>{sym_w}</b>  ({ptype_lbl})\n\nجهت معامله رو انتخاب کن:",
                                    [
                                        [{"text": "📈 BUY", "callback_data": f"alarm_dir:{cid}:buy"},
                                         {"text": "📉 SELL", "callback_data": f"alarm_dir:{cid}:sell"}],
                                        [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                    ])

                    elif step == "alarm_dir":
                        # این step از inline callback میاد (alarm_dir:cid:buy|sell)
                        # اگه کاربر text فرستاد، remind کن
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id,
                                f"🔔 <b>{dw.get('symbol','')}</b>\n\nلطفاً از دکمه‌های زیر انتخاب کن:",
                                [
                                    [{"text": "📈 BUY", "callback_data": f"alarm_dir:{cid}:buy"},
                                     {"text": "📉 SELL", "callback_data": f"alarm_dir:{cid}:sell"}],
                                    [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                ])

                    elif step == "alarm_price":
                        try:
                            dw["target_price"] = float(txt.replace(",",""))
                            _pending_alarm[cid]["step"] = "alarm_comment"
                            dir_lbl2 = "📈 BUY" if dw["condition"] == "below" else "📉 SELL"
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"🔔 <b>{dw['symbol']}</b>  {dir_lbl2}  @  <code>{fmt_price(dw['target_price'], dw['symbol'])}</code>\n\n"
                                    f"یادداشت بنویس یا بدون یادداشت ثبت کن:",
                                    [
                                        [{"text": "✅ ثبت بدون یادداشت", "callback_data": f"alarm_submit:{cid}"}],
                                        [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                    ])
                        except ValueError:
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"🔔 <b>{dw.get('symbol','')}</b>\n\n❌ عدد نامعتبر. مثال: <code>1.08500</code> یا <code>2350</code>\n\nدوباره قیمت هدف رو بنویس:",
                                    [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]])

                    elif step == "alarm_comment":
                        comment_w = "" if txt in ("✅ ثبت بدون یادداشت", "✅ ثبت") else txt
                        dw["comment"] = comment_w
                        _pending_alarm[cid]["step"] = "alarm_expiry"
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id,
                                f"🔔 <b>{dw['symbol']}</b>\n\n⏳ این آلارم چند روز معتبر باشه؟ (اگه تا اون موقع فایر نشه، خودکار حذف می‌شه)",
                                [
                                    [{"text": "۱ روز", "callback_data": f"alarm_expiry:{cid}:1"},
                                     {"text": "۳ روز", "callback_data": f"alarm_expiry:{cid}:3"},
                                     {"text": "۷ روز", "callback_data": f"alarm_expiry:{cid}:7"}],
                                    [{"text": "♾ بدون انقضا", "callback_data": f"alarm_expiry:{cid}:0"}],
                                    [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                ])

                    elif step == "alarm_expiry":
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id,
                                f"🔔 <b>{dw.get('symbol','')}</b>\n\nلطفاً از دکمه‌های زیر مدت اعتبار رو انتخاب کن:",
                                [
                                    [{"text": "۱ روز", "callback_data": f"alarm_expiry:{cid}:1"},
                                     {"text": "۳ روز", "callback_data": f"alarm_expiry:{cid}:3"},
                                     {"text": "۷ روز", "callback_data": f"alarm_expiry:{cid}:7"}],
                                    [{"text": "♾ بدون انقضا", "callback_data": f"alarm_expiry:{cid}:0"}],
                                    [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                ])

                    elif step == "edit_name_input":
                        new_name = txt.strip()
                        if len(new_name) < 2:
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    "✏️ <b>ویرایش اسم</b>\n\n❌ اسم باید حداقل ۲ حرف باشه.\nدوباره بنویس:",
                                    [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]])
                        else:
                            d_en2 = load_alerts()
                            found_en = False
                            for u in d_en2.get("users", []):
                                if str(u.get("chat_id","")) == cid:
                                    u["custom_name"] = new_name
                                    found_en = True
                                    break
                            if not found_en:
                                d_en2.setdefault("users",[]).append({
                                    "chat_id": cid, "username": uname,
                                    "joined_at": now_teh(), "custom_name": new_name
                                })
                            save_alerts(d_en2)
                            del _pending_alarm[cid]
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"✅ <b>اسم با موفقیت ذخیره شد!</b>\n\nاسم جدید: <b>{new_name}</b>", [])

                    elif step == "sos_symbol":
                        sym_w2 = txt.upper().replace("/","")
                        if len(sym_w2) < 2:
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    "⚡ <b>آلارم فوری</b>\n\n❌ نماد نامعتبر.\nمثال: <code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                                    [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]])
                        else:
                            dw["symbol"] = sym_w2
                            _pending_alarm[cid]["step"] = "sos_dir"
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"⚡ <b>آلارم فوری</b>  ─  <b>{sym_w2}</b>\n\nجهت معامله رو انتخاب کن:",
                                    [
                                        [{"text": "📈 BUY", "callback_data": f"sos_dir:{cid}:buy"},
                                         {"text": "📉 SELL", "callback_data": f"sos_dir:{cid}:sell"}],
                                        [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                    ])


                    elif step == "sos_dir":
                        # این step دیگه از text نمیاد — از callback میاد (sos_dir:cid:buy/sell)
                        # اگه کاربر text فرستاد، remind کن که از دکمه استفاده کنه
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id,
                                f"⚡ <b>آلارم فوری</b>  ─  <b>{dw.get('symbol','')}</b>\n\nلطفاً از دکمه‌های زیر انتخاب کن:",
                                [
                                    [{"text": "📈 BUY", "callback_data": f"sos_dir:{cid}:buy"},
                                     {"text": "📉 SELL", "callback_data": f"sos_dir:{cid}:sell"}],
                                    [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                ])

                    elif step == "sos_comment":
                        comment_s = "" if txt in ("✅ ارسال بدون یادداشت", "ارسال") else txt
                        sym_s = dw["symbol"]
                        condition_s = dw["condition"]
                        atype_s = "forex" if any(x in sym_s for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                        sender_s = _get_user_custom_name(cid) or uname
                        alarm_num_tag_s = _make_alarm_tag(sym_s)
                        sender_tag_s = "#" + re.sub(r"[^\w]","_", sender_s).strip("_")
                        arrow_s = "📈 ناحیه سل" if condition_s == "above" else "📉 ناحیه بای"
                        dir_lbl_s = dw.get("dir_lbl", "📈 BUY" if condition_s == "below" else "📉 SELL")
                        try: cur_s = get_price(sym_s, atype_s)
                        except: cur_s = None
                        out_s = (
                            f"🚨 <b>آلارم فوری!</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"💰 <b>#{sym_s}</b>  {arrow_s}\n"
                            f"🔖 {alarm_num_tag_s}\n"
                            f"👤 {sender_tag_s}\n"
                            f"📊 قیمت: <b>{fmt_price(cur_s, sym_s) if cur_s else '—'}</b>\n"
                            + (f"💬 {comment_s}\n" if comment_s else "")
                            + f"━━━━━━━━━━━━━━━━━━\n"
                            f"⏰ {now_pretty()} (تهران)"
                        )
                        _, all_cids2, _ = _get_token_and_cids()
                        targets2 = all_cids2 if BROADCAST_MODE else [YOUR_CHAT_ID]
                        del _pending_alarm[cid]
                        # ادیت پیام اصلی به تأییدیه
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id,
                                f"✅ <b>آلارم فوری ارسال شد!</b>\n\n"
                                f"💰 <b>{sym_s}</b>  {dir_lbl_s}\n"
                                + (f"💬 {comment_s}\n" if comment_s else "")
                                + f"⏰ {now_pretty()} (تهران)", [])
                        # broadcast به بقیه (با نظرسنجی)
                        sos_aid_s = str(int(time.time() * 1000))
                        if sym_s.upper() not in TEMP_MUTED_SYMBOLS and not _is_instant_sender_muted(sender_s):
                            _send_instant_rated(token, targets2, out_s, sos_aid_s, alarm_num_tag_s, sender_s)

                        # ذخیره توی آرشیو (این مسیر قبلاً اصلاً ذخیره نمی‌شد)
                        sos_arch_entry_s = {"id": sos_aid_s, "symbol": sym_s, "type": atype_s,
                            "condition": condition_s, "comment": comment_s, "created_by": sender_s,
                            "active": False, "fired_at": now_teh(), "fired_price": cur_s,
                            "instant": True, "created_at": now_teh(), "tag": alarm_num_tag_s}
                        d_arc_s = load_alerts()
                        d_arc_s.setdefault("archive", []).append(sos_arch_entry_s)
                        save_alerts(d_arc_s)
                        threading.Thread(target=_sb_upsert_alert, args=(sos_arch_entry_s,), daemon=True).start()

                    elif step == "broadcast_text":
                        _, all_cids3, _ = _get_token_and_cids()
                        ok3 = sum(1 for tc3 in all_cids3 if send_tg(token, tc3, txt))
                        del _pending_alarm[cid]
                        show_main_menu(token, cid, f"✅ پیام به {ok3} نفر ارسال شد.", is_adm)

                # ── signal pending text steps ─────────────────────────
                elif cid in _pending_signal and not txt.startswith("/"):
                    ps = _pending_signal[cid]
                    ps_step = ps["step"]
                    ps_data = ps["data"]
                    ps_mid  = ps.get("bot_msg_id")

                    if txt in ("❌ انصراف",):
                        del _pending_signal[cid]
                        if ps_mid:
                            edit_tg_keyboard(token, cid, ps_mid, "❌ <b>ساخت سیگنال لغو شد.</b>", [])

                    elif ps_step == "sig_symbol":
                        # کاربر نماد تایپ کرد
                        sym_s = txt.upper().replace("/","").strip()
                        if len(sym_s) < 2:
                            edit_tg_keyboard(token, cid, ps_mid,
                                "📡 <b>سیگنال جدید</b>\n\n❌ نماد نامعتبر. دوباره بنویس:",
                                [[{"text": s, "callback_data": f"sig_sym:{cid}:{s}"} for s in SIGNAL_QUICK_SYMBOLS[:3]],
                                 [{"text": s, "callback_data": f"sig_sym:{cid}:{s}"} for s in SIGNAL_QUICK_SYMBOLS[3:]],
                                 [{"text": "❌ انصراف", "callback_data": f"sig_cancel:{cid}"}]])
                        else:
                            ps_data["symbol"] = sym_s
                            ps["step"] = "sig_direction"
                            dir_kb = [[{"text": lbl, "callback_data": f"sig_dir:{cid}:{val}"} for lbl,val in SIGNAL_DIRECTIONS[:2]],
                                      [{"text": lbl, "callback_data": f"sig_dir:{cid}:{val}"} for lbl,val in SIGNAL_DIRECTIONS[2:]],
                                      [{"text": "❌ انصراف", "callback_data": f"sig_cancel:{cid}"}]]
                            edit_tg_keyboard(token, cid, ps_mid,
                                f"📡 <b>{sym_s}</b>\n\nنوع سفارش:", dir_kb)

                    elif ps_step == "sig_entry_sl":
                        # کاربر Entry + SL رو نوشته: "73370 72550"
                        parts_es = txt.strip().split()
                        if len(parts_es) < 2:
                            edit_tg_keyboard(token, cid, ps_mid,
                                f"📡 <b>{ps_data.get('symbol')}</b>  {ps_data.get('dir_lbl','')}\n\n"
                                "❌ دو عدد بنویس: <code>Entry  SL</code>\nمثال: <code>73370 72550</code>",
                                [[{"text": "❌ انصراف", "callback_data": f"sig_cancel:{cid}"}]])
                            return
                        try:
                            entry_v = float(parts_es[0].replace(",",""))
                            sl_raw  = float(parts_es[1].replace(",",""))
                        except:
                            edit_tg_keyboard(token, cid, ps_mid,
                                f"📡 <b>{ps_data.get('symbol')}</b>\n\n❌ عدد نامعتبر. دوباره بنویس:",
                                [[{"text": "❌ انصراف", "callback_data": f"sig_cancel:{cid}"}]])
                            return
                        # تشخیص پیپ vs قیمت — از inline button قبلاً تعیین شده
                        sl_mode = ps_data.get("sl_mode", "price")
                        sym_v   = ps_data["symbol"]
                        direction_v = ps_data["direction"]
                        if sl_mode == "pip":
                            sl_v = _sl_from_pips(sym_v, direction_v, entry_v, sl_raw)
                        else:
                            sl_v = sl_raw
                        sl_final, tp1, risk_pips = _calc_signal(sym_v, direction_v, entry_v, sl_v, SIGNAL_DEFAULT_RR)
                        ps_data.update({"entry": entry_v, "sl": sl_final, "tp1": tp1,
                                        "tp2": None, "tp3": None, "risk_pips": risk_pips,
                                        "tf": SIGNAL_DEFAULT_TF, "rr": SIGNAL_DEFAULT_RR})
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token, cid, ps_mid, ps_data)

                    elif ps_step == "sig_tp_edit":
                        # کاربر TP2 یا TP3 تایپ کرد
                        which = ps_data.get("_editing_tp", "tp2")
                        try:
                            val = float(txt.replace(",",""))
                            ps_data[which] = val
                        except:
                            pass
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token, cid, ps_mid, ps_data)

                    elif ps_step == "sig_note":
                        ps_data["note"] = txt.strip()
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token, cid, ps_mid, ps_data)

                # ── /sos ─────────────────────────────────────────────
                elif txt.startswith("/sos") and (cid == YOUR_CHAT_ID or (BROADCAST_MODE and _is_approved(cid))):
                    parts = txt.split(maxsplit=3)
                    if len(parts) < 2:
                        send_tg(token, cid,
                            "⚠️ فرمت:\n<code>/sos SYMBOL [buy|sell] [کامنت]</code>\n"
                            "مثال:\n<code>/sos GBPUSD sell</code>")
                    else:
                        sym = parts[1].upper().replace("/", "")
                        raw_dir = parts[2].lower() if len(parts) > 2 else "sell"
                        comment = parts[3] if len(parts) > 3 else ""
                        condition = "above" if raw_dir in ("sell","s","سل","above") else "below"
                        atype = "forex" if any(x in sym for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                        sender_name = _get_sender_name(msg)
                        cur = None
                        try: cur = get_price(sym, atype)
                        except: pass
                        arrow = "📈 ناحیه سل" if condition == "above" else "📉 ناحیه بای"
                        cmt = f"\n💬 <i>{comment}</i>" if comment else ""
                        price_text = fmt_price(cur, sym) if cur else "—"
                        alarm_num_tag = _make_alarm_tag(sym)
                        sender_hashtag = "#" + re.sub(r'[^\w]', '_', sender_name).strip('_')
                        out_msg = (
                            f"🚨 <b>آلارم فوری!</b>\n\n"
                            f"💰 <b>#{sym}</b> — {arrow}\n"
                            f"🔖 {alarm_num_tag}\n"
                            f"👤 {sender_hashtag}\n\n"
                            f"📊 قیمت لحظه‌ای: <b>{price_text}</b>"
                            f"{cmt}\n\n⏰ {now_pretty()} (تهران)"
                        )
                        _, all_cids, _ = _get_token_and_cids()
                        targets = all_cids if BROADCAST_MODE else [YOUR_CHAT_ID]
                        sos_aid_txt = f"sos_{sym}_{int(time.time())}"
                        if sym.upper() not in TEMP_MUTED_SYMBOLS and not _is_instant_sender_muted(sender_name):
                            _send_instant_rated(token, targets, out_msg, sos_aid_txt, alarm_num_tag, sender_name)
                        d = load_alerts()
                        arch = d.get("archive", [])
                        new_sos_entry = {"id": str(int(time.time()*1000)), "symbol": sym, "type": atype,
                            "condition": condition, "comment": comment, "created_by": sender_name,
                            "active": False, "fired_at": now_teh(), "fired_price": cur,
                            "instant": True, "created_at": now_teh(), "tag": alarm_num_tag}
                        arch.append(new_sos_entry)
                        d["archive"] = arch
                        save_alerts(d)
                        threading.Thread(target=_sb_upsert_alert, args=(new_sos_entry,), daemon=True).start()

                # ── /alarm ───────────────────────────────────────────
                elif txt.startswith("/alarm") and (cid == YOUR_CHAT_ID or (BROADCAST_MODE and _is_approved(cid))):
                    parts = txt.split(maxsplit=4)
                    if len(parts) < 4:
                        send_tg(token, cid,
                            "⚠️ فرمت:\n<code>/alarm SYMBOL buy|sell PRICE [کامنت]</code>\n\n"
                            "مثال‌ها:\n"
                            "<code>/alarm eurusd sell 1.12345 ناحیه سل</code>\n"
                            "<code>/alarm xauusd sell 2350 مقاومت مهم</code>")
                    else:
                        sym = parts[1].upper().replace("/", "")
                        raw_dir = parts[2].lower()
                        raw_price = parts[3]
                        comment = parts[4] if len(parts) > 4 else ""
                        condition = "above" if raw_dir in ("sell","s","سل","above") else "below"
                        atype = "forex" if any(x in sym for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                        sender_name = _get_sender_name(msg)
                        tgt_f = None
                        try:
                            tgt_f = float(raw_price)
                        except ValueError:
                            send_tg(token, cid, f"❌ قیمت نامعتبر: <code>{raw_price}</code>")
                        if tgt_f is not None:
                            dir_arrow = "📈 BUY" if condition == "below" else "📉 SELL"
                            new_alert = {
                                "id": str(int(time.time()*1000)),
                                "symbol": sym, "type": atype,
                                "target_price": tgt_f, "condition": condition,
                                "comment": comment, "created_by": sender_name,
                                "active": True, "last_price": None,
                                "last_checked": None,
                                "created_at": now_teh(),
                                "notify_only": YOUR_CHAT_ID if not BROADCAST_MODE else None
                            }
                            d = load_alerts()
                            d["alerts"].append(new_alert)
                            _sb_upsert_alert(new_alert)
                            _cache_alerts = d
                            _is_adm_alarm = (cid == YOUR_CHAT_ID)
                            confirm_alarm_txt = (
                                f"✅ آلارم ثبت شد\n\n"
                                f"<b>{sym}</b>  {dir_arrow}  @  <code>{fmt_price(tgt_f, sym)}</code>"
                                + (f"\n💬 {comment}" if comment else "")
                            )
                            show_main_menu(token, cid, confirm_alarm_txt, _is_adm_alarm)
                            def _bg_price(alert=new_alert, s=sym, t=atype, tok=token, c=cid):
                                try:
                                    cur = get_price(s, t)
                                    if cur:
                                        alert["last_price"] = cur
                                        alert["last_checked"] = now_teh()
                                        _sb_upsert_alert(alert)
                                except: pass
                            threading.Thread(target=_bg_price, daemon=True).start()

                # ── /mealarm — آلارم شخصی (فعلاً فقط ادمین) ───
                elif txt.startswith("/mealarm"):
                    if cid != YOUR_CHAT_ID:
                        send_tg(token, cid, "⚠️ این قابلیت فعلاً در دسترس نیست.")
                    else:
                        parts = txt.split(maxsplit=4)
                        if len(parts) < 4:
                            send_tg(token, cid,
                                "⚠️ فرمت:\n<code>/mealarm SYMBOL buy|sell PRICE [کامنت]</code>\n\n"
                                "مثال:\n"
                                "<code>/mealarm xauusd sell 2350 ناحیه شخصی</code>\n\n"
                                "این آلارم فقط برای شما ثبت میشه و بقیه نمیبینن.")
                        else:
                            sym = parts[1].upper().replace("/", "")
                            raw_dir = parts[2].lower()
                            raw_price = parts[3]
                            comment = parts[4] if len(parts) > 4 else ""
                            condition = "above" if raw_dir in ("sell","s","سل","above") else "below"
                            atype = "forex" if any(x in sym for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                            sender_name = _get_sender_name(msg)
                            tgt_f = None
                            try:
                                tgt_f = float(raw_price)
                            except ValueError:
                                send_tg(token, cid, f"❌ قیمت نامعتبر: <code>{raw_price}</code>")
                            if tgt_f is not None:
                                arrow = "سل 📈" if condition == "above" else "بای 📉"
                                new_alert = {
                                    "id": str(int(time.time()*1000)),
                                    "symbol": sym, "type": atype,
                                    "target_price": tgt_f, "condition": condition,
                                    "comment": comment, "created_by": sender_name,
                                    "active": True, "last_price": None,
                                    "last_checked": None,
                                    "created_at": now_teh(),
                                    "notify_only": cid,
                                    "private_cid": cid,
                                    "is_private": True
                                }
                                d = load_alerts()
                                d["alerts"].append(new_alert)
                                _sb_upsert_alert(new_alert)
                                _cache_alerts = d
                                _is_adm_me = (cid == YOUR_CHAT_ID)
                                _me_dir = "📈 BUY" if condition == "below" else "📉 SELL"
                                _me_confirm = (
                                    f"✅ آلارم شخصی ثبت شد 🔒\n\n"
                                    f"<b>{sym}</b>  {_me_dir}  @  <code>{fmt_price(tgt_f, sym)}</code>"
                                    + (f"\n💬 {comment}" if comment else "")
                                )
                                show_main_menu(token, cid, _me_confirm, _is_adm_me)
                                def _bg_price_me(alert=new_alert, s=sym, t=atype):
                                    try:
                                        cur = get_price(s, t)
                                        if cur:
                                            alert["last_price"] = cur
                                            alert["last_checked"] = now_teh()
                                            _sb_upsert_alert(alert)
                                    except: pass
                                threading.Thread(target=_bg_price_me, daemon=True).start()
                # ── /news ────────────────────────────────────────────
                elif txt.startswith("/myalerts"):
                    btns2 = [[{"text": "🌐 آلارم\u200cهای تیمی", "callback_data": f"myalerts:pub:{cid}"}]]
                    if _has_private_access(cid):
                        btns2.append([{"text": "🔒 آلارم\u200cهای شخصی", "callback_data": f"myalerts:priv:{cid}"}])
                    btns2.append([{"text": "📋 همه آلارم\u200cهای من", "callback_data": f"myalerts:all:{cid}"}])
                    btns2.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                    send_tg_keyboard(token, cid, "⭐ <b>آلارم\u200cهای من</b>\n\nکدوم رو می\u200cخوای ببینی؟", btns2)

                elif txt.startswith("/news") and cid == YOUR_CHAT_ID:
                    send_tg(token, cid, "⏳ در حال دریافت تقویم اقتصادی...")
                    events, err = fetch_ff_news()
                    if err and not events:
                        send_tg(token, cid, err)
                    else:
                        msg = format_ff_message(events) if events else "📭 امروز رویداد مهم فارکس نداریم."
                        send_tg(token, cid, msg)

                # ── /text ────────────────────────────────────────────
                elif txt.startswith("/text") and cid == YOUR_CHAT_ID:
                    body_text = txt[5:].strip()
                    if not body_text:
                        send_tg(token, cid, "\u26a0\ufe0f \u0641\u0631\u0645\u062a:\n<code>/text \u0645\u062a\u0646 \u067e\u06cc\u0627\u0645\u062a \u0627\u06cc\u0646\u062c\u0627</code>")
                    else:
                        _, all_cids, _ = _get_token_and_cids()
                        if not all_cids:
                            send_tg(token, cid, "\u274c \u0647\u06cc\u0686 \u06a9\u0627\u0631\u0628\u0631\u06cc \u062b\u0628\u062a \u0646\u0634\u062f\u0647")
                        else:
                            ok_count = 0
                            for tc in all_cids:
                                r = send_tg(token, tc, body_text)
                                if r: ok_count += 1
                            send_tg(token, cid, f"\u2705 \u067e\u06cc\u0627\u0645 \u0628\u0647 {ok_count} \u0646\u0641\u0631 \u0627\u0631\u0633\u0627\u0644 \u0634\u062f")

  except Exception as e:
    import traceback
    print(f"[do_update] {e}")
    traceback.print_exc()

notified = set()
_deleted_ids: set = set()  # آلارم‌هایی که پاک شدن — دیگه fire نشن
_loop_count = 0
# حلقه‌ی live price هر ۱ دقیقه یک بار اجرا می‌شود.
CHECK_INTERVAL_SECONDS = 60
# ⏸️ موقت: نمادهایی که آلارمشون فایر می‌شه ولی پیام تلگرام نمی‌فرستیم (فقط طلا، فعلاً)
TEMP_MUTED_SYMBOLS = set()
# refresh آلارم‌های فعال از Supabase هر ۵ دور = حدود ۱۰ دقیقه یک بار.
ACTIVE_REFRESH_EVERY_LOOPS = 5

_price_fetch_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="price_fetch")
PRICE_FETCH_TIMEOUT = 15  # ثانیه — اگه یه API بیشتر از این طول کشید، skip میشه

def _fetch_with_timeout(fn, *args, timeout=PRICE_FETCH_TIMEOUT):
    """یه تابع price رو با timeout مستقل اجرا کن"""
    future = _price_fetch_executor.submit(fn, *args)
    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError:
        print(f"[check] ⚠️ price fetch timeout ({timeout}s): {args}")
        future.cancel()
        return None
    except Exception as e:
        print(f"[check] price fetch error: {e}")
        return None

def _load_fired_backup_ids() -> set:
    """لود آیدی‌های fired از فایل بک‌آپ محلی — لایه‌ی دفاعی دوم مستقل از Supabase"""
    if os.path.exists(FIRED_BACKUP_FILE):
        try:
            with open(FIRED_BACKUP_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def _sb_load_active_only():
    """
    فقط ردیف‌های فعال (status=active) + ردیف کانفیگ رو از Supabase بخون —
    بدون آرشیو تاریخی. حلقه‌ی زنده‌ی چک قیمت فقط به آلارم‌های فعال نیاز داره؛
    آرشیو جدا و فقط وقتی کاربر واقعاً صفحه‌ی بایگانی رو تو سایت باز می‌کنه
    خونده می‌شه (endpoint /api/archive از قبل مستقیم و بدون کش این کارو می‌کنه).
    """
    if not SUPABASE_KEY: return None
    try:
        _cols_base = "id,status,active,symbol,type,condition,target_price,created_by,created_at,comment,is_private,private_cid,notify_only,last_price,last_checked,fired_at,fired_price,expires_at,telegram_token,chat_ids,users"
        # supporters (ستاره ناحیه) باید توی select باشه وگرنه با هر refresh کش پاک می‌شه
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?or=(id.eq.__config__,status.eq.active)&select={_cols_base},supporters&limit=500",
            headers=_sb_h(), timeout=10)
        if r.status_code == 400 and "supporters" in r.text:
            # ستون supporters هنوز توی جدول نیست — بدونش لود کن تا حلقه‌ی چک قیمت نخوابه
            print("[alerts] ⚠️ ستون supporters توی جدول alerts نیست — ستاره‌ها بعد از refresh حفظ نمی‌شن")
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/alerts?or=(id.eq.__config__,status.eq.active)&select={_cols_base}&limit=500",
                headers=_sb_h(), timeout=10)
        if r.status_code != 200:
            print(f"[alerts] load active-only failed: {r.status_code} {r.text[:80]}")
            return None
        rows = r.json()
        if not rows:
            return None
        config_row = next((x for x in rows if x["id"] == "__config__"), None)
        tg = {"bot_token": "", "chat_ids": []}
        users = []
        if config_row:
            tg["bot_token"] = config_row.get("telegram_token","") or ""
            raw_cids = config_row.get("chat_ids") or []
            tg["chat_ids"] = raw_cids if isinstance(raw_cids, list) else json.loads(raw_cids)
            raw_users = config_row.get("users") or []
            users = raw_users if isinstance(raw_users, list) else json.loads(raw_users)
        alerts = []
        for row in rows:
            if row["id"] == "__config__": continue
            if row.get("status") != "active" or not row.get("active"):
                continue
            alerts.append({
                "id":           row["id"],
                "symbol":       row.get("symbol",""),
                "type":         row.get("type","forex"),
                "condition":    row.get("condition","above"),
                "target_price": row.get("target_price",0),
                "created_by":   row.get("created_by",""),
                "created_at":   row.get("created_at",""),
                "comment":      row.get("comment",""),
                "is_private":   row.get("is_private", False),
                "private_cid":  row.get("private_cid") or row.get("notify_only"),
                "notify_only":  row.get("notify_only"),
                "active":       row.get("active", True),
                "last_price":   row.get("last_price"),
                "last_checked": row.get("last_checked"),
                "fired_at":     row.get("fired_at"),
                "fired_price":  row.get("fired_price"),
                "expires_at":   row.get("expires_at"),
                "supporters":   row.get("supporters") or [],
            })
        data = {"alerts": alerts, "archive": [], "telegram": tg,
                "users": users, "errors": [], "last_update": now_teh()}
        print(f"[alerts] Loaded ACTIVE-ONLY from Supabase — {len(alerts)} active (بدون آرشیو)")
        return data
    except Exception as e:
        print(f"[alerts] load active-only error: {e}")
        return None


def check_alerts():
    global _loop_count
    while True:
        try:
            _loop_count += 1
            global _cache_alerts
            with _alerts_cache_lock:
                # حلقه‌ی چک قیمت هر ۲ دقیقه اجرا می‌شود. برای کم کردن Egress،
                # خودِ Supabase را فقط هر ۵ دور (حدود ۱۰ دقیقه) برای آلارم‌های فعال refresh می‌کنیم.
                # بین این refreshها از cache حافظه استفاده می‌شود. ثبت/حذف آلارم از
                # endpointهای خودش cache را به‌روز می‌کند، بنابراین نیاز به fetch مداوم نیست.
                if _loop_count % ACTIVE_REFRESH_EVERY_LOOPS == 1:
                    fresh = _sb_load_active_only()
                    if fresh is not None:
                        # 🐛 رفع باگ: _sb_load_active_only آرشیو رو خالی برمی‌گردونه —
                        # اگه مستقیم جایگزین کنیم، هر آلارم فایرشده‌ی اخیر از کش پاک می‌شه.
                        # آرشیوِ قبلی رو نگه می‌داریم و فقط لیست فعال‌ها آپدیت می‌شه.
                        fresh["archive"] = _cache_alerts.get("archive", []) if _cache_alerts else []
                        _cache_alerts = fresh
                    # اگه fresh None بود (خطای موقت شبکه/Supabase)، کش قبلی رو دست
                    # نخورده نگه می‌داریم — نال کردنش باعث می‌شد هر load_alerts()
                    # بعدی (از هر جای برنامه) مجبور به یه fetch کامل و سنگین بشه.
                token, cids, data = _get_token_and_cids()
            _fired_backup_ids = _load_fired_backup_ids()
            all_active_raw = [a for a in data.get("alerts", [])
                      if a.get("active") and str(a["id"]) not in _deleted_ids
                      and not a.get("fired_at")
                      and str(a["id"]) not in _fired_backup_ids]

            # ── چک انقضا — از همون لیستی که همین الان تو حافظه لود شده استفاده می‌کنه،
            # هیچ خوندن اضافه‌ای از Supabase لازم نداره. فقط وقتی آلارمی واقعاً منقضی
            # بشه (نادر)، رکوردش با status='expired' علامت‌گذاری می‌شه — نه حذف کامل،
            # تا همچنان با SQL قابل خروجی گرفتن باشه. ──
            now_naive_teh = datetime.now(TEHRAN).replace(tzinfo=None)
            active = []
            for a in all_active_raw:
                exp_str = a.get("expires_at")
                if exp_str:
                    try:
                        exp_dt = datetime.strptime(exp_str, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        exp_dt = None
                    if exp_dt and now_naive_teh >= exp_dt:
                        _deleted_ids.add(str(a["id"]))
                        print(f"[expiry] ⌛ {a.get('symbol')} ({a['id']}) منقضی شد — status='expired' می‌شه")
                        threading.Thread(target=save_alert_expired, args=(a,), daemon=True).start()
                        continue
                active.append(a)
            if not active:
                time.sleep(CHECK_INTERVAL_SECONDS)
                continue
            forex_open = is_forex_market_open()
            due_forex, due_crypto = [], []
            for a in active:
                sym = a["symbol"]
                atype = a.get("type", "crypto")
                if atype == "forex" and not forex_open:
                    continue
                if atype == "forex":
                    due_forex.append(sym)
                else:
                    due_crypto.append(sym)

            price_map = {}

            # forex: یه batch call با timeout
            if due_forex:
                batch = _fetch_with_timeout(get_forex_prices_batch, due_forex)
                if batch:
                    for sym, p in batch.items():
                        price_map[(sym, "forex")] = p

            # crypto: همزمان، هر کدوم timeout مستقل
            if due_crypto:
                unique_crypto = list(dict.fromkeys(s.upper() for s in due_crypto))
                futures_map = {sym: _price_fetch_executor.submit(get_crypto_price, sym) for sym in unique_crypto}
                for sym, fut in futures_map.items():
                    try:
                        p = fut.result(timeout=PRICE_FETCH_TIMEOUT)
                        price_map[(sym, "crypto")] = p
                    except FuturesTimeoutError:
                        print(f"[check] ⚠️ crypto timeout: {sym}")
                        fut.cancel()
                    except Exception as e:
                        print(f"[check] crypto error {sym}: {e}")

            print(f"[check] loop={_loop_count} forex_open={forex_open} due_f={len(due_forex)} due_c={len(due_crypto)} prices={len(price_map)}")
            fired = []
            now = now_teh()
            for a in active:
                sym = a["symbol"]
                atype = a.get("type", "crypto")
                key = (sym.upper(), atype)
                if key not in price_map: continue
                cur = price_map[key]
                if cur is None: continue
                tgt = float(a["target_price"])
                cond = a.get("condition", "above")
                # ✅ آپدیت قیمت لحظه‌ای برای همه آلارم‌ها
                a["last_price"] = cur
                a["last_checked"] = now
                stale_ts = price_map.get((sym.upper(), atype, "stale"))
                a["price_stale"] = stale_ts if stale_ts else None
                data["last_update"] = now
                triggered = (cond == "above" and cur >= tgt) or (cond == "below" and cur <= tgt)
                print(f"[check] {sym} cur={fmt_price(cur,sym)} tgt={fmt_price(tgt,sym)} cond={cond} → {'🔥 FIRE' if triggered else 'ok'}")
                if triggered and a["id"] not in notified and str(a["id"]) not in _deleted_ids:
                    notified.add(a["id"])
                    _deleted_ids.add(str(a["id"]))  # ← فوری blacklist — جلوگیری از double-fire
                    a["active"] = False
                    a["fired_at"] = now
                    a["fired_price"] = cur
                    alarm_num_tag = _make_alarm_tag(sym)
                    a["tag"] = alarm_num_tag
                    fired.append(a["id"])
                    # 🔒 فوری و سینک قبل از هرکاری (ارسال پیام/تعیین مسئول) تو Supabase ذخیره می‌کنیم —
                    # تا اگه سرور وسط ارسال پیام‌ها کرش/ریستارت کرد، این آلارم already-fired بمونه و دوباره فایر نشه
                    save_alert_fired(a)
                    # ⏸️ موقت: آلارم طلا (XAUUSD) فایر می‌شه و ثبت می‌شه، فقط پیام تلگرام ارسال نشه
                    if sym.upper() in TEMP_MUTED_SYMBOLS:
                        print(f"[FILTER] {sym} در لیست موقت بی‌صدا — پیام تلگرام ارسال نشد")
                    elif token and cids:
                        comment = a.get("comment", "")
                        if str(YOUR_CHAT_ID) in str(comment):
                            notify_cids = [str(YOUR_CHAT_ID)]
                            print(f"[FILTER] comment contains {YOUR_CHAT_ID} → only to you")
                        elif a.get("notify_only"):
                            notify_cids = [str(a["notify_only"])]
                            print(f"[FILTER] notify_only → {a['notify_only']}")
                        else:
                            notify_cids = cids
                            print(f"[FILTER] broadcast → {len(cids)} users")
                        arrow = "📈 ناحیه سل" if cond == "above" else "📉 ناحیه بای"
                        creator = a.get("created_by") or "سیستم"
                        cmt = f"\n💬 <i>{comment}</i>" if comment else ""
                        dist = calc_dist_str(sym, atype, cur, tgt)
                        private_label = "\n\n🔒 <i>آلارم شخصی — فقط برای شما ارسال شده</i>" if a.get("is_private") else ""
                        creator_tag = "#" + re.sub(r'[^\w]', '_', creator).strip('_')
                        # ── تعیین مسئول تریگر — فقط برای آلارم‌های تیمی، آلارم شخصی وارد تقسیم نمی‌شه ──
                        # (غیرفعال — تقسیم آلارم کنسل شده؛ هیچ مسئولی تعیین نمی‌شه و دکمه «دیدم» هم ساخته نمی‌شه)
                        # if a.get("is_private"):
                        #     _assignee, _shift = "", None
                        #     assignee_line = ""
                        # else:
                        #     _assignee, _shift = _get_assignee_for_alarm(
                        #         a["id"], alarm_num_tag, now,
                        #         symbol=sym, target_price=float(tgt), created_by=creator
                        #     )
                        #     assignee_line = f"\n\n🎯 مسئول تریگر: <b>{_assignee}</b>" if _assignee else ""
                        _assignee, _shift = "", None
                        assignee_line = ""
                        created_at_raw = a.get("created_at", "")
                        created_label = f" | 📅 ثبت: <i>{created_at_raw[:16]}</i>" if created_at_raw else ""
                        _sups = a.get("supporters") or []
                        _sup_names = []
                        for _su in _sups:
                            if isinstance(_su, dict) and _su.get("name"):
                                _sup_names.append(str(_su["name"]))
                            elif isinstance(_su, str) and _su.strip():
                                _sup_names.append(_su.strip())
                        supporters_line = ""
                        if _sup_names:
                            supporters_line = "\n⭐ تأیید ناحیه توسط: <b>" + "، ".join(_sup_names) + "</b>"
                        fired_msg = (
                            f"🚨 <b>آلارم قیمت!</b>\n\n"
                            f"💰 <b>#{sym}</b> — {arrow}\n"
                            f"🔖 {alarm_num_tag}\n"
                            f"👤 {creator_tag}\n\n"
                            f"🎯 هدف: <code>{fmt_price(tgt,sym)}</code>\n"
                            f"📊 قیمت لحظه‌ای: <b>{fmt_price(cur,sym)}</b>\n"
                            f"📏 فاصله: <b>{dist}</b>"
                            f"{cmt}"
                            f"{private_label}"
                            f"{supporters_line}"
                            f"{assignee_line}\n\n⏰ {now_pretty()} (تهران){created_label}"
                        )
                        assignee_ack_id = TEAM_MEMBER_IDS.get(_assignee, "") if _assignee else ""
                        _is_team_alert = not a.get("is_private")
                        _rate_state0 = _compute_rating_state([], _resolve_creator_name(creator)) if _is_team_alert else None
                        def _fired_kb(cid):
                            rows_kb = []
                            if assignee_ack_id and str(cid) == str(assignee_ack_id):
                                rows_kb.append([{"text": "دیدم", "callback_data": f"ack_trigger:{a['id']}:{assignee_ack_id}"}])
                            if _is_team_alert and _rate_state0 is not None:
                                rows_kb.extend(_rating_keyboard(a["id"], _rate_state0))
                            return rows_kb
                        fired_msg_send = fired_msg
                        if _is_team_alert and _rate_state0 is not None:
                            fired_msg_send = fired_msg + _build_rating_section(_rate_state0)
                        fired_cid_to_mid = {}
                        if a.get("is_private") and a.get("notify_only"):
                            priv_cid = str(a["notify_only"])
                            mid_f = send_tg_keyboard(token, priv_cid, fired_msg_send, _fired_kb(priv_cid), track=False)
                            if mid_f:
                                fired_cid_to_mid[priv_cid] = mid_f
                        else:
                            for cid in notify_cids:
                                mid_f = send_tg_keyboard(token, str(cid), fired_msg_send, _fired_kb(str(cid)), track=False)
                                if mid_f:
                                    fired_cid_to_mid[str(cid)] = mid_f
                        if fired_cid_to_mid:
                            fired_cid_to_mid["__tag__"] = alarm_num_tag
                            fired_cid_to_mid["__text__"] = fired_msg
                            fired_cid_to_mid["__creator__"] = _resolve_creator_name(creator)
                            if _is_team_alert:
                                fired_cid_to_mid["__rating_deadline__"] = str(int(time.time() + _rate_auto_close_seconds(sym, False)))
                            _fired_msg_ids[a["id"]] = fired_cid_to_mid
                            print(f"[fire] saved msg map for {a['id']}: chats={list(fired_cid_to_mid.keys())}")
                            # همگام — تا قبل از اولین رای، map تو Supabase باشه (multi-instance)
                            try:
                                _sb_save_fired_msgs(a["id"], fired_cid_to_mid)
                            except Exception as e:
                                print(f"[fire] fired_msgs save exc: {e}")
                                threading.Thread(target=_sb_save_fired_msgs, args=(a["id"], fired_cid_to_mid), daemon=True).start()
            if fired:
                # save_alert_fired() برای هر آلارم Fired شده، وضعیت نهایی را مستقیم
                # در Supabase ذخیره و cache را هم به‌روز کرده است. بنابراین اینجا
                # نباید save_alerts(data) را اجرا کنیم؛ چون آن تابع همه‌ی آلارم‌های
                # فعال باقی‌مانده را دوباره Upsert می‌کرد و Egress/Write بی‌دلیل می‌ساخت.
                with _alerts_cache_lock:
                    if _cache_alerts is not None:
                        # cache را از data هم همگام نگه می‌داریم، بدون هیچ درخواست شبکه‌ای
                        data = _cache_alerts
            # مهم: وقتی هیچ آلارمی fire نشده، هیچ Write به Supabase انجام نمی‌شود.
            # last_price و last_checked فقط در RAM تغییر می‌کنند و برای مقایسه‌ی
            # قیمت فعلی با target استفاده می‌شوند.
        except Exception as e:
            log_error(f"check_alerts: {e}")
        time.sleep(CHECK_INTERVAL_SECONDS)

def fmt_price(p, sym=""):
    if p is None: return "—"
    v = float(p)
    su = sym.upper()
    if "XAU" in su or "XAG" in su:
        return f"${v:.2f}"
    if "JPY" in su:
        return f"{v:.3f}"
    return f"{v:.5f}"

def calc_dist_str(symbol, atype, cur, tgt):
    if not cur or not tgt: return ""
    diff = abs(float(cur) - float(tgt))
    sym_up = symbol.upper()
    if atype == "crypto": return f"{diff/float(tgt)*100:.2f}%"
    if "XAU" in sym_up or "XAG" in sym_up: return f"{diff:.2f} $"
    if "JPY" in sym_up: return f"{round(diff*100):,} pip"
    return f"{round(diff*10000):,} pip"

# ==================== Routes ====================
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/config", methods=["GET","POST"])
def config():
    data = load_alerts()
    if request.method == "POST":
        body = request.json or {}
        tg = data.get("telegram", {})
        if body.get("bot_token"):
            tg["bot_token"] = body["bot_token"]
        if body.get("chat_id"):
            cid = str(body["chat_id"])
            ids = [str(x) for x in tg.get("chat_ids", [])]
            if cid not in ids: ids.append(cid)
            tg["chat_ids"] = ids
            tg["chat_id"] = cid
        data["telegram"] = tg
        save_alerts(data)
        return jsonify({"ok": True})
    tg = data.get("telegram", {})
    return jsonify({
        "bot_token": tg.get("bot_token",""), "chat_id": tg.get("chat_id",""),
        "chat_ids": tg.get("chat_ids",[]), "user_count": len(data.get("users",[]))
    })

@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    all_alerts = load_alerts().get("alerts", [])
    # آلارم‌های شخصی (is_private=True) رو از لیست عمومی حذف کن
    public = [a for a in all_alerts if not a.get("is_private")]
    return jsonify(public)

@app.route("/api/alerts/my", methods=["GET"])
def get_my_alerts():
    """آلارم‌های شخصیِ یه کاربر (چه عادی چه شخصی/is_private) — با name فیلتر میشه.
    اگه این اسم صاحب کد فعال‌سازی (web_pin) باشه، حتماً باید pin هم درست بیاد،
    وگرنه (کاربرهایی که هنوز کد نگرفتن) طبق روال قبل فقط با تطابق اسم برمی‌گرده."""
    name = request.args.get("name", "").strip()
    cid  = request.args.get("cid",  "").strip()
    pin  = request.args.get("pin",  "").strip()
    if not name and not cid:
        return jsonify([])
    data = load_alerts()
    all_alerts = data.get("alerts", [])
    name_lc = name.lower()
    resolved_cid = cid
    matched_user = None
    if name and not resolved_cid:
        for usr in data.get("users", []):
            if usr.get("custom_name", "").strip().lower() == name_lc:
                matched_user = usr
                resolved_cid = str(usr.get("chat_id", ""))
                break
    if not matched_user and resolved_cid:
        for usr in data.get("users", []):
            if str(usr.get("chat_id", "")) == str(resolved_cid):
                matched_user = usr
                break
    if matched_user and matched_user.get("web_pin"):
        if str(pin) != str(matched_user.get("web_pin")):
            return jsonify([])
    if matched_user and matched_user.get("full_access"):
        # این کاربر از پنل ادمین دسترسی کامل گرفته — کل آلارم‌ها (عادی + شخصی) رو ببینه
        return jsonify(all_alerts)
    my = [
        a for a in all_alerts
        if (
            a.get("is_private") and (
                (resolved_cid and str(a.get("private_cid", "")) == resolved_cid) or
                (name and a.get("created_by", "").strip().lower() == name_lc)
            )
        ) or (
            not a.get("is_private") and name and a.get("created_by", "").strip().lower() == name_lc
        )
    ]
    return jsonify(my)

@app.route("/api/alerts", methods=["POST"])
def add_alert():
    body = request.json or {}
    sym = body.get("symbol","").upper().strip()
    atype = body.get("type","forex")
    tgt = float(body.get("target_price", 0))
    condition = body.get("condition","above")
    creator = body.get("creator", "").strip()
    pin = body.get("pin", "").strip()
    if not _is_name_approved(creator, pin):
        return jsonify({"ok": False, "error": "این نام هنوز از طرف ادمین تایید نشده. لطفاً ابتدا از طریق ربات تلگرام (/start) درخواست فعال‌سازی بدید."}), 403

    # موافقت با ناحیه موجود (ستاره) — فقط وب
    support_id = str(body.get("support_alert_id") or "").strip()
    if support_id:
        updated = _add_supporter_to_alert(support_id, creator)
        if not updated:
            return jsonify({"ok": False, "error": "آلارم مورد نظر پیدا نشد یا غیرفعال/شخصی است."}), 404
        return jsonify({
            "ok": True,
            "supported": True,
            "alert": updated,
            "message": f"⭐ تأیید شما روی ناحیه {updated.get('created_by')} ثبت شد."
        })

    # آلارم شخصی فقط برای کسایی که از پنل ادمین/تلگرام دسترسیش رو گرفتن — وگرنه سایلنت عادی ثبت می‌شه
    is_private = False
    private_cid = None
    if bool(body.get("is_private", False)):
        u_priv = _verified_user_by_name(creator, pin)
        if u_priv and (u_priv.get("private_access") or str(u_priv.get("chat_id", "")) == str(YOUR_CHAT_ID)):
            is_private = True
            private_cid = str(u_priv.get("chat_id", ""))

    force_new = bool(body.get("force_new", False))
    # چک نزدیکی — فقط تیمی، فقط وقتی force_new نیست
    if not is_private and not force_new and tgt > 0:
        nearby = _find_nearby_team_alerts(sym, tgt, condition, exclude_name=creator)
        if nearby:
            thr = nearby[0].get("threshold")
            return jsonify({
                "ok": False,
                "needs_confirm": True,
                "threshold_pips": thr,
                "nearby": nearby,
                "message": "نزدیک این قیمت، آلارم تیمی دیگری هست. جدید ثبت کنید یا همان ناحیه را تأیید (ستاره) کنید."
            }), 409

    cur = get_price(sym, atype) if (atype!="forex" or is_forex_market_open()) else None
    expire_days = body.get("expire_days")
    expires_at = None
    try:
        expire_days = int(expire_days) if expire_days not in (None, "", 0, "0") else 0
    except (TypeError, ValueError):
        expire_days = 0
    if expire_days > 0:
        expires_at = (datetime.now(TEHRAN) + timedelta(days=expire_days)).strftime("%Y-%m-%d %H:%M:%S")
    a = {
        "id": str(int(time.time() * 1000)), "symbol": sym, "type": atype,
        "target_price": tgt, "condition": condition,
        "comment": body.get("comment","").strip(), "active": True,
        "created_by": creator or "ناشناس",
        "is_private": is_private,
        "private_cid": private_cid,
        "notify_only": private_cid,
        "expires_at": expires_at,
        "supporters": [],
        "last_price": cur, "last_checked": now_teh() if cur else None,
        "created_at": now_teh()
    }
    # قفل: خوندن، اضافه‌کردن، و ذخیره باید یه عملیات atomic باشه —
    # وگرنه اگه check_alerts هم‌زمان کش رو رفرش کنه، یکی از تغییرات گم می‌شه
    with _alerts_cache_lock:
        data = load_alerts()
        data["alerts"].append(a)
        save_alerts(data)
    return jsonify({"ok": True, "alert": a})

@app.route("/api/alarm-assignments/<aid>/false", methods=["POST"])
def web_false_assignment(aid):
    """فالس کردن یه آلارم مشخص از صفحه‌ی گزارش هفتگی وب (با کامنت/علت) — نیاز به اسم+کد تاییدشده داره.
    فقط برای آلارم‌های تیمی (غیرشخصی) کار می‌کنه، دقیقاً مثل چیزی که تو گزارش هفتگی نمایش داده می‌شه."""
    body = request.json or {}
    name = (body.get("name") or "").strip()
    pin = (body.get("pin") or "").strip()
    reason = (body.get("reason") or "").strip()
    u = _verified_user_by_name(name, pin)
    if not u:
        return jsonify({"ok": False, "error": "هویت تایید نشد. لطفاً اسم و کد فعال‌سازی درست رو وارد کنید."}), 403

    alert_wfa = None
    if SUPABASE_KEY:
        try:
            r_wfa = requests.get(
                f"{SUPABASE_URL}/rest/v1/alerts?id=eq.{aid}&select=id,is_private",
                headers=_sb_h(), timeout=8)
            if r_wfa.status_code == 200:
                found_wfa = r_wfa.json()
                alert_wfa = found_wfa[0] if found_wfa else None
        except Exception as e:
            print(f"[web_false_assignment] targeted fetch error: {e}")
    if alert_wfa and alert_wfa.get("is_private"):
        return jsonify({"ok": False, "error": "این آلارم شخصیه و از این صفحه قابل فالس‌کردن نیست."}), 403

    sender_name = u.get("custom_name") or name
    with _false_in_progress_lock:
        if aid in _false_in_progress:
            return jsonify({"ok": False, "error": "این آلارم همین الان داره پردازش می‌شه، چند ثانیه صبر کنید."}), 409
        _false_in_progress.add(aid)
    try:
        _sb_false_assignment(aid, sender_name, reason)
        _rebuild_active_assign_count(_sb_load_active_assignments())
    finally:
        with _false_in_progress_lock:
            _false_in_progress.discard(aid)
    return jsonify({"ok": True})


@app.route("/api/alerts/<aid>", methods=["DELETE"])
def del_alert(aid):
    data = load_alerts()
    data["alerts"] = [a for a in data.get("alerts", []) if a["id"] != aid]
    global _cache_alerts
    _cache_alerts = data
    threading.Thread(target=_sb_delete_alert, args=(aid,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/users", methods=["GET"])
def get_users():
    return jsonify(load_alerts().get("users", []))

@app.route("/api/users/<cid>", methods=["DELETE"])
def del_user(cid):
    data = load_alerts()
    data["users"] = [u for u in data.get("users",[]) if str(u["chat_id"]) != str(cid)]
    data["telegram"]["chat_ids"] = [x for x in data["telegram"].get("chat_ids",[]) if str(x) != str(cid)]
    save_alerts(data)
    return jsonify({"ok": True})

@app.route("/api/price/<atype>/<symbol>")
def live_price(atype, symbol):
    sym = symbol.upper().replace("-","/")
    p = get_price(sym, atype)
    if p is None:
        return jsonify({"error": "قیمت پیدا نشد"}), 404
    return jsonify({"symbol": sym, "price": p})

@app.route("/api/instant-alert", methods=["POST"])
def instant_alert():
    body = request.json or {}
    sym = body.get("symbol", "").upper().strip()
    if not sym:
        return jsonify({"ok": False, "error": "نماد وارد نشده"}), 400
    atype = body.get("type", "forex")
    condition = body.get("condition", "above")
    comment = body.get("comment", "").strip()
    creator = body.get("creator", "").strip()
    target_price = body.get("target_price")
    only_me = body.get("only_me", False)
    pin = body.get("pin", "").strip()

    if not _is_name_approved(creator, pin):
        return jsonify({"ok": False, "error": "این نام هنوز از طرف ادمین تایید نشده. لطفاً ابتدا از طریق ربات تلگرام (/start) درخواست فعال‌سازی بدید."}), 403

    token, all_cids, data = _get_token_and_cids()
    if not token:
        return jsonify({"ok": False, "error": "توکن تلگرام تنظیم نشده"}), 400

    targets = [YOUR_CHAT_ID] if only_me else (all_cids if BROADCAST_MODE else [YOUR_CHAT_ID])
    if not targets:
        return jsonify({"ok": False, "error": "هیچ chat_id‌ای ثبت نشده"}), 400

    # قیمت لحظه‌ای
    cur = None
    try:
        cur = get_price(sym, atype)
    except:
        pass

    arrow = "📈 ناحیه سل" if condition == "above" else "📉 ناحیه بای"
    cmt = f"\n💬 <i>{comment}</i>" if comment else ""
    price_text = fmt_price(cur, sym) if cur else "—"
    _creator = creator or 'سیستم'
    alarm_num_tag = _make_alarm_tag(sym)
    creator_tag = "#" + re.sub(r'[^\w]', '_', _creator).strip('_')
    out_msg = (
        f"🚨 <b>{'آلارم قیمت' if target_price else 'آلارم فوری'}!</b>\n\n"
        f"💰 <b>#{sym}</b> — {arrow}\n"
        f"🔖 {alarm_num_tag}\n"
        f"👤 {creator_tag}\n\n"
        + (f"🎯 هدف: <code>{fmt_price(target_price, sym)}</code>\n" if target_price else "")
        + f"📊 قیمت لحظه‌ای: <b>{price_text}</b>"
        f"{cmt}\n\n⏰ {now_pretty()} (تهران)"
    )

    # هر کاربر جداگانه با دکمه هشدار دوره‌ای
    sent_count = 0
    if sym.upper() not in TEMP_MUTED_SYMBOLS and not _is_instant_sender_muted(_creator):
        for cid in targets:
            mid = send_tg_keyboard(token, str(cid), out_msg, [])
            if mid: sent_count += 1

    # ذخیره در آرشیو
    try:
        d = load_alerts()
        d.setdefault("archive", []).append({
            "id": str(int(time.time() * 1000)),
            "symbol": sym, "type": atype,
            "condition": condition, "comment": comment,
            "created_by": creator, "active": False,
            "fired_at": now_teh(), "fired_price": cur,
            "target_price": target_price,
            "instant": True, "created_at": now_teh()
        })
        save_alerts(d)
    except Exception as e:
        log_error(f"instant_alert archive: {e}")

    print(f"[INSTANT] {sym} ارسال شد به {sent_count}/{len(targets)} نفر")
    return jsonify({"ok": True, "sent": sent_count, "total": len(targets)})


def test_tg():
    token, cids, _ = _get_token_and_cids()
    if not token or not cids:
        return jsonify({"ok": False, "error": "توکن یا chat_id ست نشده"})
    res = broadcast(token, cids, f"✅ تست موفق\n⏰ {now_pretty()}")
    return jsonify({"ok": any(res), "sent": sum(res), "total": len(cids)})

# ===================== SIGNALS API (وب‌سایت) =====================
SIGNAL_VALID_DIRECTIONS = {"buy_limit", "buy_stop", "sell_limit", "sell_stop"}

def _sb_delete_signal(sig_id):
    """حذف سیگنال از Supabase"""
    if not SUPABASE_KEY: return
    try:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/signals?id=eq.{sig_id}",
            headers=_sb_h(), timeout=8)
    except: pass

def _build_signal_record(body: dict):
    """از روی دیتای فرم سایت، رکوردی عیناً هم‌ساختار با چیزی که ربات می‌سازه برمی‌گردونه"""
    sym = (body.get("symbol") or "").upper().strip()
    direction = (body.get("direction") or "").lower().strip()
    if not sym:
        return None, "نماد وارد نشده"
    if direction not in SIGNAL_VALID_DIRECTIONS:
        return None, "جهت سیگنال نامعتبره (buy_limit/buy_stop/sell_limit/sell_stop)"
    try:
        entry = float(body.get("entry"))
    except (TypeError, ValueError):
        return None, "قیمت ورود الزامی است"

    def _f(key):
        v = body.get(key)
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    sl  = _f("sl")
    tp1 = _f("tp1")
    tp2 = _f("tp2")
    tp3 = _f("tp3")
    tf  = (body.get("tf") or SIGNAL_DEFAULT_TF).strip()
    note = (body.get("note") or "").strip()
    creator = (body.get("creator") or "وب‌سایت").strip()

    risk_pips = round(abs(entry - sl) * get_pip_multiplier(sym), 1) if sl is not None else None
    rr = None
    if sl is not None and tp1 is not None and abs(entry - sl) > 0:
        rr = round(abs(tp1 - entry) / abs(entry - sl), 2)

    seq = _sb_next_signal_seq()
    sig = {
        "id": f"S{seq:05d}", "seq": seq, "symbol": sym, "direction": direction,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "tf": tf, "risk_pips": risk_pips, "rr": rr,
        "sent_by": creator, "sent_at": now_teh(), "channel_msg_id": None,
        "status": "active", "note": note or None,
    }
    return sig, None

@app.route("/api/signals", methods=["GET"])
def get_signals():
    limit = request.args.get("limit", 50)
    try: limit = int(limit)
    except: limit = 50
    return jsonify(_sb_load_signals(limit=limit))

@app.route("/api/signals", methods=["POST"])
def add_signal():
    """ثبت سیگنال در دیتابیس — بدون ارسال به کانال تلگرام"""
    body = request.json or {}
    sig, err = _build_signal_record(body)
    if err:
        return jsonify({"ok": False, "error": err}), 400
    _sb_save_signal(sig)
    sig["text"] = _build_signal_text(sig)
    return jsonify({"ok": True, "signal": sig})

@app.route("/api/signals/<sig_id>", methods=["DELETE"])
def del_signal(sig_id):
    threading.Thread(target=_sb_delete_signal, args=(sig_id,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/send-signal", methods=["POST"])
def send_signal():
    """ثبت سیگنال + ارسال عیناً به کانال تلگرام، با همون فرمتی که ربات می‌سازه"""
    body = request.json or {}
    sig, err = _build_signal_record(body)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    token, _, _ = _get_token_and_cids()
    channel_mid = None
    if token and SIGNAL_CHANNEL:
        try:
            r_ch = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": SIGNAL_CHANNEL, "text": _build_signal_text(sig), "parse_mode": "HTML"},
                timeout=10, headers=H)
            if r_ch.status_code == 200:
                channel_mid = r_ch.json().get("result", {}).get("message_id")
                sig["channel_msg_id"] = channel_mid
        except Exception as e:
            print(f"[signal] web channel send error: {e}")

    _sb_save_signal(sig)
    sig["text"] = _build_signal_text(sig)
    if not SIGNAL_CHANNEL:
        return jsonify({"ok": True, "signal": sig, "sent": 0,
                         "warning": "کانال سیگنال (SIGNAL_CHANNEL) تنظیم نشده — فقط در دیتابیس ذخیره شد"})
    return jsonify({"ok": True, "signal": sig, "sent": 1 if channel_mid else 0})
# ===================================================================

@app.route("/api/status")
def status():
    alerts = load_alerts()
    all_active = [a for a in alerts.get("alerts", []) if a.get("active")]
    team_active = [a for a in all_active if not a.get("is_private")]
    private_active = [a for a in all_active if a.get("is_private")]
    return jsonify({
        "status": "ok", "last_update": alerts.get("last_update"),
        "errors": alerts.get("errors", [])[-5:], "time_tehran": now_teh(),
        "alert_count": len(all_active),          # کل (تیمی + شخصی)
        "team_alert_count": len(team_active),     # فقط تیمی
        "private_alert_count": len(private_active), # فقط شخصی
        "forex_open": is_forex_market_open(),
        "loop_count": _loop_count
    })

@app.route("/api/version")
def version():
    return jsonify({"version": VERSION})

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

@app.route("/api/assignments", methods=["GET"])
def api_assignments():
    """
    لیست کامل assignments با join از alerts جدول.
    پارامترها:
      ?active=true    — فقط فعال‌ها
      ?week=true      — از ابتدای هفته جاری (شنبه تهران)
      ?all=true       — همه (پیش‌فرض)
    """
    try:
        active_only = request.args.get("active") == "true"
        week_only   = request.args.get("week") == "true"

        # لود assignments از Supabase
        url = f"{SUPABASE_URL}/rest/v1/alarm_assignments?select=*&order=fired_at.desc"
        if active_only:
            url += "&is_active=eq.true"
        if week_only:
            now_dt = datetime.now(TEHRAN)
            days_since_sat = (now_dt.weekday() - 5) % 7
            week_start = (now_dt - timedelta(days=days_since_sat)).replace(
                hour=0, minute=0, second=0, microsecond=0)
            url += f"&fired_at=gte.{week_start.strftime('%Y-%m-%dT%H:%M:%S')}"

        r = requests.get(url, headers=_sb_h(), timeout=10)
        if r.status_code != 200:
            return jsonify({"ok": False, "error": r.text[:100]}), 500
        rows = r.json()

        # لود فقط alertهایی که تو این assignmentها هستن (نه کل جدول)
        ids_aa = sorted({str(rw.get("id","")) for rw in rows if rw.get("id")})
        alerts_map = {}
        if SUPABASE_KEY and ids_aa:
            try:
                r_aa = requests.get(
                    f"{SUPABASE_URL}/rest/v1/alerts?id=in.({','.join(ids_aa)})&select=*",
                    headers=_sb_h(), timeout=10)
                if r_aa.status_code == 200:
                    for row in r_aa.json():
                        alerts_map[str(row.get("id",""))] = row
            except Exception as e:
                print(f"[api_assignments] targeted fetch error: {e}")

        result = []
        for row in rows:
            aid    = str(row.get("id", ""))
            alert  = alerts_map.get(aid, {})
            sym    = alert.get("symbol", "")
            tgt    = alert.get("target_price", 0) or 0
            result.append({
                # از alarm_assignments
                "id":           aid,
                "alarm_tag":    row.get("alarm_tag", ""),
                "assigned_to":  row.get("assigned_to", "") or "",
                "shift":        row.get("shift", ""),
                "is_active":    row.get("is_active", True),
                "fired_at":     row.get("fired_at", ""),
                "false_at":     row.get("false_at", "") or "",
                "false_by":     row.get("false_by", "") or "",
                "false_reason": row.get("false_reason", "") or "",
                # از alerts جدول
                "symbol":       sym,
                "condition":    alert.get("condition", ""),
                "target_price": tgt,
                "target_fmt":   fmt_price(float(tgt), sym) if tgt else "",
                "created_by":   alert.get("created_by", "") or "",
                "created_at":   alert.get("created_at", "") or "",
                "comment":      alert.get("comment", "") or "",
                "fired_price":  alert.get("fired_price", "") or "",
                "is_private":   alert.get("is_private", False),
            })

        # فیلتر آلارم‌های شخصی
        result = [r for r in result if not r["is_private"]]
        return jsonify({"ok": True, "count": len(result), "items": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# =====================================================================
# 🌐 پنل ادمین وب — API endpoints
# =====================================================================

@app.route("/admin-panel")
def admin_panel_page():
    """صفحه‌ی HTML پنل ادمین وب — لاگین + داشبورد"""
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "admin_panel.html")


@app.route("/api/admin-panel/login", methods=["POST"])
def admin_panel_login():
    body = request.json or {}
    pw = body.get("password", "")
    if not ADMIN_PANEL_PASSWORD:
        return jsonify({"ok": False, "error": "رمز پنل تنظیم نشده — باید ADMIN_PANEL_PASSWORD رو تو Railway بذاری"}), 500
    if not secrets.compare_digest(pw, ADMIN_PANEL_PASSWORD):
        return jsonify({"ok": False, "error": "رمز اشتباهه"}), 401
    token = _create_admin_session()
    return jsonify({"ok": True, "token": token})


@app.route("/api/admin-panel/assignments", methods=["GET"])
def admin_panel_assignments():
    """لیست آلارم‌های فعال با مسئول‌هاشون — برای بخش «مسئولین آلارم» پنل وب"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    rows = _sb_load_active_assignments()
    ids_apa = sorted({str(rw.get("id","")) for rw in rows if rw.get("id")})
    alerts_map = {}
    if SUPABASE_KEY and ids_apa:
        try:
            r_apa = requests.get(
                f"{SUPABASE_URL}/rest/v1/alerts?id=in.({','.join(ids_apa)})&select=*",
                headers=_sb_h(), timeout=10)
            if r_apa.status_code == 200:
                for row in r_apa.json():
                    alerts_map[str(row.get("id",""))] = row
        except Exception as e:
            print(f"[admin_panel_assignments] targeted fetch error: {e}")

    items = []
    for row in rows:
        aid = str(row.get("id", ""))
        alert = alerts_map.get(aid, {})
        if alert.get("is_private"):
            continue
        sym = alert.get("symbol", "")
        tgt = alert.get("target_price", 0) or 0
        items.append({
            "id": aid,
            "alarm_tag": row.get("alarm_tag", ""),
            "assigned_to": row.get("assigned_to", "") or "",
            "symbol": sym,
            "condition": alert.get("condition", ""),
            "target_fmt": fmt_price(float(tgt), sym) if tgt else "",
            "fired_at": row.get("fired_at", ""),
        })
    return jsonify({"ok": True, "items": items, "team_members": TEAM_MEMBERS})


@app.route("/api/admin-panel/users", methods=["GET"])
def admin_panel_users():
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    data = load_alerts()
    users = data.get("users", [])
    return jsonify({"ok": True, "users": [
        {"chat_id": str(u.get("chat_id", "")),
         "name": u.get("custom_name", "") or u.get("username", "") or str(u.get("chat_id", "")),
         "private_access": bool(u.get("private_access")),
         "full_access": bool(u.get("full_access"))}
        for u in users
    ]})


@app.route("/api/admin-panel/users/<chat_id>/full-access", methods=["POST"])
def admin_panel_set_full_access(chat_id):
    """فعال/غیرفعال کردن دیدن کل آلارم‌ها (نه فقط آلارم‌های خودش) برای یه کاربر — از پنل وب"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    body = request.json or {}
    value = bool(body.get("value", False))
    with _alerts_cache_lock:
        data = load_alerts()
        found = False
        for u in data.get("users", []):
            if str(u.get("chat_id", "")) == str(chat_id):
                u["full_access"] = value
                found = True
                break
        if not found:
            return jsonify({"ok": False, "error": "کاربر پیدا نشد"}), 404
        save_alerts(data)
    return jsonify({"ok": True, "full_access": value})


@app.route("/api/admin-panel/users/<chat_id>/private-access", methods=["POST"])
def admin_panel_set_private_access(chat_id):
    """فعال/غیرفعال کردن دسترسی «آلارم شخصی» برای یه کاربر — از پنل وب"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    body = request.json or {}
    value = bool(body.get("value", False))
    with _alerts_cache_lock:
        data = load_alerts()
        found = False
        for u in data.get("users", []):
            if str(u.get("chat_id", "")) == str(chat_id):
                u["private_access"] = value
                found = True
                break
        if not found:
            return jsonify({"ok": False, "error": "کاربر پیدا نشد"}), 404
        save_alerts(data)
    return jsonify({"ok": True, "private_access": value})


@app.route("/api/admin-panel/users/<chat_id>", methods=["DELETE"])
def admin_panel_delete_user(chat_id):
    """حذف کاربر — دقیقاً همون منطق admin:confirmdelete تو تلگرام"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    with _alerts_cache_lock:
        data = load_alerts()
        data["users"] = [u for u in data.get("users", []) if str(u.get("chat_id", "")) != str(chat_id)]
        data["telegram"]["chat_ids"] = [x for x in data["telegram"].get("chat_ids", []) if str(x) != str(chat_id)]
        save_alerts(data)
    return jsonify({"ok": True})


@app.route("/api/admin-panel/bulk-false", methods=["POST"])
def admin_panel_bulk_false():
    """فالس دسته‌جمعی همه‌ی آلارم‌های فعال — دقیقاً همون منطق admin:bulkfalse تو تلگرام"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    body = request.json or {}
    reason = (body.get("reason") or "").strip()

    rows = _sb_load_active_assignments()
    tg_tok, _, _ = _get_token_and_cids()
    done_count = 0
    for row in rows:
        aid = row.get("id")
        tag = row.get("alarm_tag", "")
        _sb_false_assignment(aid, "ادمین (پنل وب)", reason)
        done_count += 1
        msg_map = _fired_msg_ids.get(aid, {})
        reason_line = f"\n📝 علت: {reason}" if reason else ""
        reply_text = f"❌ <b>فالس (دسته‌جمعی از پنل وب)</b>\n\n{tag}{reason_line}"
        for tc, tm in msg_map.items():
            if tc in ("__tag__", "__text__"):
                continue
            try:
                requests.post(f"https://api.telegram.org/bot{tg_tok}/sendMessage",
                              json={"chat_id": tc, "text": reply_text, "parse_mode": "HTML", "reply_to_message_id": tm},
                              timeout=8, headers=H)
            except Exception:
                pass
    _rebuild_active_assign_count(_sb_load_active_assignments())
    return jsonify({"ok": True, "count": done_count})


@app.route("/api/admin-panel/deprioritize-masoud", methods=["GET"])
def admin_panel_get_deprioritize():
    """وضعیت فعلی تاگل اولویت پایین مسعود رو برمی‌گردونه"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    return jsonify({"ok": True, "active": _get_deprioritize_masoud()})


@app.route("/api/admin-panel/deprioritize-masoud", methods=["POST"])
def admin_panel_set_deprioritize():
    """روشن/خاموش کردن اولویت پایین مسعود از پنل وب"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    body = request.json or {}
    value = bool(body.get("active", True))
    _set_deprioritize_masoud(value)
    return jsonify({"ok": True, "active": value})


# ─── پیام سیستمی تکرارشونده (از پنل ادمین) ─────────────────────────
# یه پیام انگلیسی که هر N دقیقه از طرف «سیستم» (نه یه آدم) برای همه‌ی اعضای ربات ارسال می‌شه.
# وضعیتش تو Supabase (ستون system_broadcast روی ردیف __config__) ذخیره می‌شه.
_sysmsg_lock = threading.Lock()
_sysmsg_state = {"active": False, "text": "", "interval_min": 2, "until": 0}

def _sb_load_system_broadcast():
    """None = خوندن ناموفق (state حافظه دست‌نخورده می‌مونه)"""
    if not SUPABASE_KEY:
        return None
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__&select=system_broadcast",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            rows = r.json()
            if rows:
                val = rows[0].get("system_broadcast")
                if val is None:
                    return {}
                return val if isinstance(val, dict) else json.loads(val)
    except Exception as e:
        print(f"[sysmsg] load exc: {e}")
    return None

def _sb_save_system_broadcast(state: dict):
    if not SUPABASE_KEY:
        return
    try:
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.__config__",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json={"system_broadcast": state}, timeout=8)
        if r.status_code >= 300:
            print(f"[sysmsg] save failed: {r.status_code} {r.text[:120]}")
    except Exception as e:
        print(f"[sysmsg] save exc: {e}")

def _sysmsg_apply(remote: dict):
    """state حافظه رو با چیزی که از Supabase خونده شد هماهنگ کن"""
    with _sysmsg_lock:
        _sysmsg_state["active"] = bool(remote.get("active", False))
        _sysmsg_state["text"] = str(remote.get("text", "") or "")
        try:
            _sysmsg_state["interval_min"] = max(1, min(60, int(remote.get("interval_min", 2))))
        except (TypeError, ValueError):
            _sysmsg_state["interval_min"] = 2
        try:
            _sysmsg_state["until"] = float(remote.get("until", 0) or 0)
        except (TypeError, ValueError):
            _sysmsg_state["until"] = 0

def _sysmsg_format(text: str) -> str:
    import html as _html
    return "⚙️ <b>System Notice</b>\n\n" + _html.escape(text, quote=False)

def system_broadcast_loop():
    last_sent = 0.0
    try:
        remote = _sb_load_system_broadcast()
        if remote:
            _sysmsg_apply(remote)
    except Exception as e:
        print(f"[sysmsg] startup load: {e}")
    while True:
        try:
            with _sysmsg_lock:
                st = dict(_sysmsg_state)
            if st["active"]:
                now = time.time()
                if st["until"] and now >= st["until"]:
                    with _sysmsg_lock:
                        _sysmsg_state["active"] = False
                        snap = dict(_sysmsg_state)
                    _sb_save_system_broadcast(snap)
                    print("[sysmsg] زمان توقف خودکار رسید — متوقف شد")
                elif st["text"].strip() and now - last_sent >= st["interval_min"] * 60:
                    # قبل از هر ارسال از Supabase بخون تا توقف از جای دیگه هم اعمال بشه
                    remote = _sb_load_system_broadcast()
                    if remote is not None and remote:
                        _sysmsg_apply(remote)
                    with _sysmsg_lock:
                        st = dict(_sysmsg_state)
                    if st["active"] and st["text"].strip():
                        token, cids, _ = _get_token_and_cids()
                        if token and cids:
                            broadcast(token, cids, _sysmsg_format(st["text"]))
                            print(f"[sysmsg] ارسال شد به {len(cids)} نفر")
                        last_sent = time.time()
        except Exception as e:
            print(f"[sysmsg] loop error: {e}")
        time.sleep(5)

@app.route("/api/admin-panel/system-broadcast", methods=["GET"])
def admin_panel_get_system_broadcast():
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    remote = _sb_load_system_broadcast()
    if remote:
        _sysmsg_apply(remote)
    with _sysmsg_lock:
        st = dict(_sysmsg_state)
    remaining = max(0, int(st["until"] - time.time())) if st["until"] else 0
    return jsonify({"ok": True, "active": st["active"], "text": st["text"],
                    "interval_min": st["interval_min"], "remaining_sec": remaining})

@app.route("/api/admin-panel/system-broadcast", methods=["POST"])
def admin_panel_set_system_broadcast():
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    body = request.json or {}
    text = str(body.get("text", "") or "").strip()
    if len(text) > 3500:
        return jsonify({"ok": False, "error": "متن خیلی طولانیه (حداکثر ۳۵۰۰ کاراکتر)"}), 400
    try:
        interval_min = max(1, min(60, int(body.get("interval_min", 2))))
        stop_after_min = max(0, min(1440, int(body.get("stop_after_min", 0) or 0)))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "عدد نامعتبر"}), 400

    # ارسال تستی فقط برای خود ادمین — چیزی ذخیره یا فعال نمی‌شه
    if body.get("test"):
        if not text:
            return jsonify({"ok": False, "error": "متن خالیه"}), 400
        token, _, _ = _get_token_and_cids()
        if not token:
            return jsonify({"ok": False, "error": "توکن ربات پیدا نشد"}), 500
        ok = send_tg(token, YOUR_CHAT_ID, _sysmsg_format(text))
        return jsonify({"ok": bool(ok), "error": None if ok else "ارسال تستی ناموفق بود"})

    active = bool(body.get("active", False))
    if active and not text:
        return jsonify({"ok": False, "error": "برای فعال کردن، اول متن رو بنویس"}), 400
    until = (time.time() + stop_after_min * 60) if (active and stop_after_min > 0) else 0
    with _sysmsg_lock:
        _sysmsg_state.update({"active": active, "text": text,
                              "interval_min": interval_min, "until": until})
        snap = dict(_sysmsg_state)
    _sb_save_system_broadcast(snap)
    return jsonify({"ok": True, "active": active, "interval_min": interval_min})


@app.route("/api/admin-panel/muted-instant-senders", methods=["GET"])
def admin_panel_get_muted_instant():
    """لیست فعلی افرادی که آلارم فوریشون ذخیره میشه ولی به تلگرام ارسال نمیشه"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    with _muted_instant_lock:
        muted = sorted(_muted_instant_senders)
    return jsonify({"ok": True, "muted": muted, "team_members": TEAM_MEMBERS})


@app.route("/api/admin-panel/muted-instant-senders", methods=["POST"])
def admin_panel_set_muted_instant():
    """روشن/خاموش کردن ارسال تلگرامِ آلارم فوری یک نفر خاص (خودِ ذخیره‌سازی همیشه انجام میشه)"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    body = request.json or {}
    name = (body.get("name") or "").strip()
    muted = bool(body.get("muted", False))
    if not name:
        return jsonify({"ok": False, "error": "اسم مشخص نشده"}), 400
    _set_instant_sender_muted(name, muted)
    with _muted_instant_lock:
        current = sorted(_muted_instant_senders)
    return jsonify({"ok": True, "muted": current})


@app.route("/api/admin-panel/team-availability", methods=["GET"])
def admin_panel_get_team_availability():
    """لیست کل اعضای تیم + وضعیت در دسترس بودنشون برای تقسیم آلارم"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    unavailable = _get_unavailable_members()
    return jsonify({"ok": True, "members": [
        {"name": m, "available": m not in unavailable} for m in TEAM_MEMBERS
    ]})


@app.route("/api/admin-panel/team-availability", methods=["POST"])
def admin_panel_set_team_availability():
    """روشن/خاموش کردن دسترسی یه عضو خاص برای گرفتن آلارم جدید (مثلاً مرخصی)"""
    auth_err = _require_admin_session()
    if auth_err:
        return auth_err
    body = request.json or {}
    name = (body.get("name") or "").strip()
    available = bool(body.get("available", True))
    if name not in TEAM_MEMBERS:
        return jsonify({"ok": False, "error": "عضو ناشناخته"}), 404
    _set_member_availability(name, available)
    return jsonify({"ok": True, "name": name, "available": available})


print("=" * 60)
print(f"[STARTUP] 🚀 سرور در حال راه‌اندازی...")
print("=" * 60)
threading.Thread(target=check_alerts, daemon=True).start()
print("[STARTUP] thread check_alerts شروع شد")

threading.Thread(target=daily_news_scheduler, daemon=True).start()
threading.Thread(target=system_broadcast_loop, daemon=True).start()
print("[STARTUP] thread system_broadcast_loop شروع شد")
print(f"[STARTUP] thread daily_news_scheduler شروع شد — ارسال ساعت {FF_NEWS_HOUR:02d}:{FF_NEWS_MINUTE:02d} تهران")
# ربات جدا و مخصوص طلا (تستی) — هرکی /start بزنه، مشترک می‌شه و همه‌ی آلارم‌های طلا رو می‌بینه
_gold_bot_subscribers: set = set()
_gold_bot_lock = threading.Lock()

def _add_gold_bot_subscriber(chat_id: str):
    global _gold_bot_subscribers
    cid = str(chat_id)
    with _gold_bot_lock:
        if cid in _gold_bot_subscribers:
            return False
        _gold_bot_subscribers.add(cid)
        snapshot = sorted(_gold_bot_subscribers)
    _sb_save_gold_bot_subscribers(snapshot)
    return True

def _broadcast_to_gold_bot(text: str):
    """این پیام رو مستقل از mute بودن ربات اصلی، به همه‌ی مشترکین ربات طلا می‌فرسته"""
    if not GOLD_BOT_TOKEN:
        return
    with _gold_bot_lock:
        subs = list(_gold_bot_subscribers)
    for cid in subs:
        try:
            send_tg(GOLD_BOT_TOKEN, cid, text)
        except Exception as e:
            print(f"[goldbot] send error to {cid}: {e}")

def poll_gold_bot():
    """حلقه‌ی polling ساده‌ی ربات طلا — فقط /start رو مدیریت می‌کنه"""
    if not GOLD_BOT_TOKEN:
        print("[goldbot] GOLD_BOT_TOKEN تنظیم نشده — این ربات غیرفعاله")
        return
    last_id = 0
    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{GOLD_BOT_TOKEN}/getUpdates",
                params={"offset": last_id + 1, "timeout": 20, "limit": 100},
                timeout=30, headers=H)
            if r.status_code != 200:
                time.sleep(10)
                continue
            for upd in r.json().get("result", []):
                last_id = upd["update_id"]
                msg = upd.get("message", {})
                text = (msg.get("text") or "").strip()
                cid = msg.get("chat", {}).get("id")
                if cid is None:
                    continue
                if text == "/start":
                    is_new = _add_gold_bot_subscriber(cid)
                    welcome = ("🟡 <b>ربات آلارم طلا</b>\n\n"
                               "از این به بعد هر آلارم طلا (فوری و معمولی) اینجا برات میاد.")
                    send_tg(GOLD_BOT_TOKEN, cid, welcome if is_new else "✅ از قبل مشترک بودی، همچنان فعاله.")
        except Exception as e:
            print(f"[goldbot] poll error: {e}")
        time.sleep(5)


def _delete_webhook_if_any(token):
    if not token:
        return
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/deleteWebhook",
                          params={"drop_pending_updates": "false"}, timeout=15)
        print(f"[STARTUP] deleteWebhook({token[:10]}...): {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"[STARTUP] deleteWebhook error: {e}")

_main_tok, _, _ = _get_token_and_cids()
_delete_webhook_if_any(_main_tok)
_delete_webhook_if_any(GOLD_BOT_TOKEN)

threading.Thread(target=poll_telegram, daemon=True).start()
threading.Thread(target=poll_gold_bot, daemon=True).start()
print("[STARTUP] thread poll_telegram شروع شد")

# ── بازیابی fired_msgs و counters از Supabase بعد از restart ──
_sb_load_fired_msgs()
threading.Thread(target=_rating_autoclose_loop, daemon=True).start()
_sb_load_sym_counters()
# بازسازی کامل state از Supabase بعد از هر restart
_sb_restore_on_startup()

# ── بازیابی notified set از Supabase — جلوگیری از double-fire بعد از restart ──
def _restore_notified():
    """آلارم‌هایی که قبلاً fire شدن رو به notified اضافه کن تا دوباره fire نشن"""
    if not SUPABASE_KEY: return
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?status=eq.fired&select=id&limit=5000",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200:
            for row in r.json():
                aid = row.get("id")
                if aid:
                    notified.add(str(aid))
                    _deleted_ids.add(str(aid))
            print(f"[STARTUP] notified بازسازی شد — {len(notified)} آلارم fired")
    except Exception as e:
        print(f"[STARTUP] notified restore error: {e}")
_restore_notified()


@app.route("/api/gold-alarms")
def api_gold_alarms():
    """
    آلارم‌های طلا (فعال یا فایرشده) — مستقیم و فیلترشده از Supabase می‌خونه
    (نه از کش حافظه)، بدون فیلتر شخصی/تیمی (استثنا: طلا برای همه قابل دیدنه).
    این روش هم دقیق‌تره (وابسته به تازگی کش نیست) هم کم‌مصرف‌تر روی Egress،
    چون فقط ردیف‌های طلا و فقط همون صفحه‌ی درخواستی از Supabase کشیده می‌شه،
    نه کل جدول.
    Query params: status=active|fired, page (از ۱), per_page (پیش‌فرض ۱۰),
                  from, to (YYYY-MM-DD), sender (substring match روی created_by)
    """
    if not SUPABASE_KEY:
        return jsonify({"ok": False, "error": "Supabase تنظیم نشده"}), 500

    status = request.args.get("status", "active")
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(50, max(1, int(request.args.get("per_page", 10))))
    date_from = request.args.get("from", "").strip()
    date_to = request.args.get("to", "").strip()
    sender_q = request.args.get("sender", "").strip()

    date_field = "created_at" if status == "active" else "fired_at"
    params = {"symbol": "ilike.*XAU*", "order": f"{date_field}.desc"}
    if status == "active":
        params["active"] = "eq.true"
    else:
        params["fired_at"] = "not.is.null"
    if sender_q:
        params["created_by"] = f"ilike.*{sender_q}*"

    # چون یه ستون ممکنه هم gte هم lte بخواد، از پارامتر ترکیبی and= استفاده می‌کنیم
    and_conditions = []
    if date_from:
        and_conditions.append(f"{date_field}.gte.{date_from}")
    if date_to:
        and_conditions.append(f"{date_field}.lte.{date_to}T23:59:59")
    if and_conditions:
        params["and"] = f"({','.join(and_conditions)})"

    offset = (page - 1) * per_page
    headers = {**_sb_h(), "Prefer": "count=exact", "Range-Unit": "items",
               "Range": f"{offset}-{offset + per_page - 1}"}
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/alerts", params=params, headers=headers, timeout=10)
    except Exception as e:
        return jsonify({"ok": False, "error": f"اتصال به Supabase ناموفق: {e}"}), 500

    if r.status_code not in (200, 206):
        return jsonify({"ok": False, "error": f"Supabase خطا داد ({r.status_code}): {r.text[:200]}"}), 500

    total = 0
    cr = r.headers.get("content-range", "")
    if "/" in cr:
        try: total = int(cr.split("/")[-1])
        except Exception: pass

    rows = r.json() if isinstance(r.json(), list) else []
    pages = max(1, (total + per_page - 1) // per_page) if total else 1
    page = min(page, pages)

    out = [{
        "id": a.get("id"),
        "condition": a.get("condition"),
        "target_price": a.get("target_price"),
        "created_by": a.get("created_by") or "—",
        "created_at": a.get("created_at"),
        "comment": a.get("comment") or "",
        "fired_at": a.get("fired_at"),
        "fired_price": a.get("fired_price"),
        "instant": bool(a.get("instant")),
    } for a in rows]

    return jsonify({"ok": True, "items": out, "total": total, "page": page, "pages": pages})

_home_summary_cache = {"data": None, "ts": 0}
_home_summary_lock = threading.Lock()
HOME_SUMMARY_CACHE_TTL = 90  # ثانیه — چند نفر همزمان صفحه رو باز کنن، فقط یه‌بار حساب می‌شه


@app.route("/api/home-summary")
def api_home_summary():
    """خلاصه‌ی اخبار امروز برای بالای صفحه‌ی اصلی — هیچ کوئری‌ای به Supabase نمی‌زنه،
    فقط از لیست اخباری که هر روز صبح یه‌بار خونده می‌شه استفاده می‌کنه."""
    with _home_summary_lock:
        if _home_summary_cache["data"] is not None and (time.time() - _home_summary_cache["ts"]) < HOME_SUMMARY_CACHE_TTL:
            return jsonify(_home_summary_cache["data"])

    now = datetime.now(TEHRAN)
    today_news = []
    next_news = None
    try:
        for ev in _today_red_events:
            t = ev.get("time_teh")
            if not t or ":" not in t:
                continue
            today_news.append({"title": ev.get("title", "—"), "time": t})
            hh, mm = map(int, t.split(":"))
            ev_dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            minutes_left = int((ev_dt - now).total_seconds() // 60)
            if minutes_left >= 0 and (next_news is None or minutes_left < next_news["minutes_left"]):
                next_news = {"title": ev.get("title", "—"), "time": t, "minutes_left": minutes_left}
    except Exception as e:
        print(f"[home-summary] news error: {e}")

    data = {
        "ok": True,
        "next_news": next_news,
        "today_news": today_news,
        "generated_at": now.strftime("%H:%M"),
    }
    with _home_summary_lock:
        _home_summary_cache["data"] = data
        _home_summary_cache["ts"] = time.time()
    return jsonify(data)


@app.route("/api/all-alarms")
def api_all_alarms():
    """
    نسخه‌ی عمومیِ /api/gold-alarms — برای همه‌ی نمادها (نه فقط طلا).
    Query params: status=active|fired, page, per_page (پیش‌فرض ۱۰),
                  from, to (YYYY-MM-DD), symbol (substring match، خالی = همه)
    """
    if not SUPABASE_KEY:
        return jsonify({"ok": False, "error": "Supabase تنظیم نشده"}), 500

    status = request.args.get("status", "fired")
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(50, max(1, int(request.args.get("per_page", 10))))
    date_from = request.args.get("from", "").strip()
    date_to = request.args.get("to", "").strip()
    symbol_q = request.args.get("symbol", "").strip()

    date_field = "created_at" if status == "active" else "fired_at"
    params = {"order": f"{date_field}.desc"}
    if status == "active":
        params["active"] = "eq.true"
    else:
        params["fired_at"] = "not.is.null"
    if symbol_q:
        params["symbol"] = f"ilike.*{symbol_q}*"

    and_conditions = []
    if date_from:
        and_conditions.append(f"{date_field}.gte.{date_from}")
    if date_to:
        and_conditions.append(f"{date_field}.lte.{date_to}T23:59:59")
    if and_conditions:
        params["and"] = f"({','.join(and_conditions)})"

    offset = (page - 1) * per_page
    headers = {**_sb_h(), "Prefer": "count=exact", "Range-Unit": "items",
               "Range": f"{offset}-{offset + per_page - 1}"}
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/alerts", params=params, headers=headers, timeout=10)
    except Exception as e:
        return jsonify({"ok": False, "error": f"اتصال به Supabase ناموفق: {e}"}), 500

    if r.status_code not in (200, 206):
        return jsonify({"ok": False, "error": f"Supabase خطا داد ({r.status_code}): {r.text[:200]}"}), 500

    total = 0
    cr = r.headers.get("content-range", "")
    if "/" in cr:
        try: total = int(cr.split("/")[-1])
        except Exception: pass

    rows = r.json() if isinstance(r.json(), list) else []
    pages = max(1, (total + per_page - 1) // per_page) if total else 1
    page = min(page, pages)

    out = [{
        "id": a.get("id"),
        "symbol": a.get("symbol"),
        "condition": a.get("condition"),
        "target_price": a.get("target_price"),
        "created_by": a.get("created_by") or "—",
        "created_at": a.get("created_at"),
        "comment": a.get("comment") or "",
        "fired_at": a.get("fired_at"),
        "fired_price": a.get("fired_price"),
        "instant": bool(a.get("instant")),
    } for a in rows if not a.get("is_private")]

    return jsonify({"ok": True, "items": out, "total": total, "page": page, "pages": pages})


@app.route("/api/symbol-candles")
def api_symbol_candles():
    """کندل‌های خام یه نماد دلخواه از biquote.io — برای چارت صفحه‌ی همه‌آلارم‌ها"""
    symbol = request.args.get("symbol", "").strip().upper()
    interval = request.args.get("interval", "15m")
    limit = min(1000, max(10, int(request.args.get("limit", 1000))))
    if not symbol:
        return jsonify({"ok": False, "error": "نماد مشخص نشده"}), 400
    try:
        r = requests.get(f"https://biquote.io/api/{symbol}/ohlc",
                          params={"interval": interval, "limit": limit}, timeout=12)
        r.raise_for_status()
        bars = r.json().get("bars", [])
        candles = [{"time": b["openTime"], "open": b["open"], "high": b["high"],
                    "low": b["low"], "close": b["close"]} for b in bars]
        return jsonify({"ok": True, "candles": candles})
    except Exception as e:
        return jsonify({"ok": False, "error": f"کندل‌ها لود نشد: {e}", "candles": []})


@app.route("/all-alarms")
def all_alarms_page():
    return send_from_directory(app.static_folder, "all-alarms.html")


@app.route("/gold-alarms")
def gold_alarms_page():
    return send_from_directory(app.static_folder, "gold-alarms.html")



@app.route("/api/gold-chart-data")
def api_gold_chart_data():
    """
    کندل‌های XAUUSD (از biquote.io — همون سرویسی که برای قیمت لحظه‌ای استفاده می‌کنیم)
    + همه‌ی آلارم‌های فعال طلا (برای رسم خط روی چارت).
    Query params: interval (پیش‌فرض 1h), limit (پیش‌فرض 300)
    """
    interval = request.args.get("interval", "1h")
    limit = min(1000, max(10, int(request.args.get("limit", 300))))
    candles_only = request.args.get("candles_only", "").lower() == "true"

    candles, alarms, fired, errors = [], [], [], []

    try:
        r = requests.get(f"https://biquote.io/api/XAUUSD/ohlc",
                          params={"interval": interval, "limit": limit}, timeout=12)
        r.raise_for_status()
        bars = r.json().get("bars", [])
        candles = [{"time": b["openTime"], "open": b["open"], "high": b["high"],
                    "low": b["low"], "close": b["close"]} for b in bars]
    except Exception as e:
        errors.append(f"کندل‌ها لود نشد: {e}")

    if candles_only:
        return jsonify({"ok": True, "candles": candles, "alarms": [], "fired": [], "errors": errors})

    if SUPABASE_KEY:
        try:
            rr = requests.get(
                f"{SUPABASE_URL}/rest/v1/alerts",
                params={"symbol": "ilike.*XAU*", "active": "eq.true",
                        "select": "id,condition,target_price,created_by"},
                headers=_sb_h(), timeout=10)
            if rr.status_code == 200:
                for a in rr.json():
                    alarms.append({
                        "price": a.get("target_price"),
                        "condition": a.get("condition"),
                        "label": a.get("created_by") or "—",
                    })
        except Exception as e:
            errors.append(f"آلارم‌ها لود نشد: {e}")

        # آلارم‌های فایرشده‌ی اخیر (شامل آلارم فوری) — برای مارکر روی چارت
        try:
            two_weeks_ago = (datetime.now(TEHRAN) - timedelta(days=14)).strftime("%Y-%m-%d")
            rf = requests.get(
                f"{SUPABASE_URL}/rest/v1/alerts",
                params={"symbol": "ilike.*XAU*", "fired_at": f"gte.{two_weeks_ago}",
                        "select": "id,condition,fired_price,fired_at,created_by",
                        "order": "fired_at.desc", "limit": "100"},
                headers=_sb_h(), timeout=10)
            if rf.status_code == 200:
                for a in rf.json():
                    if a.get("fired_price") and a.get("fired_at"):
                        fired.append({
                            "price": a.get("fired_price"),
                            "condition": a.get("condition"),
                            "label": a.get("created_by") or "—",
                            "fired_at": a.get("fired_at"),
                        })
        except Exception as e:
            errors.append(f"فایرشده‌ها لود نشد: {e}")

    return jsonify({"ok": True, "candles": candles, "alarms": alarms, "fired": fired, "errors": errors})


@app.route("/gold-chart")
def gold_chart_page():
    return send_from_directory(app.static_folder, "gold-chart.html")


@app.route("/sva-report")
def sva_report_page():
    """
    گزارش SVA — همون فایل ثابتی که دستی (با تحلیل کامل) ساخته و تأیید شده.
    هر بار که داده‌ی جدید بیاد، این فایل static/sva-report.html جایگزین می‌شه و push می‌خوریم؛
    این مسیر همیشه آخرین نسخه‌ای که منتشر شده رو نشون می‌ده.
    """
    return send_from_directory(app.static_folder, "sva-report.html")


@app.route("/report/weekly")
def report_weekly_html():
    """گزارش آلارم‌های تیم — با بازه تاریخ دلخواه و شماره‌گذاری"""
    from_str = request.args.get("from", "")
    to_str = request.args.get("to", "")
    now_dt = datetime.now(TEHRAN)

    range_start = None
    range_end = None
    try:
        if from_str:
            range_start = TEHRAN.localize(datetime.strptime(from_str, "%Y-%m-%d"))
    except: range_start = None
    try:
        if to_str:
            range_end = TEHRAN.localize(datetime.strptime(to_str, "%Y-%m-%d")) + timedelta(days=1)
    except: range_end = None

    if range_start is None and range_end is None:
        # پیش‌فرض: هفته جاری (شنبه تا الان)
        days_since_sat = (now_dt.weekday() - 5) % 7
        range_start = (now_dt - timedelta(days=days_since_sat)).replace(hour=0, minute=0, second=0, microsecond=0)
        range_end = None
    elif range_start is None:
        range_start = range_end - timedelta(days=7)
    elif range_end is None:
        range_end = now_dt + timedelta(days=1)

    from_value = (range_start).strftime("%Y-%m-%d")
    to_value = (range_end - timedelta(days=1)).strftime("%Y-%m-%d") if range_end else now_dt.strftime("%Y-%m-%d")
    week_label = f"{range_start.strftime('%d/%m/%Y')} — {to_value[8:10]}/{to_value[5:7]}/{to_value[0:4]}"

    range_start_str = range_start.strftime("%Y-%m-%dT%H:%M:%S")
    PAGE_SIZE = 50
    try:
        page = max(1, int(request.args.get("page", "1")))
    except (TypeError, ValueError):
        page = 1
    offset = (page - 1) * PAGE_SIZE
    rows = []
    total_count = 0
    if SUPABASE_KEY:
        try:
            url = (f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                   f"?fired_at=gte.{range_start_str}&select=*&order=fired_at.desc"
                   f"&limit={PAGE_SIZE}&offset={offset}")
            if range_end:
                url += f"&fired_at=lt.{range_end.strftime('%Y-%m-%dT%H:%M:%S')}"
            r = requests.get(url, headers={**_sb_h(), "Prefer": "count=exact"}, timeout=10)
            if r.status_code in (200, 206):
                rows = r.json()
                cr = r.headers.get("Content-Range", "")  # مثلاً "0-49/213"
                if "/" in cr:
                    try: total_count = int(cr.split("/")[-1])
                    except ValueError: total_count = len(rows)
                else:
                    total_count = len(rows)
        except: pass
    total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
    alert_ids = sorted({str(r.get("id","")) for r in rows if r.get("id")})
    alerts_map = {}
    if SUPABASE_KEY and alert_ids:
        try:
            r2 = requests.get(
                f"{SUPABASE_URL}/rest/v1/alerts?id=in.({','.join(alert_ids)})&select=*",
                headers=_sb_h(), timeout=10)
            if r2.status_code == 200:
                for row in r2.json():
                    alerts_map[str(row.get("id",""))] = row
        except Exception as e:
            print(f"[report_weekly] targeted alerts fetch error: {e}")
    rows = [r for r in rows if not alerts_map.get(str(r.get("id","")), {}).get("is_private")]

    # آلارم‌های فوری (instant) — این‌ها اصلاً وارد چرخه‌ی تخصیص نمی‌شن، پس جدا از جدول alerts می‌خونیم
    instant_rows = []
    if SUPABASE_KEY:
        try:
            iu = (f"{SUPABASE_URL}/rest/v1/alerts"
                  f"?instant=eq.true&fired_at=gte.{range_start_str}&select=*&order=fired_at.desc")
            if range_end:
                iu += f"&fired_at=lt.{range_end.strftime('%Y-%m-%dT%H:%M:%S')}"
            ri = requests.get(iu, headers=_sb_h(), timeout=10)
            if ri.status_code == 200:
                instant_rows = [a for a in ri.json() if not a.get("is_private")]
        except Exception as e:
            print(f"[report_weekly] instant fetch error: {e}")

    rows_html = ""
    false_by_set = set()
    for idx, row in enumerate(rows, 1):
        aid       = str(row.get("id",""))
        tag       = row.get("alarm_tag","—")
        assignee  = row.get("assigned_to","") or "⏳ منتظر"
        fired     = row.get("fired_at","")[:16]
        false_by  = row.get("false_by","") or ""
        false_at  = row.get("false_at","")[:16] if row.get("false_at") else ""
        false_rsn = row.get("false_reason","") or ""
        is_active = row.get("is_active", True)
        if false_by:
            false_by_set.add(false_by)
        # تاریخچه کامل false
        false_history = row.get("false_history") or []
        if isinstance(false_history, str):
            try: false_history = json.loads(false_history)
            except: false_history = []
        alert     = alerts_map.get(aid, {})
        sym       = alert.get("symbol","") or row.get("symbol","") or ""
        tgt_raw   = alert.get("target_price",0) or row.get("target_price",0) or 0
        target    = fmt_price(float(tgt_raw), sym) if tgt_raw else "—"
        creator   = alert.get("created_by","") or row.get("created_by","") or "—"
        alarm_comment = alert.get("comment","") or ""
        created   = str(alert.get("created_at",""))[:16]
        status_cls = "active" if is_active else "false"
        status_txt = "فعال" if is_active else f"False — {false_by}"
        cond_html = alert.get("condition", "") or row.get("condition", "")
        is_buy = (cond_html == "below")
        direction_icon = "📉" if cond_html == "above" else ("📈" if cond_html == "below" else "❓")
        dir_zone_label = "ناحیه سل" if cond_html == "above" else ("ناحیه بای" if cond_html == "below" else "")
        candle_cls = "candle-up" if is_buy else "candle-down"
        false_detail = ""
        false_action = ""
        if not is_active:
            if false_history:
                hist_items = ""
                for idx_h, h in enumerate(false_history, 1):
                    h_by  = h.get("by","")
                    h_at  = str(h.get("at",""))[:16]
                    h_rsn = h.get("reason","")
                    rsn_span = f'<span class="reason">{h_rsn}</span>' if h_rsn else ""
                    hist_items += f'<span class="hist-entry"><b>{idx_h}.</b> {h_by} — 🕐 {h_at}{(" · "+rsn_span) if rsn_span else ""}</span>'
                false_detail = f'<div class="false-detail false-history">{hist_items}</div>'
            else:
                false_detail = f'<div class="false-detail"><span>🕐 {false_at}</span>{("<span class=reason>"+false_rsn+"</span>") if false_rsn else ""}</div>'
        else:
            false_action = f'''<div class="false-action" data-aid="{aid}">
              <input type="text" class="false-reason-inp" placeholder="علت/کامنت (اختیاری)">
              <button type="button" class="false-btn" onclick="markFalse('{aid}', this)">❌ فالس کردن</button>
            </div>'''
        rows_html += f"""
        <div class="card card-{status_cls}" data-kind="regular" data-search="{(assignee + ' ' + sym + ' ' + tag + ' ' + creator + ' ' + false_by).lower()}" data-falseby="{false_by.lower()}" data-status="{status_cls}">
          <div class="card-num">{idx}</div>
          <div class="card-inner">
          <div class="card-glow"></div>
          <div class="card-header">
            <div class="card-icon">{direction_icon}</div>
            <div class="card-title">
              <span class="tag">{tag}</span>
              <span class="sym">{sym}{(' • ' + dir_zone_label) if dir_zone_label else ''}</span>
            </div>
            <span class="badge badge-{status_cls}">{status_txt}</span>
          </div>
          <div class="card-target">
            <span class="target-lbl">🎯 قیمت هدف</span>
            <span class="target-val">{target}</span>
          </div>
          <div class="card-body">
            <div class="info-grid">
              <div class="info-cell"><span class="lbl">📅 ثبت</span><span class="val">{created}</span></div>
              <div class="info-cell"><span class="lbl">⏰ فایر</span><span class="val">{fired}</span></div>
              <div class="info-cell"><span class="lbl">👤 سازنده</span><span class="val">{creator}</span></div>
              <div class="info-cell"><span class="lbl">🙋 مسئول</span><span class="val highlight">{assignee}</span></div>
            </div>
            {false_detail}
            {false_action}
            {'<div class="false-detail" style="margin-top:8px"><span>💬 ' + alarm_comment + '</span></div>' if alarm_comment else ''}
          </div>
          <div class="card-rail">
            <div class="rail-dot {candle_cls}"></div>
            <div class="mini-candles">
              <span class="mc {candle_cls}" style="height:40%"></span>
              <span class="mc {candle_cls}" style="height:65%"></span>
              <span class="mc {candle_cls}" style="height:30%"></span>
              <span class="mc {candle_cls}" style="height:85%"></span>
              <span class="mc {candle_cls}" style="height:50%"></span>
            </div>
            <span class="rail-label">{dir_zone_label if dir_zone_label else ''}</span>
          </div>
          </div>
        </div>"""

    # ── کارت‌های آلارم فوری (پیش‌فرض مخفی — فقط با فیلتر دیده می‌شن) ──
    for idx_i, a in enumerate(instant_rows, 1):
        i_sym = a.get("symbol","") or ""
        i_cond = a.get("condition","") or ""
        i_creator = a.get("created_by","") or "—"
        i_comment = a.get("comment","") or ""
        i_fired = str(a.get("fired_at",""))[:16]
        i_created = str(a.get("created_at",""))[:16]
        i_tag = a.get("tag","—") or "—"
        i_fired_price = a.get("fired_price")
        i_target = fmt_price(float(i_fired_price), i_sym) if i_fired_price else "—"
        i_is_buy = (i_cond == "below")
        i_dir_icon = "📉" if i_cond == "above" else ("📈" if i_cond == "below" else "⚡")
        i_zone_label = "ناحیه سل" if i_cond == "above" else ("ناحیه بای" if i_cond == "below" else "")
        i_candle_cls = "candle-up" if i_is_buy else "candle-down"
        rows_html += f"""
        <div class="card card-active" data-kind="instant" data-search="{('فوری ' + i_sym + ' ' + i_tag + ' ' + i_creator).lower()}" data-falseby="" data-status="active">
          <div class="card-num">⚡</div>
          <div class="card-inner">
          <div class="card-glow"></div>
          <div class="card-header">
            <div class="card-icon">{i_dir_icon}</div>
            <div class="card-title">
              <span class="tag">{i_tag}</span>
              <span class="sym">{i_sym}{(' • ' + i_zone_label) if i_zone_label else ''}</span>
            </div>
            <span class="badge badge-active">⚡ فوری</span>
          </div>
          <div class="card-target">
            <span class="target-lbl">💰 قیمت فایر</span>
            <span class="target-val">{i_target}</span>
          </div>
          <div class="card-body">
            <div class="info-grid">
              <div class="info-cell"><span class="lbl">📅 ثبت</span><span class="val">{i_created}</span></div>
              <div class="info-cell"><span class="lbl">⏰ فایر</span><span class="val">{i_fired}</span></div>
              <div class="info-cell"><span class="lbl">👤 سازنده</span><span class="val">{i_creator}</span></div>
              <div class="info-cell"><span class="lbl">🙋 مسئول</span><span class="val highlight">⚡ broadcast فوری</span></div>
            </div>
            {'<div class="false-detail" style="margin-top:8px"><span>💬 ' + i_comment + '</span></div>' if i_comment else ''}
          </div>
          <div class="card-rail">
            <div class="rail-dot {i_candle_cls}"></div>
            <div class="mini-candles">
              <span class="mc {i_candle_cls}" style="height:40%"></span>
              <span class="mc {i_candle_cls}" style="height:65%"></span>
              <span class="mc {i_candle_cls}" style="height:30%"></span>
              <span class="mc {i_candle_cls}" style="height:85%"></span>
              <span class="mc {i_candle_cls}" style="height:50%"></span>
            </div>
            <span class="rail-label">{i_zone_label}</span>
          </div>
          </div>
        </div>"""

    false_by_options = "".join(
        f'<option value="by:{name.lower()}">👤 {name}</option>' for name in sorted(false_by_set)
    )
    # چون rows الان فقط همین صفحه‌ست، برای آمار سربرگ (کل/فعال/False) به‌جای
    # فچ کامل، یه کوئری سبک count-only (بدون داده‌ی واقعی) برای کل بازه می‌زنیم.
    def _sb_count_assignments(extra_filter=""):
        if not SUPABASE_KEY: return 0
        try:
            u = (f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                 f"?fired_at=gte.{range_start_str}&select=id&limit=1{extra_filter}")
            if range_end:
                u += f"&fired_at=lt.{range_end.strftime('%Y-%m-%dT%H:%M:%S')}"
            rc = requests.get(u, headers={**_sb_h(), "Prefer": "count=exact"}, timeout=10)
            cr = rc.headers.get("Content-Range", "")
            if "/" in cr:
                return int(cr.split("/")[-1])
        except Exception:
            pass
        return 0
    active_count = _sb_count_assignments("&is_active=eq.true")
    false_count  = total_count - active_count

    pagination_html = ""
    if total_pages > 1:
        base_url = f"/report/weekly?from={from_value}&to={to_value}"
        prev_link = f'<a href="{base_url}&page={page-1}" class="page-btn">← قبلی</a>' if page > 1 else '<span class="page-btn disabled">← قبلی</span>'
        next_link = f'<a href="{base_url}&page={page+1}" class="page-btn">بعدی →</a>' if page < total_pages else '<span class="page-btn disabled">بعدی →</span>'
        pagination_html = f'''<div class="pagination">
          {prev_link}
          <span class="page-info">صفحه {page} از {total_pages} ({total_count} مورد)</span>
          {next_link}
        </div>'''
    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>گزارش آلارم‌های تیم — {week_label}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  :root{{
    --bg:#070a12;--surface:#0d1424;--surface2:#10192e;
    --border:#1e293b;--border2:#2d3f5c;
    --text:#e2e8f0;--muted:#64748b;--subtle:#334155;
    --blue:#3b82f6;--blue-dim:#1e3a5f;--blue-glow:rgba(59,130,246,.35);
    --green:#22c55e;--green-dim:#052e16;--green-border:#166534;--green-glow:rgba(34,197,94,.3);
    --red:#ef4444;--red-dim:#1c0a0a;--red-border:#7f1d1d;--red-glow:rgba(239,68,68,.3);
    --gold:#fbbf24;--purple:#8b5cf6;
  }}
  [data-theme="light"]{{
    --bg:#eef2f7;--surface:#ffffff;--surface2:#ffffff;
    --border:#e2e8f0;--border2:#cbd5e1;
    --text:#1e293b;--muted:#64748b;--subtle:#94a3b8;
    --blue:#2563eb;--blue-dim:#dbeafe;--blue-glow:rgba(37,99,235,.15);
    --green:#16a34a;--green-dim:#dcfce7;--green-border:#86efac;--green-glow:rgba(22,163,74,.15);
    --red:#dc2626;--red-dim:#fee2e2;--red-border:#fca5a5;--red-glow:rgba(220,38,38,.15);
    --gold:#d97706;--purple:#7c3aed;
  }}
  html{{scroll-behavior:smooth}}
  body{{font-family:'Inter',Tahoma,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;direction:rtl;transition:background .3s,color .3s;position:relative;overflow-x:hidden}}

  /* ── Animated background particles ── */
  .bg-grid{{position:fixed;inset:0;z-index:0;opacity:.4;pointer-events:none;
    background-image:linear-gradient(var(--border) 1px,transparent 1px),linear-gradient(90deg,var(--border) 1px,transparent 1px);
    background-size:40px 40px;mask-image:radial-gradient(ellipse 60% 50% at 50% 0%,#000 0%,transparent 100%)}}
  .bg-orb{{position:fixed;border-radius:50%;filter:blur(80px);pointer-events:none;z-index:0;opacity:.35;animation:float 16s ease-in-out infinite}}
  .bg-orb.o1{{width:300px;height:300px;background:var(--blue);top:-100px;right:-80px}}
  .bg-orb.o2{{width:260px;height:260px;background:var(--purple);top:30%;left:-100px;animation-delay:-5s}}
  .bg-orb.o3{{width:240px;height:240px;background:var(--green);bottom:0;right:10%;animation-delay:-10s}}
  @keyframes float{{0%,100%{{transform:translate(0,0) scale(1)}}50%{{transform:translate(30px,-40px) scale(1.1)}}}}

  /* ── Scroll progress ── */
  .scroll-bar{{position:fixed;top:0;right:0;left:0;height:4px;background:var(--border);z-index:999;overflow:hidden}}
  .scroll-bar-fill{{height:100%;width:0%;background:linear-gradient(90deg,var(--green),var(--blue),var(--purple));transition:width .08s;position:relative}}
  .scroll-bar-fill::after{{content:'';position:absolute;inset:0;
    background:repeating-linear-gradient(90deg,transparent 0 6px,rgba(255,255,255,.3) 6px 8px)}}
  .scroll-dot{{position:fixed;top:0;width:16px;height:16px;border-radius:50%;
    background:radial-gradient(circle,#fff,var(--blue));box-shadow:0 0 14px var(--blue);z-index:1000;
    transform:translate(-50%,-6px);transition:left .08s;display:flex;align-items:center;justify-content:center;font-size:9px}}

  /* ── Theme toggle ── */
  .theme-toggle{{position:fixed;top:14px;left:14px;z-index:998;width:44px;height:44px;border-radius:50%;border:1px solid var(--border2);background:var(--surface);color:var(--text);font-size:19px;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 16px rgba(0,0,0,.25);transition:.25s}}
  .theme-toggle:hover{{transform:scale(1.1) rotate(15deg)}}

  /* ── Header ── */
  .hero{{padding:42px 20px 28px;text-align:center;position:relative;z-index:1}}
  .hero h1{{font-size:26px;font-weight:800;margin-bottom:8px;letter-spacing:-.5px;
    background:linear-gradient(135deg,var(--blue),var(--purple));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;
    animation:fadeDown .5s ease}}
  .hero .period{{font-size:13px;color:var(--muted);margin-bottom:24px;animation:fadeDown .6s ease}}

  /* ── Stats bar ── */
  .stats{{display:flex;gap:14px;justify-content:center;flex-wrap:wrap;animation:fadeUp .6s ease}}
  .stat{{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:14px 26px;text-align:center;min-width:90px;transition:.25s;position:relative;overflow:hidden}}
  .stat::before{{content:'';position:absolute;inset:0;background:linear-gradient(135deg,var(--blue-glow),transparent);opacity:0;transition:.3s}}
  .stat:hover::before{{opacity:1}}
  .stat:hover{{transform:translateY(-4px) scale(1.03);box-shadow:0 8px 24px var(--blue-glow)}}
  .stat .num{{font-size:26px;font-weight:800;position:relative}}
  .stat .lbl2{{font-size:11px;color:var(--muted);margin-top:4px;position:relative}}
  .stat.green .num{{color:var(--green)}}
  .stat.red .num{{color:var(--red)}}
  .stat.green:hover{{box-shadow:0 8px 24px var(--green-glow)}}
  .stat.red:hover{{box-shadow:0 8px 24px var(--red-glow)}}

  /* ── Date range nav ── */
  .range-nav{{max-width:680px;margin:0 auto;padding:14px 20px 4px;
    display:flex;flex-direction:column;gap:10px;align-items:center}}
  .range-presets{{display:flex;gap:8px;flex-wrap:wrap;justify-content:center}}
  .preset-btn{{padding:8px 18px;border-radius:12px;border:1px solid var(--border2);background:var(--surface);
    color:var(--muted);font-size:12px;font-weight:600;cursor:pointer;transition:.25s;font-family:'Inter',Tahoma,sans-serif}}
  .preset-btn:hover{{border-color:var(--blue);color:var(--blue);transform:translateY(-2px)}}
  .pagination{{display:flex;align-items:center;justify-content:center;gap:14px;margin:20px 0}}
  .page-btn{{padding:8px 18px;border-radius:12px;border:1px solid var(--border2);background:var(--surface);
    color:var(--blue);font-size:12px;font-weight:700;cursor:pointer;transition:.25s;text-decoration:none;
    font-family:'Inter',Tahoma,sans-serif}}
  .page-btn:hover{{border-color:var(--blue);transform:translateY(-2px)}}
  .page-btn.disabled{{color:var(--muted);opacity:.4;pointer-events:none}}
  .page-info{{font-size:12px;color:var(--muted);font-weight:600}}
  .range-inputs{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;justify-content:center;
    background:var(--surface);border:1px solid var(--border2);border-radius:14px;padding:8px 14px;backdrop-filter:blur(10px)}}
  .range-inputs label{{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--muted);font-weight:600}}
  .range-inputs input[type="date"]{{padding:7px 10px;border-radius:10px;border:1px solid var(--border2);
    background:var(--surface2);color:var(--text);font-family:'Inter',Tahoma,sans-serif;font-size:12px;outline:none}}
  .range-inputs input[type="date"]:focus{{border-color:var(--blue)}}
  .range-go{{padding:8px 20px;border-radius:10px;border:none;cursor:pointer;font-size:12px;font-weight:700;
    background:linear-gradient(135deg,var(--blue),var(--purple));color:#fff;box-shadow:0 6px 16px var(--blue-glow)}}
  .range-go:hover{{transform:translateY(-1px)}}

  /* ── Search ── */
  .search-wrap{{max-width:680px;margin:0 auto;padding:0 20px 14px;position:relative;z-index:1}}
  .search-input{{width:100%;padding:13px 18px;border-radius:14px;border:1px solid var(--border2);
    background:var(--surface);color:var(--text);font-size:13px;font-family:'Inter',Tahoma,sans-serif;
    direction:rtl;outline:none;transition:.25s}}
  .search-input:focus{{border-color:var(--blue);box-shadow:0 0 0 3px var(--blue-glow)}}
  .search-input::placeholder{{color:var(--muted)}}
  .search-select{{width:100%;margin-top:10px;padding:13px 18px;border-radius:14px;border:1px solid var(--border2);
    background:var(--surface);color:var(--text);font-size:13px;font-family:'Inter',Tahoma,sans-serif;
    direction:rtl;outline:none;transition:.25s;cursor:pointer}}
  .search-select:focus{{border-color:var(--blue);box-shadow:0 0 0 3px var(--blue-glow)}}
  .search-count{{display:block;text-align:center;font-size:11px;color:var(--muted);margin-top:8px}}

  /* ── Cards ── */
  .list{{padding:10px 20px 40px;max-width:680px;margin:0 auto;position:relative;z-index:1}}
  .card{{border-radius:20px;border:1px solid var(--border);margin-bottom:18px;overflow:hidden;transition:.3s;
         opacity:0;transform:translateY(20px) scale(.98);animation:cardIn .5s ease forwards;
         background:var(--surface);position:relative;display:flex;align-items:stretch}}
  .card:hover{{transform:translateY(-5px) scale(1.01);box-shadow:0 16px 40px var(--blue-glow);border-color:var(--blue)}}
  .card-false:hover{{box-shadow:0 16px 40px var(--red-glow);border-color:var(--red)}}
  .card-glow{{position:absolute;top:-50%;left:-20%;width:60%;height:200%;
    background:radial-gradient(circle,var(--blue-glow),transparent 70%);pointer-events:none;opacity:.5}}
  .card-false .card-glow{{background:radial-gradient(circle,var(--red-glow),transparent 70%)}}
  .card-inner{{flex:1;min-width:0;position:relative}}
  .card-num{{flex:0 0 40px;display:flex;align-items:center;justify-content:center;
    background:var(--surface2);border-left:1px solid var(--border);
    color:var(--muted);font-family:'Inter',monospace;font-size:14px;font-weight:800}}

  .card-header{{display:flex;align-items:center;gap:12px;padding:18px 18px 14px;position:relative;z-index:1}}
  .card-icon{{font-size:26px;width:46px;height:46px;display:flex;align-items:center;justify-content:center;
    background:var(--surface2);border-radius:12px;border:1px solid var(--border)}}
  .card-title{{display:flex;flex-direction:column;gap:3px;flex:1}}
  .tag{{font-weight:800;font-size:17px;color:var(--blue)}}
  .sym{{font-size:11px;color:var(--muted);font-weight:600;letter-spacing:.5px}}
  .badge{{font-size:11px;font-weight:700;padding:6px 14px;border-radius:24px;white-space:nowrap}}
  .badge-active{{background:var(--green-dim);color:var(--green);border:1px solid var(--green-border)}}
  .badge-false{{background:var(--red-dim);color:var(--red);border:1px solid var(--red-border)}}

  /* ── Side rail: timeline dot + mini candles ── */
  .card-rail{{display:flex;align-items:center;justify-content:flex-start;gap:10px;padding:10px 18px;
    border-top:1px solid var(--border);background:var(--surface2)}}
  .rail-label{{font-size:11px;color:var(--muted);font-weight:600;margin-right:auto}}
  .rail-dot{{width:10px;height:10px;border-radius:50%;flex-shrink:0;box-shadow:0 0 8px currentColor}}
  .rail-dot.candle-up{{background:var(--green);color:var(--green)}}
  .rail-dot.candle-down{{background:var(--red);color:var(--red)}}
  .mini-candles{{display:flex;align-items:flex-end;gap:3px;height:26px}}
  .mc{{width:4px;border-radius:2px;opacity:.55;transition:.3s}}
  .mc.candle-up{{background:var(--green)}}
  .mc.candle-down{{background:var(--red)}}
  .card:hover .mc{{opacity:1;transform:scaleY(1.15)}}

  /* ── Target highlight bar ── */
  .card-target{{display:flex;justify-content:space-between;align-items:center;
    margin:0 18px 14px;padding:14px 18px;border-radius:14px;
    background:linear-gradient(135deg,var(--blue-dim),transparent);
    border:1px solid var(--border);position:relative;overflow:hidden}}
  .card-false .card-target{{background:linear-gradient(135deg,var(--red-dim),transparent)}}
  .target-lbl{{font-size:12px;color:var(--muted);font-weight:600}}
  .target-val{{font-family:'Inter',monospace;font-size:20px;font-weight:800;color:var(--gold);letter-spacing:.5px}}

  .card-body{{padding:0 18px 18px;position:relative;z-index:1}}
  .info-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px}}
  .info-cell{{display:flex;flex-direction:column;gap:4px;padding:10px 12px;background:var(--surface2);border-radius:10px;border:1px solid var(--border)}}
  .lbl{{font-size:11px;color:var(--muted);font-weight:500}}
  .val{{font-size:13px;color:var(--text);font-weight:600}}
  .val.highlight{{color:var(--blue);font-weight:800}}
  .false-detail{{margin-top:10px;padding:10px 14px;background:var(--red-dim);border-radius:10px;border:1px solid var(--red-border);display:flex;flex-wrap:wrap;gap:8px;align-items:center;font-size:12px;color:var(--red);font-weight:500}}
  .false-detail .reason{{font-style:italic}}
  .false-history{{flex-direction:column;gap:4px}}
  .hist-entry{{display:block;font-size:11px;color:var(--red);padding:2px 0}}
  .false-action{{margin-top:10px;display:flex;gap:8px;align-items:center}}
  .false-reason-inp{{flex:1;background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:8px 10px;font-size:12px;color:var(--text);font-family:inherit}}
  .false-reason-inp:focus{{outline:none;border-color:var(--red)}}
  .false-btn{{background:var(--red-dim);color:var(--red);border:1px solid var(--red-border);border-radius:8px;padding:8px 14px;font-size:12px;font-weight:700;cursor:pointer;white-space:nowrap;transition:.15s}}
  .false-btn:hover{{background:var(--red);color:#fff}}
  .false-btn:disabled{{opacity:.5;cursor:default}}

  /* ── Empty ── */
  .empty{{text-align:center;padding:90px 20px;color:var(--muted);animation:fadeUp .5s ease;position:relative;z-index:1}}
  .empty .icon{{font-size:54px;margin-bottom:14px}}

  /* ── Footer ── */
  .footer{{text-align:center;padding:28px;font-size:11px;color:var(--subtle);position:relative;z-index:1}}

  /* ── Animations ── */
  @keyframes fadeDown{{from{{opacity:0;transform:translateY(-12px)}}to{{opacity:1;transform:translateY(0)}}}}
  @keyframes fadeUp{{from{{opacity:0;transform:translateY(12px)}}to{{opacity:1;transform:translateY(0)}}}}
  @keyframes cardIn{{to{{opacity:1;transform:translateY(0) scale(1)}}}}
</style>
</head>
<body data-theme="dark">
<div class="bg-grid"></div>
<div class="bg-orb o1"></div>
<div class="bg-orb o2"></div>
<div class="bg-orb o3"></div>
<div class="scroll-bar"><div class="scroll-bar-fill" id="scrollFill"></div></div>
<div class="scroll-dot" id="scrollDot">📈</div>
<button class="theme-toggle" id="themeToggle" onclick="toggleTheme()">🌙</button>
<a href="/report/export?from={from_value}&to={to_value}" target="_blank" rel="noopener" style="position:fixed;top:16px;left:64px;z-index:50;background:var(--surface,#1a1f2e);border:1px solid var(--border,#2a3142);border-radius:10px;padding:8px 14px;font-size:13px;color:inherit;text-decoration:none;display:flex;align-items:center;gap:6px">📥 دانلود دیتا</a>
<div class="hero">
  <h1>📋 گزارش آلارم‌های تیم</h1>
  <div class="period">{week_label}</div>
  <div class="stats">
    <div class="stat"><div class="num">{total_count}</div><div class="lbl2">کل آلارم</div></div>
    <div class="stat green"><div class="num">{active_count}</div><div class="lbl2">فعال</div></div>
    <div class="stat red"><div class="num">{false_count}</div><div class="lbl2">False شده</div></div>
  </div>
</div>
<div style="text-align:center;margin:-8px 0 14px">
  <a href="/report/export?from={from_value}&to={to_value}" target="_blank" rel="noopener" class="preset-btn" style="text-decoration:none;display:inline-block">📥 دانلود دیتا</a>
</div>
<form class="range-nav" id="rangeNav" method="get" action="/report/weekly">
  <div class="range-presets">
    <button type="button" class="preset-btn" onclick="setRange(0)">📅 این هفته</button>
    <button type="button" class="preset-btn" onclick="setRange(1)">📅 هفته قبل</button>
    <button type="button" class="preset-btn" onclick="setRange(2)">🗓️ این ماه</button>
    <button type="button" class="preset-btn" onclick="setRange(3)">🗓️ ماه قبل</button>
  </div>
  <div class="range-inputs">
    <label>از <input type="date" name="from" id="fromInput" value="{from_value}"></label>
    <label>تا <input type="date" name="to" id="toInput" value="{to_value}"></label>
    <button type="submit" class="range-go">نمایش 🔍</button>
  </div>
</form>
<div class="search-wrap">
  <input type="text" id="searchBox" class="search-input" placeholder="🔍 جستجو بر اساس مسئول، نماد، تگ یا سازنده..." oninput="filterCards()">
  <select id="filterSelect" class="search-select" onchange="filterCards()">
    <option value="">📂 همه آلارم‌ها (بدون فوری)</option>
    <optgroup label="نوع">
      <option value="kind:instant">⚡ فقط آلارم فوری</option>
      <option value="kind:all">📊 همه (معمولی + فوری)</option>
    </optgroup>
    <optgroup label="وضعیت">
      <option value="status:active">✅ فقط فعال</option>
      <option value="status:false">❌ فقط False شده</option>
    </optgroup>
    <optgroup label="False شده توسط">
      {false_by_options}
    </optgroup>
  </select>
  <span class="search-count" id="searchCount"></span>
</div>
<div class="list" id="cardList">
  {'<div class="empty"><div class="icon">📭</div>آلارمی ثبت نشده</div>' if not rows and not instant_rows else rows_html}
  <div class="empty" id="noResults" style="display:none"><div class="icon">🔍</div>چیزی پیدا نشد</div>
</div>
{pagination_html}
<div class="footer">آخرین بروزرسانی: {now_dt.strftime('%H:%M — %d/%m/%Y')}</div>
<script>
  // فالس کردن یه آلارم مستقیم از همین صفحه (با کامنت اختیاری)
  async function markFalse(aid, btnEl) {{
    const wrap = btnEl.closest('.false-action');
    const reasonInp = wrap ? wrap.querySelector('.false-reason-inp') : null;
    const reason = reasonInp ? reasonInp.value.trim() : '';
    let name = localStorage.getItem('trader_name') || '';
    let pin = localStorage.getItem('trader_pin') || '';
    if (!name) {{
      name = prompt('نام شما (همون نامی که تو سایت آلارم استفاده می‌کنید):', '') || '';
      if (!name) return;
    }}
    if (!confirm('این آلارم به عنوان False ثبت بشه؟')) return;
    btnEl.disabled = true;
    btnEl.textContent = '⏳ ...';
    try {{
      const r = await fetch(`/api/alarm-assignments/${{aid}}/false`, {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{name, pin, reason}})
      }});
      const d = await r.json();
      if (d.ok) {{
        location.reload();
      }} else {{
        alert('❌ ' + (d.error || 'خطا در ثبت'));
        btnEl.disabled = false;
        btnEl.textContent = '❌ فالس کردن';
      }}
    }} catch (e) {{
      alert('❌ خطا در ارتباط با سرور');
      btnEl.disabled = false;
      btnEl.textContent = '❌ فالس کردن';
    }}
  }}

  // miyanbor‌های بازه تاریخ
  function _fmtDate(d) {{
    const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,'0'), day = String(d.getDate()).padStart(2,'0');
    return `${{y}}-${{m}}-${{day}}`;
  }}
  function setRange(type) {{
    const now = new Date();
    let from, to;
    if (type === 0) {{ // این هفته: شنبه تا امروز
      const diffToSat = (now.getDay() - 6 + 7) % 7;
      from = new Date(now); from.setDate(now.getDate() - diffToSat);
      to = now;
    }} else if (type === 1) {{ // هفته قبل
      const diffToSat = (now.getDay() - 6 + 7) % 7;
      const thisWeekStart = new Date(now); thisWeekStart.setDate(now.getDate() - diffToSat);
      to = new Date(thisWeekStart); to.setDate(thisWeekStart.getDate() - 1);
      from = new Date(thisWeekStart); from.setDate(thisWeekStart.getDate() - 7);
    }} else if (type === 2) {{ // این ماه
      from = new Date(now.getFullYear(), now.getMonth(), 1);
      to = now;
    }} else if (type === 3) {{ // ماه قبل
      from = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      to = new Date(now.getFullYear(), now.getMonth(), 0);
    }}
    document.getElementById('fromInput').value = _fmtDate(from);
    document.getElementById('toInput').value = _fmtDate(to);
    document.getElementById('rangeNav').submit();
  }}
  // scroll progress — chart line style
  window.addEventListener('scroll', () => {{
    const h = document.documentElement;
    const pct = (h.scrollTop / (h.scrollHeight - h.clientHeight)) * 100;
    document.getElementById('scrollFill').style.width = pct + '%';
    document.getElementById('scrollDot').style.left = pct + '%';
  }});
  // stagger card animations
  document.querySelectorAll('.card').forEach((c,i) => {{
    c.style.animationDelay = (i * 0.06) + 's';
  }});
  // search/filter
  function filterCards() {{
    const q = document.getElementById('searchBox').value.trim().toLowerCase();
    const sel = document.getElementById('filterSelect').value;
    const cards = document.querySelectorAll('#cardList .card');
    let visible = 0;
    cards.forEach(c => {{
      const kind = c.dataset.kind || 'regular';
      let match = !q || (c.dataset.search || '').includes(q);
      if (sel === 'kind:instant') {{
        match = match && (kind === 'instant');
      }} else if (sel === 'kind:all') {{
        // هر دو نوع مجازن، فیلترهای دیگه (status/by) فقط رو معمولی‌ها معنی دارن
      }} else if (sel.startsWith('status:')) {{
        match = match && (kind === 'regular') && (c.dataset.status === sel.slice(7));
      }} else if (sel.startsWith('by:')) {{
        match = match && (kind === 'regular') && (c.dataset.falseby === sel.slice(3));
      }} else {{
        // پیش‌فرض (بدون هیچ فیلتری) — فوری‌ها نشون داده نشن
        match = match && (kind === 'regular');
      }}
      c.style.display = match ? '' : 'none';
      if (match) visible++;
    }});
    document.getElementById('noResults').style.display = (visible === 0) ? '' : 'none';
    document.getElementById('searchCount').textContent = (q || sel) ? `${{visible}} نتیجه پیدا شد` : '';
  }}
  // theme toggle with localStorage
  function toggleTheme() {{
    const body = document.body;
    const isDark = body.getAttribute('data-theme') === 'dark';
    body.setAttribute('data-theme', isDark ? 'light' : 'dark');
    document.getElementById('themeToggle').textContent = isDark ? '☀️' : '🌙';
    try {{ localStorage.setItem('reportTheme', isDark ? 'light' : 'dark'); }} catch(e) {{}}
  }}
  try {{
    const saved = localStorage.getItem('reportTheme');
    if (saved) {{
      document.body.setAttribute('data-theme', saved);
      document.getElementById('themeToggle').textContent = saved === 'dark' ? '🌙' : '☀️';
    }}
  }} catch(e) {{}}
  filterCards();
</script>
</body></html>"""
    return html, 200, {"Content-Type": "text/html; charset=utf-8"}

# ═══════════════════════════════════════════════════════════════════
# 📥 خروجی اکسل آلارم‌ها — بر اساس سیستم جدید (بدون مسئول/تقسیم)
# منبع اصلی: جدول alerts + رأی‌ها (alarm_ratings) + متن پیام ربات (fired_msgs)
# ═══════════════════════════════════════════════════════════════════
_EXP_G_INFO  = "اطلاعات آلارم"
_EXP_G_TIME  = "زمان"
_EXP_G_RATE  = "امتیاز و ستاپ"
_EXP_G_MEMB  = "امتیاز و کامنت هر عضو"
_EXP_G_FALSE = "False (داده‌ی قدیمی)"
_EXP_G_BOT   = "پیام ربات"
_EXP_G_MAN   = "ستون‌های خالی برای پر کردن دستی"

def _export_column_defs():
    """(key, عنوان, عرض, گروه, پیش‌فرض تیک‌خورده) — هم صفحه‌ی دانلود و هم خروجی از همین لیست می‌خونن"""
    cols = [
        ("alert_id",       "شناسه آلارم",        16, _EXP_G_INFO, False),
        ("symbol",         "نماد",               10, _EXP_G_INFO, True),
        ("direction",      "جهت",                 7, _EXP_G_INFO, True),
        ("target_price",   "قیمت ناحیه",         11, _EXP_G_INFO, True),
        ("fired_price",    "قیمت فایر",          11, _EXP_G_INFO, True),
        ("status",         "وضعیت",              12, _EXP_G_INFO, True),
        ("created_by",     "ثبت‌کننده",          12, _EXP_G_INFO, True),
        ("comment",        "کامنت ثبت‌کننده",    32, _EXP_G_INFO, True),
        ("supporters_names", "ستاره‌دهنده‌ها (تأیید ناحیه)", 20, _EXP_G_INFO, True),
        ("supporters_count", "تعداد ستاره",       9, _EXP_G_INFO, True),
        ("is_private",     "خصوصی",               7, _EXP_G_INFO, True),

        ("created_date",   "تاریخ ثبت",          11, _EXP_G_TIME, True),
        ("created_time",   "ساعت ثبت",            9, _EXP_G_TIME, True),
        ("fired_date",     "تاریخ فایر",         11, _EXP_G_TIME, True),
        ("fired_time",     "ساعت فایر",           9, _EXP_G_TIME, True),
        ("duration_hours", "مدت تا فایر (ساعت)", 12, _EXP_G_TIME, True),
        ("expired_date",   "تاریخ انقضا",        11, _EXP_G_TIME, False),
        ("expired_time",   "ساعت انقضا",          9, _EXP_G_TIME, False),

        ("score_total",    "جمع امتیاز نهایی",   12, _EXP_G_RATE, True),
        ("setup_type",     "نوع ستاپ",           20, _EXP_G_RATE, True),
        ("voters_count",   "تعداد رأی‌دهنده",    10, _EXP_G_RATE, True),
        ("rating_status",  "وضعیت رأی‌گیری",     12, _EXP_G_RATE, True),
        ("votes_text",     "نظر و امتیاز اعضا (یکجا)", 48, _EXP_G_RATE, True),
    ]
    for m in TEAM_MEMBERS:
        cols.append((f"score_{m}", f"امتیاز {m}", 9, _EXP_G_MEMB, True))
    for m in TEAM_MEMBERS:
        cols.append((f"cmt_{m}", f"کامنت {m}", 28, _EXP_G_MEMB, False))
    cols += [
        ("false_reason",   "علت False",          22, _EXP_G_FALSE, True),
        ("false_date",     "تاریخ False",        11, _EXP_G_FALSE, True),
        ("false_time",     "ساعت False",          9, _EXP_G_FALSE, True),

        ("bot_text",       "متن کامل پیام ربات", 70, _EXP_G_BOT, True),

        ("manual_reaction", "واکنش قیمت (دستی)", 20, _EXP_G_MAN, True),
        ("manual_result",   "نتیجه / R (دستی)",  16, _EXP_G_MAN, True),
        ("manual_note",     "یادداشت (دستی)",    26, _EXP_G_MAN, True),
    ]
    return cols

def _exp_get_all(path, params, page=1000):
    """GET صفحه‌به‌صفحه از Supabase (سقف ۱۰۰۰ ردیف در هر درخواست). اگه خطا بده Exception می‌ده —
    دیگه بی‌صدا خروجی خالی نمی‌ده."""
    params = list(params)
    if not any(k == "order" for k, _ in params):
        params.append(("order", "id.asc"))
    out, off = [], 0
    while True:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{path}",
                         params=params + [("limit", str(page)), ("offset", str(off))],
                         headers=_sb_h(), timeout=25)
        if r.status_code != 200:
            raise RuntimeError(f"{path}: HTTP {r.status_code} — {r.text[:200]}")
        batch = r.json() or []
        out.extend(batch)
        if len(batch) < page:
            break
        off += page
    return out

def _exp_in_list(values):
    """in.("a","b") با کوتیشن، تا اسم فارسی/فاصله/کاما مشکل نسازه"""
    return "in.(" + ",".join('"' + str(v).replace('"', '') + '"' for v in values) + ")"

@app.route("/report/export")
def report_export_html():
    """صفحه‌ی دانلود دیتا — بازه‌ی تاریخ + فیلترها + انتخاب ستون‌ها + دکمه‌ی خروجی اکسل"""
    today_str = datetime.now(TEHRAN).strftime("%Y-%m-%d")
    month_ago_str = (datetime.now(TEHRAN) - timedelta(days=30)).strftime("%Y-%m-%d")
    default_from = request.args.get("from", "") or month_ago_str
    default_to = request.args.get("to", "") or today_str
    col_rows, cur_grp = "", None
    for key, label, _w, grp, dflt in _export_column_defs():
        if grp != cur_grp:
            col_rows += f'<div class="col-grp">{grp}</div>'
            cur_grp = grp
        col_rows += (f'<label><input type="checkbox" class="colChk" value="{key}"'
                     f'{" checked" if dflt else ""}> {label}</label>')
    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>دانلود دیتا — گزارش آلارم‌ها</title>
<style>
  :root{{--bg:#070a12;--surface:#0d1424;--surface2:#10192e;--border:#1e293b;--border2:#2d3f5c;
    --text:#e2e8f0;--muted:#64748b;--blue:#3b82f6;--green:#22c55e;--red:#ef4444}}
  *{{box-sizing:border-box}}
  body{{font-family:'Inter',Tahoma,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;
    direction:rtl;margin:0;padding:24px 16px;display:flex;justify-content:center}}
  .card{{max-width:560px;width:100%;background:var(--surface);border:1px solid var(--border2);
    border-radius:18px;padding:26px 22px}}
  h1{{font-size:19px;margin:0 0 4px;font-weight:800}}
  .sub{{font-size:12px;color:var(--muted);margin-bottom:22px}}
  .fld{{margin-bottom:16px}}
  label{{display:block;font-size:12px;color:var(--muted);font-weight:600;margin-bottom:6px}}
  input[type=date],input[type=text]{{width:100%;padding:10px 12px;border-radius:10px;
    border:1px solid var(--border2);background:var(--surface2);color:var(--text);font-size:13px;
    font-family:inherit;outline:none}}
  input:focus{{border-color:var(--blue)}}
  .row2{{display:flex;gap:10px}}
  .row2 .fld{{flex:1}}
  .chk-group{{display:flex;flex-wrap:wrap;gap:10px;background:var(--surface2);
    border:1px solid var(--border2);border-radius:10px;padding:12px 14px}}
  .chk-row{{display:flex;align-items:center;gap:6px;font-size:13px;margin:0;font-weight:400;color:var(--text)}}
  .chk-row input{{width:16px;height:16px}}
  .dl-btn{{width:100%;padding:13px;border:none;border-radius:12px;background:linear-gradient(135deg,var(--blue),#2563eb);
    color:#fff;font-size:14px;font-weight:800;cursor:pointer;margin-top:8px;transition:.2s}}
  .dl-btn:hover{{filter:brightness(1.1)}}
  .hint{{font-size:11px;color:var(--muted);margin-top:6px;line-height:1.7}}
  a.back{{color:var(--blue);font-size:12px;text-decoration:none;display:inline-block;margin-bottom:14px}}
  .modal-overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:100;
    align-items:center;justify-content:center;padding:16px}}
  .modal-overlay.show{{display:flex}}
  .modal-box{{max-width:420px;width:100%;max-height:85vh;overflow-y:auto;background:var(--surface);
    border:1px solid var(--border2);border-radius:16px;padding:22px}}
  .modal-box h2{{font-size:16px;margin:0 0 4px;font-weight:800}}
  .modal-sub{{font-size:11px;color:var(--muted);margin-bottom:14px}}
  .col-actions{{display:flex;gap:8px;margin-bottom:12px}}
  .col-actions button{{flex:1;padding:7px;border-radius:8px;border:1px solid var(--border2);
    background:var(--surface2);color:var(--text);font-size:11px;cursor:pointer;font-family:inherit}}
  .col-list{{display:flex;flex-direction:column;gap:2px;background:var(--surface2);
    border:1px solid var(--border2);border-radius:10px;padding:8px 12px;margin-bottom:16px}}
  .col-list label{{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:400;
    color:var(--text);margin:0;padding:6px 0}}
  .col-list input{{width:16px;height:16px}}
  .col-grp{{font-size:11px;font-weight:800;color:var(--blue);margin:10px 0 2px;padding-top:8px;
    border-top:1px solid var(--border2)}}
  .col-grp:first-child{{border-top:none;margin-top:0;padding-top:0}}
  .modal-btns{{display:flex;gap:8px}}
  .modal-btns button{{flex:1;padding:11px;border-radius:10px;border:none;font-size:13px;font-weight:700;
    cursor:pointer;font-family:inherit}}
  .btn-cancel{{background:var(--surface2);color:var(--text);border:1px solid var(--border2) !important}}
  .btn-confirm{{background:linear-gradient(135deg,var(--blue),#2563eb);color:#fff}}
</style>
</head>
<body>
<div class="card">
  <a class="back" href="/report/weekly">→ برگشت به گزارش هفتگی</a>
  <h1>📥 دانلود دیتا</h1>
  <div class="sub">یه بازه و فیلتر انتخاب کن، فایل اکسل (xlsx) دانلود کن.</div>

  <div class="row2">
    <div class="fld"><label>از تاریخ</label><input type="date" id="expFrom" value="{default_from}"></div>
    <div class="fld"><label>تا تاریخ</label><input type="date" id="expTo" value="{default_to}"></div>
  </div>

  <div class="fld">
    <label>وضعیت (خالی = فایرشده + False شده + منقضی‌شده)</label>
    <div class="chk-group">
      <label class="chk-row"><input type="checkbox" value="fired" class="statusChk"> ✅ فایرشده</label>
      <label class="chk-row"><input type="checkbox" value="false" class="statusChk"> ❌ False شده</label>
      <label class="chk-row"><input type="checkbox" value="expired" class="statusChk"> ⌛ منقضی‌شده</label>
      <label class="chk-row"><input type="checkbox" value="active" class="statusChk"> 🟢 فعال (هنوز فایر نشده)</label>
      <label class="chk-row"><input type="checkbox" value="instant" class="statusChk"> ⚡ فوری</label>
    </div>
  </div>

  <div class="row2">
    <div class="fld">
      <label>ثبت‌کننده (اختیاری — چندتا با کاما)</label>
      <input type="text" id="expCreator" placeholder="مثلاً مسعود, علی">
    </div>
    <div class="fld">
      <label>نماد/ارز (اختیاری — چندتا با کاما)</label>
      <input type="text" id="expSymbol" placeholder="مثلاً XAUUSD, EURUSD" dir="ltr">
    </div>
  </div>

  <button class="dl-btn" id="dlBtn" onclick="openColumnModal()">📥 دانلود اکسل</button>
  <div class="hint">بازه‌ی تاریخ برای فایرشده/False/فوری بر اساس تاریخ فایر، برای منقضی‌شده بر اساس تاریخ انقضا و برای «فعال» بر اساس تاریخ ثبت اعمال می‌شه. فیلترها با هم AND می‌شن.<br>
  امتیاز، نوع ستاپ (A / B / پرریسک)، کامنت هر عضو، ستاره‌ها و متن کامل پیام ربات از سیستم جدید خونده می‌شن. ستون‌های «False (داده‌ی قدیمی)» فقط برای آلارم‌های قدیمی پر می‌شن.</div>
</div>

<div class="modal-overlay" id="columnModal">
  <div class="modal-box">
    <h2>ستون‌های اکسل</h2>
    <div class="modal-sub">هر ستونی رو نمی‌خوای تو خروجی باشه، تیکش رو بردار.</div>
    <div class="col-actions">
      <button type="button" onclick="setAllColumns(true)">✅ انتخاب همه</button>
      <button type="button" onclick="setAllColumns(false)">❌ پاک کردن همه</button>
    </div>
    <div class="col-list">
      {col_rows}
    </div>
    <div class="modal-btns">
      <button class="btn-cancel" onclick="closeColumnModal()">انصراف</button>
      <button class="btn-confirm" onclick="doExport()">📥 دانلود</button>
    </div>
  </div>
</div>
<script>
function openColumnModal() {{
  document.getElementById('columnModal').classList.add('show');
}}
function closeColumnModal() {{
  document.getElementById('columnModal').classList.remove('show');
}}
function setAllColumns(val) {{
  document.querySelectorAll('.colChk').forEach(c => c.checked = val);
}}
function doExport() {{
  const from = document.getElementById('expFrom').value;
  const to = document.getElementById('expTo').value;
  const statuses = Array.from(document.querySelectorAll('.statusChk:checked')).map(c => c.value).join(',');
  const creator = document.getElementById('expCreator').value.trim();
  const symbol = document.getElementById('expSymbol').value.trim();
  const columns = Array.from(document.querySelectorAll('.colChk:checked')).map(c => c.value).join(',');
  if (!columns) {{ alert('حداقل یک ستون رو تیک بزن.'); return; }}
  const params = new URLSearchParams();
  if (from) params.set('from', from);
  if (to) params.set('to', to);
  if (statuses) params.set('statuses', statuses);
  if (creator) params.set('creator', creator);
  if (symbol) params.set('symbol', symbol);
  params.set('columns', columns);
  window.location.href = '/api/report/export.xlsx?' + params.toString();
  closeColumnModal();
}}
</script>
</body></html>"""
    return html, 200, {"Content-Type": "text/html; charset=utf-8"}


@app.route("/api/report/export.xlsx")
def report_export_xlsx():
    """
    خروجی اکسل آلارم‌ها بر اساس سیستم جدید:
    آلارم از جدول alerts + رأی‌ها از alarm_ratings + متن پیام ربات و وضعیت لغو از fired_msgs.
    مسئول/تقسیم کنسل شده و دیگه نقشی نداره. هر خطای Supabase به‌جای فایل خالی، به‌صورت پیام خطا برمی‌گرده.
    """
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment, PatternFill
        from openpyxl.utils import get_column_letter
    except ImportError:
        return jsonify({"ok": False, "error": "پکیج openpyxl روی سرور نصب نیست — باید به requirements.txt اضافه بشه."}), 500
    import io, html as html_lib

    if not SUPABASE_KEY:
        return jsonify({"ok": False, "error": "Supabase تنظیم نشده"}), 500

    from_str = request.args.get("from", "")
    to_str = request.args.get("to", "")
    statuses = [s.strip() for s in request.args.get("statuses", "").split(",") if s.strip()]
    creators = [c.strip() for c in request.args.get("creator", "").split(",") if c.strip()]
    symbols = [s.strip().upper() for s in request.args.get("symbol", "").split(",") if s.strip()]

    want_all = not statuses
    want_fired = want_all or "fired" in statuses
    want_false = want_all or "false" in statuses
    want_expired = want_all or "expired" in statuses
    want_active = "active" in statuses     # آلارم هنوز فایرنشده — فقط با تیک صریح
    want_instant = "instant" in statuses   # فوری — فقط با تیک صریح

    # تاریخ به‌صورت YYYY-MM-DD فیلتر می‌شه (هم برای ستون متنی و هم timestamp درست کار می‌کنه)
    now_n = datetime.now(TEHRAN).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
    def _parse_d(s, default):
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except Exception:
            return default
    gte = _parse_d(from_str, now_n - timedelta(days=30)).strftime("%Y-%m-%d")
    lt = (_parse_d(to_str, now_n) + timedelta(days=1)).strftime("%Y-%m-%d")

    flt = [("id", "neq.__config__")]
    if creators:
        flt.append(("created_by", _exp_in_list(creators)))
    if symbols:
        flt.append(("symbol", _exp_in_list(symbols)))

    rows = {}
    try:
        def _add(batch):
            for x in batch:
                rows.setdefault(str(x.get("id", "")), x)
        if want_fired or want_false:
            _add(_exp_get_all("alerts", [("select", "*"), ("status", "eq.fired"),
                                         ("fired_at", "gte." + gte), ("fired_at", "lt." + lt)] + flt))
        if want_expired:
            _add(_exp_get_all("alerts", [("select", "*"), ("status", "eq.expired"),
                                         ("expired_at", "gte." + gte), ("expired_at", "lt." + lt)] + flt))
        if want_active:
            _add(_exp_get_all("alerts", [("select", "*"), ("status", "eq.active"),
                                         ("created_at", "gte." + gte), ("created_at", "lt." + lt)] + flt))
        if want_instant:
            _add(_exp_get_all("alerts", [("select", "*"), ("instant", "eq.true"),
                                         ("fired_at", "gte." + gte), ("fired_at", "lt." + lt)] + flt))
    except Exception as e:
        return jsonify({"ok": False, "error": f"خطا در خوندن آلارم‌ها از Supabase: {e}"}), 500

    ids = [i for i in rows.keys() if i]
    votes_by, mm_by, asg_by = {}, {}, {}
    try:
        for i in range(0, len(ids), 100):
            inl = "in.(" + ",".join(ids[i:i + 100]) + ")"
            for v in _exp_get_all("alarm_ratings", [("select", "*"), ("alert_id", inl)]):
                votes_by.setdefault(str(v.get("alert_id", "")), []).append(v)
            for f in _exp_get_all("fired_msgs", [("select", "id,msg_map"), ("id", inl)]):
                raw = f.get("msg_map") or {}
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except Exception:
                        raw = {}
                mm_by[str(f.get("id", ""))] = raw if isinstance(raw, dict) else {}
    except Exception as e:
        return jsonify({"ok": False, "error": f"خطا در خوندن امتیازها/پیام‌ها از Supabase: {e}"}), 500
    for vs in votes_by.values():
        vs.sort(key=lambda v: str(v.get("voted_at") or ""))
    # داده‌ی قدیمی False (قبل از کنسل شدن سیستم مسئول) — اختیاری؛ خطاش نباید خروجی رو خراب کنه
    try:
        for i in range(0, len(ids), 100):
            inl = "in.(" + ",".join(ids[i:i + 100]) + ")"
            for x in _exp_get_all("alarm_assignments",
                                  [("select", "id,is_active,false_at,false_reason"), ("id", inl)]):
                asg_by[str(x.get("id", ""))] = x
    except Exception as e:
        print(f"[export] alarm_assignments (قدیمی) خونده نشد، ادامه می‌دیم: {e}")

    def _split_dt(s):
        s = (s or "").strip().replace("T", " ")
        if not s:
            return "", ""
        p = s.split(" ", 1)
        return (p[0], p[1][:8]) if len(p) == 2 else (p[0], "")

    def _hours_between(a_s, b_s):
        try:
            fmt = "%Y-%m-%d %H:%M:%S"
            a_d = datetime.strptime((a_s or "").replace("T", " ")[:19], fmt)
            b_d = datetime.strptime((b_s or "").replace("T", " ")[:19], fmt)
            return round((b_d - a_d).total_seconds() / 3600.0, 1)
        except Exception:
            return ""

    _tier_fa = {"setup_a": "Setup A", "setup_b": "Setup B", "risky": "پرریسک", "no_votes": "بدون رأی"}

    records = []
    for aid, a in rows.items():
        mm = mm_by.get(aid, {})
        asg = asg_by.get(aid, {})
        votes = votes_by.get(aid, [])
        st = a.get("status", "")
        cancelled = (mm.get("__rating_cancelled__") == "1") or (asg.get("is_active") is False)
        is_instant = bool(a.get("instant"))

        if is_instant:
            if not want_instant:
                continue
            status_lbl = "⚡ فوری"
        elif st == "fired":
            if cancelled:
                if not want_false:
                    continue
                status_lbl = "False شده"
            else:
                if not want_fired:
                    continue
                status_lbl = "فایرشده"
        elif st == "expired":
            if not want_expired:
                continue
            status_lbl = "منقضی‌شده"
        elif st == "active":
            if not want_active:
                continue
            status_lbl = "فعال (هنوز فایر نشده)"
        else:
            continue

        # ── امتیاز و ستاپ ──
        creator_res = _resolve_creator_name(mm.get("__creator__") or a.get("created_by", ""))
        forced = mm.get("__rating_manual__") in ("1", "auto")
        state = _compute_rating_state(votes, creator_res, force_closed=forced)
        state_now = _compute_rating_state(votes, creator_res, force_closed=True)
        had_rating = bool(votes) or bool(mm.get("__text_rated__")) or ("__creator__" in mm)
        if cancelled:
            setup_txt, rating_status = "لغو (False)", "لغو"
        elif not had_rating:
            setup_txt, rating_status = "رأی‌گیری نداشته", "—"
        elif state["closed"]:
            setup_txt, rating_status = _tier_fa.get(state["label"], state["label"]), "بسته"
        else:
            setup_txt = f"در حال رأی‌گیری (فعلاً {_tier_fa.get(state_now['label'], '')})"
            rating_status = "باز"
        score_total = state_now["total"] if votes else ""

        by_member, lines = {}, []
        for v in state_now["votes"]:
            nm = (v.get("voter_name") or "").strip()
            member = next((m for m in TEAM_MEMBERS if _norm_name(m) == _norm_name(nm)), None) \
                or TEAM_ID_TO_NAME.get(str(v.get("voter_cid") or ""))
            sc, why = v.get("score", ""), (v.get("reason") or "").strip()
            if member:
                by_member[member] = (sc, why)
            lines.append(f"{nm or member or '—'}: {sc}/5" + (f" — {why}" if why else ""))

        # ── ستاره‌ها ──
        sup_names = []
        for su in (a.get("supporters") or []):
            if isinstance(su, dict) and su.get("name"):
                sup_names.append(str(su["name"]))
            elif isinstance(su, str) and su.strip():
                sup_names.append(su.strip())

        # ── متن کامل پیام ربات (بدون بخش امتیازدهی، بدون تگ HTML) ──
        bot_txt = _strip_rating_section(mm.get("__text__") or mm.get("__text_rated__") or "")
        bot_txt = html_lib.unescape(re.sub(r"<[^>]+>", "", bot_txt)).strip()

        created_d, created_t = _split_dt(a.get("created_at", ""))
        fired_d, fired_t = _split_dt(a.get("fired_at", ""))
        expired_d, expired_t = _split_dt(a.get("expired_at", ""))
        false_d, false_t = _split_dt(asg.get("false_at", ""))

        vals = {
            "alert_id": aid,
            "symbol": a.get("symbol", ""),
            "direction": "بای" if a.get("condition") == "below" else "سل",
            "target_price": a.get("target_price", ""),
            "fired_price": a.get("fired_price", "") or "",
            "status": status_lbl,
            "created_by": a.get("created_by", ""),
            "comment": a.get("comment", "") or "",
            "supporters_names": "، ".join(sup_names),
            "supporters_count": len(sup_names) if sup_names else 0,
            "is_private": "بله" if a.get("is_private") else "",
            "created_date": created_d, "created_time": created_t,
            "fired_date": fired_d, "fired_time": fired_t,
            "duration_hours": _hours_between(a.get("created_at"), a.get("fired_at")) if a.get("fired_at") else "",
            "expired_date": expired_d, "expired_time": expired_t,
            "score_total": score_total, "setup_type": setup_txt,
            "voters_count": state_now["voters_count"] if votes else 0,
            "rating_status": rating_status,
            "votes_text": "\n".join(lines),
            "false_reason": asg.get("false_reason", "") or "",
            "false_date": false_d, "false_time": false_t,
            "bot_text": bot_txt[:30000],
            "manual_reaction": "", "manual_result": "", "manual_note": "",
        }
        for m in TEAM_MEMBERS:
            sc_why = by_member.get(m)
            vals[f"score_{m}"] = sc_why[0] if sc_why else ""
            vals[f"cmt_{m}"] = sc_why[1] if sc_why else ""
        sort_key = a.get("fired_at") or a.get("expired_at") or a.get("created_at") or ""
        records.append((str(sort_key).replace("T", " "), a, vals, setup_txt, score_total, status_lbl))

    records.sort(key=lambda r: r[0])

    # ── انتخاب ستون‌ها ──
    col_defs = _export_column_defs()
    requested = [c.strip() for c in request.args.get("columns", "").split(",") if c.strip()]
    if requested:
        selected = [d for d in col_defs if d[0] in requested]
    else:
        selected = [d for d in col_defs if d[4]]
    if not selected:
        selected = [d for d in col_defs if d[4]]
    keys = [d[0] for d in selected]

    _bad_ch = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
    def _cell(v):
        return _bad_ch.sub("", v) if isinstance(v, str) else v

    wb = Workbook()
    ws = wb.active
    ws.title = "آلارم‌ها"
    ws.sheet_view.rightToLeft = False
    ws.append([d[1] for d in selected])
    head_fill = PatternFill("solid", fgColor="D9D9D9")
    for c in ws[1]:
        c.font = Font(bold=True)
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.fill = head_fill
    ws.row_dimensions[1].height = 32
    ws.freeze_panes = "A2"

    buy_fill = PatternFill("solid", fgColor="D9F2E3")
    sell_fill = PatternFill("solid", fgColor="FBDEDE")
    zebra_fill = PatternFill("solid", fgColor="F5F5F5")
    setup_fills = {"Setup A": PatternFill("solid", fgColor="BFE8CC"),
                   "Setup B": PatternFill("solid", fgColor="CFE2FA"),
                   "پرریسک": PatternFill("solid", fgColor="F8CFCF")}
    dir_i = keys.index("direction") + 1 if "direction" in keys else None
    setup_i = keys.index("setup_type") + 1 if "setup_type" in keys else None
    score_i = keys.index("score_total") + 1 if "score_total" in keys else None
    wrap_keys = {"comment", "votes_text", "bot_text", "supporters_names", "false_reason", "manual_note",
                 "manual_reaction"} | {f"cmt_{m}" for m in TEAM_MEMBERS}

    for _sk, a, vals, setup_txt, score_total, status_lbl in records:
        ws.append([_cell(vals[k]) for k in keys])
        r = ws.max_row
        for ci, k in enumerate(keys, start=1):
            cell = ws.cell(row=r, column=ci)
            cell.alignment = Alignment(vertical="top", wrap_text=(k in wrap_keys))
        if (r % 2) == 0:
            for cell in ws[r]:
                cell.fill = zebra_fill
        if dir_i:
            ws.cell(row=r, column=dir_i).fill = buy_fill if vals["direction"] == "بای" else sell_fill
        fill_setup = next((f for k2, f in setup_fills.items() if setup_txt.startswith(k2)), None)
        if fill_setup:
            if setup_i:
                ws.cell(row=r, column=setup_i).fill = fill_setup
            if score_i:
                ws.cell(row=r, column=score_i).fill = fill_setup

    for i, d in enumerate(selected, start=1):
        ws.column_dimensions[get_column_letter(i)].width = d[2]
    if records:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(selected))}{ws.max_row}"

    # ── شیت خلاصه ──
    ws2 = wb.create_sheet("خلاصه")
    ws2.append(["بازه", f"{gte} تا {(_parse_d(to_str, now_n)).strftime('%Y-%m-%d')}"])
    ws2.append(["تعداد کل ردیف‌ها", len(records)])
    ws2.append([])
    ws2.append(["وضعیت", "تعداد"])
    cnt_status = {}
    for _sk, a, vals, setup_txt, score_total, status_lbl in records:
        cnt_status[status_lbl] = cnt_status.get(status_lbl, 0) + 1
    for k2, n2 in cnt_status.items():
        ws2.append([k2, n2])
    ws2.append([])
    ws2.append(["نوع ستاپ", "تعداد"])
    cnt_setup = {}
    for _sk, a, vals, setup_txt, score_total, status_lbl in records:
        cnt_setup[setup_txt] = cnt_setup.get(setup_txt, 0) + 1
    for k2, n2 in sorted(cnt_setup.items(), key=lambda x: -x[1]):
        ws2.append([k2, n2])
    ws2.append([])
    ws2.append(["ثبت‌کننده", "تعداد", "میانگین امتیاز", "Setup A", "Setup B", "پرریسک"])
    per_c = {}
    for _sk, a, vals, setup_txt, score_total, status_lbl in records:
        d2 = per_c.setdefault(a.get("created_by", "") or "—", {"n": 0, "sum": 0, "rated": 0, "A": 0, "B": 0, "R": 0})
        d2["n"] += 1
        if score_total != "":
            d2["sum"] += score_total
            d2["rated"] += 1
        if setup_txt.startswith("Setup A"): d2["A"] += 1
        elif setup_txt.startswith("Setup B"): d2["B"] += 1
        elif setup_txt.startswith("پرریسک"): d2["R"] += 1
    for nm2, d2 in sorted(per_c.items(), key=lambda x: -x[1]["n"]):
        ws2.append([_cell(nm2), d2["n"], round(d2["sum"] / d2["rated"], 1) if d2["rated"] else "",
                    d2["A"], d2["B"], d2["R"]])
    ws2.column_dimensions["A"].width = 26
    for col_l in "BCDEF":
        ws2.column_dimensions[col_l].width = 14
    for rr in ws2.iter_rows():
        if rr and rr[0].value in ("وضعیت", "نوع ستاپ", "ثبت‌کننده"):
            for c in rr:
                c.font = Font(bold=True)
                c.fill = head_fill

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    fname = f"alarms_export_{from_str or 'all'}_{to_str or 'all'}.xlsx"
    return buf.read(), 200, {
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Content-Disposition": f'attachment; filename="{fname}"'
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"[STARTUP] Flask روی پورت {port} اجرا میشه")
    app.run(host="0.0.0.0", port=port, debug=False)
# .
