import os, re, json, time, html, queue, threading, requests, random
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify, send_from_directory

SB_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SB_KEY = os.environ.get("SUPABASE_KEY", "")
BOT = os.environ.get("BOT_TOKEN", "")
# شناسه‌ی چت اعضا (همان TEAM_MEMBER_IDS ربات). ربات به پیوی هر عضو پیام می‌دهد.
MEMBER_IDS = json.loads(os.environ["MEMBER_IDS"]) if os.environ.get("MEMBER_IDS") else {
    'مسعود': '109419675', 'پیمان': '5089282711', 'علی': '138116289',
    'اتابک': '349639401', 'مهران': '117005892', 'زرداری': '90178485'}
RELAY_KEY = os.environ.get("RELAY_KEY", "")
RELAY_FN = os.environ.get("RELAY_FUNCTION", "tg-relay")   # اسم فانکشن در Supabase
# Hugging Face دسترسی به api.telegram.org را می‌بندد؛ با RELAY_KEY پیام‌ها از طریق Edge Function سوپابیس (tg-relay) می‌روند
# و توکن‌های تلگرام فقط توی Secretهای Supabase می‌مانند، نه روی Hugging Face.
NOTIFY_CID = MEMBER_IDS.get("علی", "")   # همان RATING_RESULT_NOTIFY_CID ربات
GOLD = os.environ.get("GOLD_BOT_TOKEN", "")
GOLD_CHATS = [c.strip() for c in os.environ.get("GOLD_CHAT_IDS", "").split(",") if c.strip()]
# حالت رله: توکن‌های واقعی فقط توی Supabase هستند و این‌جا فقط نشانه‌ی «کدام ربات» می‌ماند (باید بعد از خواندن GOLD بیاید)
if RELAY_KEY:
    BOT, GOLD = "main", "gold"
SITE = os.environ.get("SITE_URL", "")
MEMBERS = [m.strip() for m in os.environ.get("TEAM_MEMBERS", "اتابک,مهران,علی,پیمان,مسعود,زرداری").split(",") if m.strip()]
CHATS = [c.strip() for c in os.environ.get("GROUP_CHAT_IDS", "").split(",") if c.strip()] or \
        [str(MEMBER_IDS[m]) for m in MEMBERS if m in MEMBER_IDS]

app = Flask(__name__, static_folder="static")
lock = threading.RLock()
Z = {}          # همه نواحی (کش حافظه؛ منبع اصلی Supabase)
Q = queue.Queue()  # صف ارسال تلگرام (ترتیب پیام‌ها حفظ می‌شه)
AWAY = {}       # مرخصی/غیبت اعضا {نام: {"from": ms, "until": ms, "by": نام}}؛ در Supabase با ردیف id=__away__ ذخیره می‌شود

# ───────── Supabase ─────────
def _h():
    return {"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY, "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"}

def load_all():
    off = 0
    while True:
        r = requests.get(f"{SB_URL}/rest/v1/zones", headers=_h(), timeout=20,
                         params={"select": "id,data", "order": "id.asc", "limit": 1000, "offset": off})
        r.raise_for_status()
        rows = r.json()
        for x in rows:
            if x["id"] == "__away__":
                AWAY.update(x["data"] or {})
                continue
            Z[x["id"]] = x["data"]
        if len(rows) < 1000:
            break
        off += 1000

def save(z):
    try:
        r = requests.post(f"{SB_URL}/rest/v1/zones", headers=_h(), timeout=15, json={"id": z["id"], "data": z})
        if r.status_code >= 300:
            print("save error", r.status_code, r.text[:200])
    except Exception as e:
        print("save exc", e)

def save_away():
    try:
        r = requests.post(f"{SB_URL}/rest/v1/zones", headers=_h(), timeout=15, json={"id": "__away__", "data": AWAY})
        if r.status_code >= 300:
            print("save_away error", r.status_code, r.text[:200])
    except Exception as e:
        print("save_away exc", e)

# ───────── منطق امتیاز: آستانه‌ها بر اساس تعداد اعضای واجد رأی ─────────
# تعداد اعضا: (حداقل امتیاز A, حداقل امتیاز B)؛ با ۵ نفر همان ۱۵ و ۱۰ قبلی
TH = {5: (15, 10), 4: (14, 9), 3: (12, 8), 2: (9, 6), 1: (5, 3)}
def thr(mx): return TH.get(mx, (15, 10))

def tier(t, mx=5):
    a, b = thr(mx)
    return "A" if t >= a else "B" if t >= b else "R"

def is_away(m, at):
    a = AWAY.get(m)
    return bool(a) and a["from"] <= at < a["until"]

def skip_live(z, at):
    """اعضایی که در لحظه‌ی at مرخصی‌اند و رأی نداده‌اند (فقط برای نواحی نسخه‌ی ۲؛ نواحی قدیمی مثل قبل حساب می‌شوند)"""
    if z.get("v") != 2: return []
    return [m for m in MEMBERS if m != z["by"] and m not in z["votes"] and is_away(m, at)]

def skip_of(z):
    if z.get("v") != 2: return []
    return z["skip"] if "skip" in z else skip_live(z, int(time.time() * 1000))

def expired(z):
    """مهلت رأی‌گیری (دقیقه، ۰ = نامحدود) از لحظه‌ی ثبت ناحیه؛ موقع هر درخواست محاسبه می‌شود نه با تایمر"""
    d = int(z.get("dur") or 0)
    return bool(d) and int(time.time() * 1000) > z["t"] + d * 60000

def watch_of(z, closed, label):
    """وضعیت بررسی قیمت: hit=به ناحیه رسیده، inst=فوری (بدون بررسی قیمت)، wait=ستاپ A/B تأییدشده و منتظر رسیدن قیمت.
    closed در اینجا یعنی «تأییدشده» (بسته‌شدن رأی‌گیری یا رسیدن امتیاز فعلی به A/B).
    ناحیه‌ی False‌شده هیچ‌وقت wait نیست: از تب منتظر بیرون می‌آید و قیمتش دیگر بررسی نمی‌شود.
    فقط نواحی جدید (w=1) بررسی می‌شوند تا نواحی قدیمی بعد از دیپلوی هشدار الکی نزنند."""
    if z.get("hit"): return "hit"
    if z.get("inst"): return "inst"
    if z.get("w") and not z.get("false") and closed and label in ("A", "B"): return "wait"
    return None

def state(z):
    sk = skip_of(z)
    mx = max(1, len([m for m in MEMBERS if m != z["by"]]) - len(sk))
    total = sum(v["s"] for v in z["votes"].values())
    n = len(z["votes"])
    rem = max(0, mx - n)
    is_expired = expired(z)
    # با پایان مهلت، رأی جدیدی قابل ثبت نیست؛ بنابراین امتیاز فعلی نتیجه نهایی است.
    # حتی اگر هنوز همه اعضا رأی نداده باشند، برچسب باید A/B/R باشد نه P.
    closed = is_expired or rem == 0 or (n > 0 and tier(total, mx) == tier(total + rem * 5, mx)) \
        or bool(z.get("v") == 2 and z.get("closed_at"))
    a, b = thr(mx)
    label = tier(total, mx) if closed else ("N" if n == 0 else "P")
    # سطح «فعلی» (موقت) با امتیازهای ثبت‌شده تا همین لحظه؛ تا رأی‌گیری باز است ممکن است عوض شود
    lv = tier(total, mx) if n > 0 else "N"
    # بررسی قیمت از همان لحظه‌ای شروع می‌شود که ستاپ A/B تأیید شد (پیام ربات طلا رفت)، نه فقط بعد از بسته شدن رأی‌گیری.
    # اگر امتیاز دوباره به پرریسک برگردد، lv=R می‌شود و ناحیه خودکار از بررسی/تب «منتظر» بیرون می‌آید.
    watch = watch_of(z, closed or lv in ("A", "B"), label if closed else lv)
    return {"total": total, "n": n, "max": mx, "closed": closed, "expired": is_expired, "a": a, "b": b, "skip": sk,
            "label": label, "lv": lv, "watch": watch}

def freeze(z):
    """وقتی ناحیه‌ی نسخه‌ی ۲ بسته یا False شد، فهرست غایب‌ها ثابت می‌شود تا نتیجه با تغییر مرخصی‌ها عوض نشود"""
    if z.get("v") != 2 or "skip" in z: return
    if not (state(z)["closed"] or z.get("false")): return
    now = int(time.time() * 1000)
    at = now
    if z.get("false"): at = min(at, z["false"]["t"])
    elif expired(z): at = min(at, z["t"] + int(z.get("dur") or 0) * 60000)
    z["skip"] = skip_live(z, at)
    save(z)

TN = {"A": "🟢 Setup A", "B": "🔵 Setup B", "R": "🔴 پرریسک", "P": "⏳ در حال جمع‌آوری رأی", "N": "⏳ هنوز رأیی ثبت نشده"}
esc = lambda s: html.escape(str(s or ""))
side = lambda z: "بای" if z["dir"] == "buy" else "سل"

def zlink(z, text="🗳 رأی بده — همین ناحیه را در سایت باز کن"):
    """لینک مستقیم به همین ناحیه: سایت با ?z=<id> ناحیه را بالا می‌آورد و هایلایت می‌کند"""
    return f'<a href="{esc(SITE.rstrip("/"))}/?z={z["id"]}">{text}</a>' if SITE else ""

def tag(z): return f"#Z{z['no']}" if z.get("no") else ""

def head(z):
    t = f"📍 <b>ناحیه ثبت شد</b> {tag(z)}\n<b>{esc(z['sym'])}</b> {side(z)} در <code>{esc(z['price'])}</code>\n👤 ثبت‌کننده: <b>{esc(z['by'])}</b>"
    if z.get("note"): t += f"\n💬 {esc(z['note'])}"
    if z.get("link") and z["link"] != "#": t += f"\n🔗 {esc(z['link'])}"
    d = int(z.get("dur") or 0)
    t += "\n⏱ مهلت رأی: " + (f"{d} دقیقه" if 0 < d < 60 else f"{d // 60} ساعت" if d else "نامحدود")
    if z.get("inst"): t += "\n⚡ فوری — قیمت الان داخل ناحیه است"
    if SITE: t += "\n" + zlink(z)
    return t

def section(z):
    st = state(z)
    L = ["", "────────────", "⭐ <b>امتیازدهی تیم</b>", f"مجموع: <b>{st['total']}</b>  ·  {st['n']}/{st['max']} رأی"]
    if z.get("false"):
        L.append("❌ <b>لغو شد (False)</b>" + (f"\nدلیل: {esc(z['false']['reason'])}" if z["false"].get("reason") else ""))
    else:
        L.append(TN[st["label"]])
    for k, v in z["votes"].items():
        L.append(f"• <b>{esc(k)}</b>: {v['s']}/5" + (f" — <i>{esc(v['c'])}</i>" if v.get("c") else ""))
    return "\n".join(L)

# ───────── تلگرام ─────────
def tg_raw(token, method, p):
    """درخواست به تلگرام (مستقیم یا از طریق رله‌ی Supabase). خروجی: (status_code, متن پاسخ)"""
    if RELAY_KEY:
        r = requests.post(f"{SB_URL}/functions/v1/{RELAY_FN}", timeout=20,
                          headers={"Authorization": "Bearer " + SB_KEY, "x-relay-key": RELAY_KEY},
                          json={"bot": "gold" if token == GOLD else "main", "method": method, "params": p})
    else:
        r = requests.post(f"https://api.telegram.org/bot{token}/{method}", json=p, timeout=10)
    return r.status_code, r.text

def tg(token, method, **p):
    try:
        code, text = tg_raw(token, method, p)
        try: j = json.loads(text)
        except Exception: j = {}
        if code >= 300 or not j.get("ok", False):
            print("tg FAIL", method, "chat", p.get("chat_id"), "HTTP", code, text[:300])   # قبلاً خطاهای دروازه ساکت رد می‌شد
        return j.get("result")
    except Exception as e:
        print("tg", method, e)

def worker():
    while True:
        fn = Q.get()
        try: fn()
        except Exception as e: print("queue", e)
threading.Thread(target=worker, daemon=True).start()

def gold_targets():
    """گیرنده‌های سیگنال نهایی: همه‌ی اعضای تیم (MEMBER_IDS) + مشترک‌های ربات طلا از ردیف config سوپابیس + علی + GOLD_CHAT_IDS اختیاری"""
    t = set(GOLD_CHATS)
    t |= {str(MEMBER_IDS[m]) for m in MEMBERS if m in MEMBER_IDS}   # قبلاً فقط مشترک‌های ربات طلا می‌گرفتند و بقیه‌ی اعضا چیزی نمی‌دیدند
    if NOTIFY_CID: t.add(str(NOTIFY_CID))
    try:
        r = requests.get(f"{SB_URL}/rest/v1/alerts", headers=_h(), timeout=8,
                         params={"id": "eq.__config__", "select": "gold_bot_subscribers"})
        rows = r.json() if r.status_code == 200 else []
        v = rows[0].get("gold_bot_subscribers") if rows else None
        if isinstance(v, str): v = json.loads(v)
        t |= {str(x) for x in (v or [])}
    except Exception as e:
        print("gold_targets", e)
    return sorted(t)

def snap(z): return json.loads(json.dumps(z))

def gold_send(p):
    """ارسال با ربات طلا. True=رسید، False=تلگرام صریحاً رد کرد، None=نامعلوم (تایم‌اوت/خطای شبکه/پاسخ نامفهوم)"""
    try:
        code, text = tg_raw(GOLD, "sendMessage", p)
    except Exception as e:
        print("gold_send exc", e); return None
    try: j = json.loads(text)
    except Exception: j = {}
    if code < 300 and j.get("ok"): return True
    print("gold_send FAIL chat", p.get("chat_id"), "HTTP", code, text[:200])
    if code in (400, 403) or j.get("error_code") in (400, 403): return False
    return None

def send_final(txt, targets=None):
    """ارسال سیگنال نهایی به همه‌ی گیرنده‌ها: اول از ربات طلا؛ اگر نرسید (مثلاً عضو هنوز ربات طلا را استارت نکرده) از ربات اصلی"""
    for c in (targets if targets is not None else gold_targets()):
        p = dict(chat_id=c, text=txt, parse_mode="HTML", disable_web_page_preview=True)
        r = gold_send(p)
        if r is None:                       # نامعلوم (تایم‌اوت/خطای شبکه): ممکن است رسیده باشد؛ یک بار دیگر با همان ربات طلا
            time.sleep(2)
            r = gold_send(p)
        if r is False:                      # فقط وقتی تلگرام صریحاً رد کرد (مثلاً عضو ربات طلا را استارت نکرده) → ربات اصلی
            tg(BOT, "sendMessage", **p)
        elif r is None:
            print("send_final UNKNOWN (ربات اصلی صدا زده نشد تا پیام دوبار نرود) chat", c)

def q_announce(z):
    s = snap(z)
    def run():
        if not (BOT and CHATS): return
        ids = {}
        for c in CHATS:
            res = tg(BOT, "sendMessage", chat_id=c, text=head(s), parse_mode="HTML", disable_web_page_preview=True)
            if res: ids[c] = res["message_id"]
        with lock:
            Z[s["id"]]["tg"] = ids
            save(Z[s["id"]])
    Q.put(run)

def q_refresh(z):
    s = snap(z)
    def run():
        for c, mid in (s.get("tg") or {}).items():
            tg(BOT, "editMessageText", chat_id=c, message_id=mid, text=head(s) + section(s), parse_mode="HTML", disable_web_page_preview=True)
    Q.put(run)

def result_text(z, title):
    st = state(z)
    L = [f"{title} {tag(z)}".strip(), f"<b>{esc(z['sym'])}</b> {side(z)} در {esc(z['price'])}", TN[st["label"]],
         f"مجموع امتیاز: <b>{st['total']}</b> از {st['max'] * 5}  ·  {st['n']}/{st['max']} رأی", ""]
    L += [f"• {esc(k)}: {v['s']}/5" + (f" — {esc(v['c'])}" if v.get("c") else "") for k, v in z["votes"].items()]
    L += ["", f"👤 ثبت‌کننده: {esc(z['by'])}"]
    if SITE: L.append(zlink(z, "🌐 مشاهده‌ی ناحیه در سایت"))
    return "\n".join(L)

def final_text(z):
    """پیام سیگنال نهایی A/B برای ربات طلا: کامل و بدون هیچ دعوت به رأی‌دادن (رأی‌گیری بسته شده)"""
    st = state(z)
    L = [f"✅ <b>سیگنال نهایی</b>  ·  {TN[st['label']]}  {tag(z)}".rstrip(), "",
         f"<b>{esc(z['sym'])}</b> {side(z)} در <code>{esc(z['price'])}</code>",
         f"👤 ثبت‌کننده: <b>{esc(z['by'])}</b>"]
    if z.get("note"): L.append(f"💬 {esc(z['note'])}")
    if z.get("link") and z["link"] != "#": L.append(f"🔗 {esc(z['link'])}")
    L += ["", f"⭐ مجموع امتیاز: <b>{st['total']}</b> از {st['max'] * 5}  ·  {st['n']}/{st['max']} رأی"]
    L += [f"• <b>{esc(k)}</b>: {v['s']}/5" + (f" — {esc(v['c'])}" if v.get("c") else "") for k, v in z["votes"].items()]
    if z.get("inst"): L += ["", "⚡ فوری — قیمت هم‌اکنون داخل ناحیه است"]
    elif z.get("w"): L += ["", "⏳ منتظر رسیدن قیمت به ناحیه؛ با رسیدن، هشدار جدید ارسال می‌شود"]
    if SITE: L += ["", zlink(z, "🌐 مشاهده‌ی ناحیه در سایت")]
    return "\n".join(L)

def q_tier(z, up):
    """رسیدن به B قبل از بسته شدن رأی‌گیری → اعلام در ربات طلا با زمان باقی‌مانده (A همیشه رأی‌گیری را می‌بندد و q_result اعلامش می‌کند)"""
    s = snap(z); st = state(s)
    # پیام B باید مستقل و کامل باشد؛ حتی اگر رأی‌گیری هنوز باز است.
    txt = [f"🔵 <b>فعلاً ستاپ B تأیید شد</b> {tag(s)}".rstrip(),
           f"<b>{esc(s['sym'])}</b> {side(s)} در <code>{esc(s['price'])}</code>",
           f"مجموع امتیاز: <b>{st['total']}</b> از {st['max'] * 5}  ·  {st['n']}/{st['max']} رأی",
           f"👤 ثبت‌کننده ناحیه: <b>{esc(s['by'])}</b>"]
    if s.get("note"):
        txt.append(f"💬 توضیحات ثبت‌کننده: {esc(s['note'])}")
    txt.append("\n<b>جزئیات رأی‌دهندگان:</b>")
    if s.get("votes"):
        txt.extend(f"• <b>{esc(k)}</b>: {v['s']}/5" + (f" — {esc(v['c'])}" if v.get('c') else "") for k, v in s['votes'].items())
    else:
        txt.append("هنوز رأیی ثبت نشده است")
    if s.get("dur"):
        left = (s["t"] + s["dur"] * 60000 - int(time.time() * 1000)) // 60000
        txt.append(f"⏱ {max(1, left)} دقیقه تا پایان مهلت")
    else:
        txt.append("⏱ مهلت رأی‌گیری: نامحدود")
    if s.get("w") and not s.get("inst"):
        txt.append("⏳ منتظر رسیدن قیمت به ناحیه — از همین لحظه قیمت بررسی می‌شود؛ با رسیدن، هشدار جدید می‌آید")
    if SITE: txt.append(zlink(s, "🌐 مشاهده‌ی ناحیه در سایت"))
    txt = "\n".join(txt)
    def run():
        if GOLD: send_final(txt)
    Q.put(run)

def q_result(z):
    """حد نصاب رسید: A/B فقط ربات طلا (با کل اطلاعات آلارم)، پرریسک فقط یک پیام کوتاه تو گروه."""
    s = snap(z); st = state(s); txt = result_text(s, "✅ <b>نتیجه امتیازدهی</b>")
    def run():
        gt = gold_targets() if st["label"] in ("A", "B") and GOLD else []
        if gt:
            send_final(final_text(s), gt)
            return
        for c, mid in (s.get("tg") or {}).items():
            tg(BOT, "sendMessage", chat_id=c, text=txt, parse_mode="HTML", reply_to_message_id=mid)
    Q.put(run)

def q_false(z):
    s = snap(z); st = state(s)
    note = "" if st["closed"] else f"\n⚠️ قبل از رسیدن به حد نصاب: {st['n']} از {st['max']} رأی، مجموع {st['total']}"
    txt = result_text(s, "❌ <b>ناحیه False شد</b>") + (f"\nدلیل: {esc(s['false'].get('reason'))}" if s["false"].get("reason") else "") + note + f"\n🔒 توسط: {esc(s['false']['by'])}"
    def run():
        for c, mid in (s.get("tg") or {}).items():
            tg(BOT, "sendMessage", chat_id=c, text=txt, parse_mode="HTML", reply_to_message_id=mid)
    Q.put(run)

def fmt_dur(mins):
    mins = int(round(mins)); d, r = divmod(mins, 1440); h, m = divmod(r, 60)
    return " و ".join(x for x in (f"{d} روز" if d else "", f"{h} ساعت" if h else "", f"{m} دقیقه" if m else "") if x) or "کمتر از یک دقیقه"

AWAY_MSGS = [
    "🌴 <b>{n}</b> {f}با اجازه‌ی همه‌ی شما می‌رود دریا نفس بکشد؛ <b>{d}</b> نیست ({w})",
    "✈️ <b>{n}</b> {f}بار سفر می‌بندد؛ <b>{d}</b> از ناحیه‌ها و رأی‌گیری معاف است ({w})",
    "🏖 اطلاعیه: <b>{n}</b> {f}برای <b>{d}</b> از دسترس خارج است؛ بدون او غصه نخورید ({w})",
    "🚪 <b>{n}</b> {f}عقب‌نشینی تاکتیکی می‌کند؛ <b>{d}</b> نیست ({w})",
]

def q_away(name, by, mins, start, until):
    """اعلام مرخصی/غیبت یک عضو به گروه (همان ربات اصلی و همان گیرنده‌های اعلام ناحیه)"""
    tz = timezone(timedelta(hours=3, minutes=30))
    days = ["دوشنبه", "سه‌شنبه", "چهارشنبه", "پنجشنبه", "جمعه", "شنبه", "یکشنبه"]
    now = int(time.time() * 1000)
    def when(ts):
        dt = datetime.fromtimestamp(ts / 1000, tz)
        return f"{dt:%H:%M}" if abs(ts - now) < 20 * 3600000 else f"{days[dt.weekday()]} ساعت {dt:%H:%M}"
    f = f"از {when(start)} " if start > now + 60000 else ""
    txt = random.choice(AWAY_MSGS).format(n=esc(name), f=f, d=fmt_dur(mins), w="تا " + when(until))
    if by != name: txt += f"\nثبت‌شده توسط {esc(by)}"
    def run():
        if not (BOT and CHATS): return
        for c in CHATS:
            tg(BOT, "sendMessage", chat_id=c, text=txt, parse_mode="HTML", disable_web_page_preview=True)
    Q.put(run)

BACK_MSGS = [
    "🎖 برگشت غیورمندانه‌ی <b>{n}</b> از مرخصی را به اتاق تیمارستان تبریک می‌گوییم",
    "🫡 <b>{n}</b> رسماً به جبهه‌ی ناحیه‌ها بازگشت؛ بازار بیچاره دوباره امنیت ندارد",
    "🔔 مرخصی <b>{n}</b> تمام شد؛ استراحت کافی بود، چارت‌ها منتظرند",
    "🏴‍☠️ <b>{n}</b> از غیبت برگشت؛ هر کس رأی نداده بداند که او هم می‌بیند",
]

def q_back(name, by=None, early=False):
    """اعلام برگشت عضو از مرخصی (تمام‌شدن مدت یا برگشتن/لغو زودتر از موعد)"""
    txt = random.choice(BACK_MSGS).format(n=esc(name))
    if early:
        txt += "\n(مرخصی را زودتر از موعد تمام کرد" + (f"، توسط {esc(by)} برگردانده شد" if by and by != name else "") + ")"
    def run():
        if not (BOT and CHATS): return
        for c in CHATS:
            tg(BOT, "sendMessage", chat_id=c, text=txt, parse_mode="HTML", disable_web_page_preview=True)
    Q.put(run)

# ───────── بررسی قیمت ناحیه‌ها (همان ساز و کار آلارم‌های نسخه‌ی قبلی Railway) ─────────
# ستاپ A/B که تأیید شد → «منتظر ناحیه»؛ وقتی قیمت به ناحیه رسید → هشدار جدید با ربات طلا. ناحیه‌ی «فوری» اصلاً بررسی قیمت نمی‌شود.
H = {"User-Agent": "Mozilla/5.0 (compatible; PriceBot/1.0)"}
TEH = timezone(timedelta(hours=3, minutes=30))
WATCH_EVERY = int(os.environ.get("WATCH_EVERY", "60"))   # ثانیه
_last_known = {}
PX = {}   # آخرین قیمت/فاصله‌ی نواحیِ منتظر (فقط حافظه؛ برای نمایش در سایت)
_DG = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩٫٬", "01234567890123456789.,")

def nums(s): return re.findall(r"\d+(?:\.\d+)?", str(s or "").translate(_DG).replace(",", ""))
def to_f(s):
    n = nums(s)
    return float(n[0]) if len(n) == 1 else None      # فقط یک عدد؛ بازه/متن قابل بررسی نیست

def now_label(): return datetime.now(TEH).strftime("%Y-%m-%d %H:%M")

def sym_key(s): return str(s).upper().replace("/", "").replace(" ", "")

def atype_of(sym):
    return "forex" if any(x in sym.upper() for x in ["EUR", "GBP", "JPY", "XAU", "XAG", "CHF", "CAD", "AUD", "NZD"]) else "crypto"

def is_forex_market_open():
    now_utc = datetime.utcnow(); wd = now_utc.weekday()
    if wd == 5: return False
    if wd == 6: return now_utc.hour >= 21
    return True

def get_forex_prices_batch(symbols):
    if not symbols: return {}
    clean = [s.upper().replace("/", "").replace(" ", "") for s in symbols]
    qs = "&".join(f"symbols={s}" for s in clean)
    try:
        r = requests.get(f"https://biquote.io/api/latest?{qs}", timeout=12, headers=H)
        r.raise_for_status()
        raw = r.json(); result = {}
        if isinstance(raw, list):
            for item in raw:
                sym = item.get("symbol", "").upper().replace("/", "")
                bid = item.get("bid") or item.get("price") or item.get("last")
                if sym and bid and float(bid) > 0:
                    result[sym] = float(bid)
                    _last_known[sym] = {"price": float(bid), "ts": now_label(), "stale": False}
        elif isinstance(raw, dict):
            for sym, data in raw.items():
                if isinstance(data, dict): bid = data.get("bid") or data.get("price") or data.get("last")
                elif isinstance(data, (int, float)): bid = data
                else: bid = None
                if bid and float(bid) > 0:
                    result[sym.upper()] = float(bid)
                    _last_known[sym.upper()] = {"price": float(bid), "ts": now_label(), "stale": False}
        if result: return result
    except Exception: pass
    result = {}
    for sym in clean:
        if sym in _last_known:
            _last_known[sym]["stale"] = True
            result[sym] = _last_known[sym]["price"]
        else:
            try:
                base, quote = sym[:3], sym[3:6]
                r3 = requests.get(f"https://api.frankfurter.app/latest?from={base}&to={quote}", timeout=7)
                if r3.ok:
                    rate = r3.json().get("rates", {}).get(quote)
                    if rate:
                        result[sym] = float(rate)
                        _last_known[sym] = {"price": float(rate), "ts": now_label(), "stale": False}
            except Exception: pass
    return result

def get_crypto_price(symbol):
    base = symbol.upper()
    for s in ["USDT", "USDC", "USD", "BUSD"]: base = base.replace(s, "")
    base = base.replace("/", "").strip()
    try:
        r = requests.get(f"https://biquote.io/api/latest?symbols={base}USD", timeout=8, headers=H)
        if r.ok:
            raw = r.json(); bid = None
            if isinstance(raw, list) and raw: bid = raw[0].get("bid") or raw[0].get("price") or raw[0].get("last")
            elif isinstance(raw, dict): bid = raw.get("bid") or raw.get("price") or raw.get("last")
            if bid and float(bid) > 100: return float(bid)
    except Exception: pass
    for fn in (lambda: float(requests.get(f"https://www.okx.com/api/v5/market/ticker?instId={base}-USDT", headers=H, timeout=8).json()["data"][0]["last"]),
               lambda: float(requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={base}USDT", headers=H, timeout=8).json()["price"])):
        try:
            p = fn()
            if p and p > 0: return float(p)
        except Exception: pass
    print("crypto price failed", symbol)
    return None

def fmt_price(p, sym=""):
    if p is None: return "—"
    v = float(p); su = sym.upper()
    if "XAU" in su or "XAG" in su: return f"${v:.2f}"
    if "JPY" in su: return f"{v:.3f}"
    return f"{v:.5f}"

def calc_dist_str(symbol, atype, cur, tgt):
    if not cur or not tgt: return ""
    diff = abs(float(cur) - float(tgt)); su = symbol.upper()
    if atype == "crypto": return f"{diff / float(tgt) * 100:.2f}%"
    if "XAU" in su or "XAG" in su: return f"{diff:.2f} $"
    if "JPY" in su: return f"{round(diff * 100):,} pip"
    return f"{round(diff * 10000):,} pip"

def notify_gold(txt):
    """هشدار رسیدن به ناحیه: ربات طلا برای همه‌ی گیرنده‌ها؛ اگر GOLD_BOT_TOKEN نبود ربات اصلی در گروه"""
    if GOLD: send_final(txt)
    elif BOT:
        for c in CHATS: tg(BOT, "sendMessage", chat_id=c, text=txt, parse_mode="HTML", disable_web_page_preview=True)

def hit_text(z):
    st = state(z); h = z["hit"]
    arrow = "📈 ناحیه سل" if z["dir"] == "sell" else "📉 ناحیه بای"
    dist = calc_dist_str(z["sym"], atype_of(z["sym"]), h["p"], to_f(z["price"]))
    L = [f"🚨 <b>رسید به ناحیه!</b> {tag(z)}".rstrip(), f"💰 <b>{esc(z['sym'])}</b> — {arrow}", "",
         f"🎯 ناحیه: <code>{esc(z['price'])}</code>", f"📊 قیمت لحظه‌ای: <b>{esc(h['f'])}</b>"]
    if dist: L.append(f"📏 فاصله: <b>{dist}</b>")
    lab = TN[st["label"]] if st["closed"] else TN[st["lv"]] + " (فعلاً؛ رأی‌گیری هنوز باز است)"
    L += ["", f"{lab}  ·  مجموع {st['total']} از {st['max'] * 5}  ·  {st['n']}/{st['max']} رأی",
          f"👤 ثبت‌کننده: <b>{esc(z['by'])}</b>"]
    if z.get("note"): L.append(f"💬 {esc(z['note'])}")
    if z.get("link") and z["link"] != "#": L.append(f"🔗 {esc(z['link'])}")
    L.append(f"⏰ {now_label()} (تهران)")
    if SITE: L += ["", zlink(z, "🌐 مشاهده‌ی ناحیه در سایت")]
    return "\n".join(L)

def q_hit(z):
    s = snap(z)
    Q.put(lambda: notify_gold(hit_text(s)))

def watch_once():
    with lock:
        items = [(z["id"], z["sym"]) for z in Z.values() if state(z)["watch"] == "wait"]
    if not items: return
    fx = sorted({sym_key(s) for _, s in items if atype_of(s) == "forex"}) if is_forex_market_open() else []
    cr = sorted({sym_key(s) for _, s in items if atype_of(s) == "crypto"})
    prices = {}
    if fx:
        for k, p in (get_forex_prices_batch(fx) or {}).items():
            if not (_last_known.get(k) or {}).get("stale"): prices[(k, "forex")] = p     # قیمت کهنه (آخرین مقدار ذخیره‌شده) هشدار نمی‌سازد
    for k in cr:
        p = get_crypto_price(k)
        if p: prices[(k, "crypto")] = p
    now = int(time.time() * 1000); hits = []
    with lock:
        for zid, sym in items:
            z = Z.get(zid)
            if not z or state(z)["watch"] != "wait": continue          # در این فاصله False شده یا تغییر کرده
            at = atype_of(sym); cur = prices.get((sym_key(sym), at)); tgt = to_f(z["price"])
            if cur is None or tgt is None: continue
            PX[zid] = {"p": fmt_price(cur, sym), "d": calc_dist_str(sym, at, cur, tgt), "t": now}
            if (z["dir"] == "sell" and cur >= tgt) or (z["dir"] == "buy" and cur <= tgt):
                z["hit"] = {"t": now, "p": cur, "f": fmt_price(cur, sym)}
                save(z)                                                  # اول ذخیره، بعد ارسال: با ری‌استارت هشدار دوباره نمی‌رود
                hits.append(snap(z))
    for z in hits: q_hit(z)

def _watch_loop():
    time.sleep(20)
    while True:
        try: watch_once()
        except Exception as e: print("watch exc", e)
        time.sleep(WATCH_EVERY)
threading.Thread(target=_watch_loop, daemon=True).start()

# ───────── API ─────────
def err(m, c=400): return jsonify({"ok": False, "error": m}), c

@app.get("/api/state")
def api_state():
    with lock:
        zs = sorted(Z.values(), key=lambda z: z["t"], reverse=True)
        for z in zs: freeze(z)
        now = int(time.time() * 1000)
        away = {m: {"until": a["until"], "from": a["from"], "by": a.get("by", "")} for m, a in AWAY.items()
                if m in MEMBERS and now < a["until"]}   # شامل مرخصی‌های زمان‌بندی‌شده‌ی آینده (from > now)
        zl = []
        for z in zs:
            d2 = dict(snap(z), st=state(z))
            if d2["st"]["watch"] == "wait" and z["id"] in PX: d2["px"] = PX[z["id"]]
            zl.append(d2)
        return jsonify({"members": MEMBERS, "away": away, "zones": zl})

def next_no():
    """شماره‌ی ناحیه: ترتیبی و یکتا (در پیام‌های تلگرام به‌صورت هشتگ #Z12 می‌آید تا بعداً قابل جستجو باشد)"""
    return max([int(z.get("no") or 0) for z in Z.values()] + [0]) + 1

def backfill_numbers():
    """نواحی قدیمی بدون شماره، به ترتیب زمان ثبت شماره می‌گیرند (فقط یک بار)"""
    old = sorted((z for z in Z.values() if not z.get("no")), key=lambda z: z["t"])
    if not old: return
    n = next_no()
    for z in old:
        z["no"] = n; n += 1
        save(z)

@app.post("/api/zone")
def api_zone():
    d = request.get_json(force=True) or {}
    by, sym, price = d.get("by"), (d.get("sym") or "").strip().upper(), (d.get("price") or "").strip()
    if by not in MEMBERS: return err("نام عضو معتبر نیست")
    if not sym or not price: return err("نماد و قیمت لازم است")
    inst = bool(d.get("inst"))      # فوری: قیمت الان داخل ناحیه است → بررسی قیمت ندارد
    if not inst and to_f(price) is None: return err("قیمت ناحیه باید فقط یک عدد باشد (اگر قیمت الان داخل ناحیه است «فوری» را بزنید)")
    z = {"id": str(int(time.time() * 1000)), "sym": sym[:20], "dir": "sell" if d.get("dir") == "sell" else "buy",
         "price": price[:20], "link": (d.get("link") or "#").strip()[:500] or "#", "note": (d.get("note") or "").strip()[:500],
         "by": by, "t": int(time.time() * 1000), "votes": {}, "tg": {},
         "dur": d.get("dur") if d.get("dur") in (15, 60, 240) else 0, "v": 2,
         "w": 0 if inst else 1, **({"inst": True} if inst else {})}
    with lock:
        z["no"] = next_no()          # داخل همان lock تا دو ناحیه هم‌زمان شماره‌ی تکراری نگیرند
        Z[z["id"]] = z; save(z)
    q_announce(z)
    return jsonify({"ok": True})

@app.post("/api/vote")
def api_vote():
    d = request.get_json(force=True) or {}
    with lock:
        z = Z.get(d.get("id")); by = d.get("by")
        if not z: return err("ناحیه پیدا نشد", 404)
        if by not in MEMBERS or by == z["by"]: return err("این عضو نمی‌تواند رأی بدهد")
        if z.get("false"): return err("این ناحیه False شده است")
        if state(z)["closed"]: return err("رأی‌گیری این ناحیه بسته شده است", 409)
        if expired(z): return err("مهلت رأی‌گیری این ناحیه تمام شده است", 409)
        try: s = int(d.get("score"))
        except Exception: return err("امتیاز نامعتبر")
        if not 0 <= s <= 5: return err("امتیاز باید ۰ تا ۵ باشد")
        now = int(time.time() * 1000)
        old = z["votes"].get(by)
        # t = زمان «اولین» ثبت رأی (برای سرعت رأی)؛ e = زمان آخرین ویرایش
        z["votes"][by] = {"s": s, "c": (d.get("reason") or "").strip()[:300],
                          "t": old.get("t", now) if old else now, **({"e": now} if old else {})}
        st = state(z); closed = st["closed"]
        # رسیدن به B (قبل از بسته شدن): اعلام در ربات طلا؛ رسیدن به A با q_result اعلام می‌شود
        lv = tier(st["total"], st["max"])
        up = lv if lv in ("A", "B") and z.get("notified") != lv else None
        z["notified"] = lv if lv != "R" else None   # افتادن به پرریسک → دفعه‌ی بعد B دوباره اعلام می‌شود
        if closed:
            z["closed_at"] = int(time.time() * 1000)
            if z.get("v") == 2 and "skip" not in z: z["skip"] = skip_live(z, z["closed_at"])
        save(z)
    if up and not closed: q_tier(z, up)   # A همیشه closed است و q_result اعلامش می‌کند
    if closed: q_result(z)
    return jsonify({"ok": True})

@app.post("/api/false")
def api_false():
    d = request.get_json(force=True) or {}
    with lock:
        z = Z.get(d.get("id"))
        if not z: return err("ناحیه پیدا نشد", 404)
        if d.get("by") not in MEMBERS: return err("نام عضو معتبر نیست")
        if z.get("false"): return err("قبلاً False شده", 409)
        z["false"] = {"by": d["by"], "reason": (d.get("reason") or "").strip()[:300], "t": int(time.time() * 1000)}
        PX.pop(z["id"], None)         # از بررسی قیمت و تب «منتظر» بیرون می‌آید (state: false → watch=None)
        save(z); freeze(z)
    # پیام False دیگر به تلگرام نمی‌رود (q_false عمداً صدا زده نمی‌شود)
    return jsonify({"ok": True})

def recheck_open(now):
    """نواحی بازِ نسخه‌ی ۲ که با تغییر اعضای واجد رأی (شروع مرخصی و...) به حد نصاب رسیده‌اند را می‌بندد؛ باید زیر lock صدا زده شود"""
    out = []
    for z in Z.values():
        if z.get("v") != 2 or z.get("false") or z.get("closed_at") or expired(z): continue
        if state(z)["closed"]:
            z["closed_at"] = now
            z["skip"] = skip_live(z, now)
            save(z); out.append(snap(z))
    return out

def _recheck_loop():
    # مرخصی‌های زمان‌بندی‌شده بدون رویداد شروع می‌شوند؛ هر ۳۰ ثانیه بررسی می‌شود تا نتیجه‌ی ناحیه‌ی بسته‌شده اعلام شود
    while True:
        time.sleep(30)
        try:
            now = int(time.time() * 1000); backs = []
            with lock:
                closed_now = recheck_open(now)
                changed = False
                for m, a in AWAY.items():
                    if a["until"] <= now and not a.get("back"):
                        a["back"] = True; changed = True
                        if now - a["until"] < 10 * 60000: backs.append(m)   # اگر خیلی دیر (مثلاً بعد از ری‌استارت) پیام قدیمی نده
                if changed: save_away()
            for z in closed_now: q_result(z)
            for m in backs: q_back(m)
        except Exception as e:
            print("recheck exc", e)
threading.Thread(target=_recheck_loop, daemon=True).start()

@app.post("/api/away")
def api_away():
    """مرخصی/غیبت: {by, name, minutes, start?}؛ start (میلی‌ثانیه) خالی = از همین الان، یا زمان آینده برای مرخصی زمان‌بندی‌شده؛
    minutes خالی یا ۰ = لغو/برگشت به حالت حاضر. هر عضو می‌تواند وضعیت هر عضو را عوض کند."""
    d = request.get_json(force=True) or {}
    by, name = d.get("by"), d.get("name")
    if by not in MEMBERS or name not in MEMBERS: return err("نام عضو معتبر نیست")
    try: mins = float(d.get("minutes") or 0)
    except Exception: return err("مدت نامعتبر")
    if mins < 0 or mins > 60 * 24 * 30: return err("مدت باید بین ۱ دقیقه تا ۳۰ روز باشد")
    now = int(time.time() * 1000)
    try: start = int(d.get("start") or now)
    except Exception: return err("زمان شروع نامعتبر")
    if start < now: start = now
    if start > now + 30 * 86400000: return err("شروع مرخصی حداکثر ۳۰ روز بعد می‌تواند باشد")
    with lock:
        old = AWAY.get(name)
        was_active = bool(old) and old["from"] <= now < old["until"]
        if mins < 1: AWAY.pop(name, None)
        else: AWAY[name] = {"from": start, "until": start + int(mins * 60000), "by": by}
        save_away()
        closed_now = recheck_open(now)
    for z in closed_now: q_result(z)
    if mins >= 1: q_away(name, by, mins, start, start + int(mins * 60000))
    elif was_active: q_back(name, by, early=True)
    return jsonify({"ok": True})

@app.get("/api/tgtest")
def api_tgtest():
    """تست مستقیم ارسال: /api/tgtest?key=<RELAY_KEY>&chat=<chat_id اختیاری>  — پاسخ خام رله/تلگرام رو نشون می‌ده"""
    if (request.args.get("key") or "") != (RELAY_KEY or BOT or "-"):
        return err("forbidden", 403)
    cid = request.args.get("chat") or NOTIFY_CID
    try:
        code, text = tg_raw(BOT, "sendMessage", {"chat_id": cid, "text": "✅ تست ارسال از تابلوی نواحی"})
        return jsonify({"chat": cid, "relay_mode": bool(RELAY_KEY), "http": code, "body": text[:600]})
    except Exception as e:
        return jsonify({"chat": cid, "relay_mode": bool(RELAY_KEY), "exception": str(e)}), 500

@app.get("/")
def index(): return send_from_directory("static", "index.html")

try:
    if SB_URL and SB_KEY: load_all(); backfill_numbers()
    else: print("⚠️ SUPABASE_URL / SUPABASE_KEY تنظیم نشده")
except Exception as e:
    print("load_all failed:", e)

if __name__ == "__main__":
    # Space با SDK گریدیو: فایل app.py اجرا می‌شود و باید روی پورت ۷۸۶۰ سرور بالا بیاید
    from waitress import serve
    serve(app, host="0.0.0.0", port=int(os.environ.get("PORT", 7860)), threads=8)
