import os
import re
import sys
import json
import time
import threading
import datetime
import subprocess
import requests
import telebot

# Auto-install optional packages that may be missing in the deployed image
try:
    import bs4  # noqa: F401
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "beautifulsoup4"])
    import bs4  # noqa: F401

TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
if not TOKEN:
    raise SystemExit("❌ Переменная окружения TELEGRAM_BOT_TOKEN не задана.\n"
                     "   Добавьте её в настройках хостинга (Variables / Secrets).")

bot = telebot.TeleBot(TOKEN)

# belarusborder.by — electronic queue API (real-time, operated by Beltamozhservice)
BTS_API = "https://belarusborder.by/info/monitoring-new"
BTS_TOKEN = "test"
BREST_CHECKPOINT_ID = "a9173a85-3fc0-424c-84f0-defa632481e4"

_DATA_DIR      = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
SETTINGS_FILE  = os.path.join(_DATA_DIR, "settings.json")
HISTORY_FILE   = os.path.join(_DATA_DIR, "queue_history.json")
ALLOWED_INTERVALS = {1: "1 минута", 5: "5 минут", 15: "15 минут"}
TICK = 60  # main loop ticks every 60 s; per-user interval is checked individually
HISTORY_MAX_AGE    = 48 * 3600   # keep 48 h of readings
PLANNER_ALERT_BUFFER = 0.5       # warn 30 min before calculated register time

CAR_MILESTONES = [300, 200, 100, 50, 25, 10]  # notify at each of these positions
_CAR_REG_FIELDS = ("regnum", "regNumber", "carNumber", "registrationNumber",
                   "number", "plate", "vehicleNumber", "autoNumber",
                   "gosNomer", "gosNumber", "nomer", "avtoNomer",
                   "regNomer", "carNomer", "номер", "гос_номер",
                   "reg", "vehicle", "car", "auto")
MAX_TRACKED_CARS = 5

MONTHS_RU = ["января","февраля","марта","апреля","мая","июня",
             "июля","августа","сентября","октября","ноября","декабря"]

def fmt_dt(dt: datetime.datetime) -> str:
    return f"{dt.day} {MONTHS_RU[dt.month - 1]} в {dt.strftime('%H:%M')}"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'ru-RU,ru',
}

# ── Settings persistence ────────────────────────────────────────────────────
# {chat_id (str): {"threshold": int, "enabled": bool, "last_notified": int}}

def load_settings() -> dict:
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_settings(data: dict):
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


settings: dict = load_settings()
settings_lock = threading.Lock()


DEFAULT_SETTINGS = {
    "threshold": 50,
    "enabled": False,
    "interval": 5,
    "last_checked": 0,
    "step": 50,             # notify when queue grows by this many cars
    "step_baseline": None,  # queue value when last step-alert fired (None = not set yet)
    "threshold_notified": False,  # True while queue is above threshold (prevents repeat alerts)
    "paused_until": 0,    # unix timestamp; 0 = not paused
    "planner_target": None,    # "YYYY-MM-DD HH:MM" or None
    "planner_notified": False, # True once the "register now" alert was sent
    "notification_msg_ids": [], # message IDs of sent notifications (for cleanup)
    "tracked_cars": {},         # {normalized_reg: {"milestones_done": [], "called_notified": False}}
}

MAX_NOTIFICATIONS = 5  # keep only last N notification messages per chat

PAUSE_OPTIONS = [
    (1,  "1 час"),
    (2,  "2 часа"),
    (4,  "4 часа"),
    (8,  "8 часов"),
    (24, "24 часа"),
]


def get_chat_settings(chat_id: str) -> dict:
    with settings_lock:
        if chat_id not in settings:
            settings[chat_id] = dict(DEFAULT_SETTINGS)
            save_settings(settings)
        # Back-fill any keys added after first save
        changed = False
        for k, v in DEFAULT_SETTINGS.items():
            if k not in settings[chat_id]:
                settings[chat_id][k] = v
                changed = True
        if changed:
            save_settings(settings)
        return dict(settings[chat_id])


def update_chat_settings(chat_id: str, **kwargs):
    with settings_lock:
        if chat_id not in settings:
            settings[chat_id] = dict(DEFAULT_SETTINGS)
        settings[chat_id].update(kwargs)
        save_settings(settings)


# ── Notification helpers ──────────────────────────────────────────────────────

def send_notification(chat_id: str, text: str, **kwargs) -> None:
    """Send a notification and keep only the last MAX_NOTIFICATIONS per chat.
    Older messages are deleted automatically."""
    try:
        msg = bot.send_message(int(chat_id), text, **kwargs)
    except Exception:
        return
    with settings_lock:
        if chat_id not in settings:
            settings[chat_id] = dict(DEFAULT_SETTINGS)
        ids: list = list(settings[chat_id].get("notification_msg_ids", []))
        ids.append(msg.message_id)
        while len(ids) > MAX_NOTIFICATIONS:
            old_id = ids.pop(0)
            try:
                bot.delete_message(int(chat_id), old_id)
            except Exception:
                pass
        settings[chat_id]["notification_msg_ids"] = ids
        save_settings(settings)


# ── Queue history ────────────────────────────────────────────────────────────

_history_lock = threading.Lock()

def _load_history() -> list:
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []

queue_history: list = _load_history()   # [[ts, cars], ...]
_api_dispatched_24h: int | None = None  # "направлено за 24ч" from API
_api_dispatched_1h:  int | None = None  # "направлено за последний час" from API
_mon_last_fetch: float = 0              # timestamp of last mon.declarant.by fetch


def add_history_point(ts: float, cars: int):
    global queue_history
    cutoff = ts - HISTORY_MAX_AGE
    with _history_lock:
        queue_history.append([ts, cars])
        queue_history = [p for p in queue_history if p[0] >= cutoff]
        try:
            with open(HISTORY_FILE, "w", encoding="utf-8") as f:
                json.dump(queue_history, f)
        except Exception:
            pass


def calc_throughput() -> tuple[float | None, str]:
    """Estimate cars/hour throughput. Returns (cars_per_hour, source_label).
    Priority: 1h API → 24h API → recent local history (last 2h)."""
    if _api_dispatched_1h and _api_dispatched_1h > 0:
        return float(_api_dispatched_1h), f"за посл. час ({_api_dispatched_1h} авто/ч)"
    if _api_dispatched_24h and _api_dispatched_24h > 0:
        rate = _api_dispatched_24h / 24.0
        return rate, f"среднее за 24ч (~{rate:.0f} авто/ч)"

    # Fallback: история за последние 2 часа
    with _history_lock:
        pts = list(queue_history)
    cutoff = time.time() - 2 * 3600
    pts = [p for p in pts if p[0] >= cutoff]
    if len(pts) < 5:
        return None, "накапливается история…"
    rates = []
    for i in range(1, len(pts)):
        dt = pts[i][0] - pts[i - 1][0]
        dc = pts[i - 1][1] - pts[i][1]
        if dt > 0 and dc > 0:
            rates.append(dc / dt * 3600)
    if len(rates) < 3:
        return None, "накапливается история…"
    rates.sort()
    rate = rates[len(rates) // 2]
    return rate, f"из истории за 2ч (~{rate:.0f} авто/ч)"


def history_stats() -> dict:
    """Return age of oldest point and number of samples."""
    with _history_lock:
        pts = list(queue_history)
    if not pts:
        return {"samples": 0, "age_h": 0}
    age_h = (time.time() - pts[0][0]) / 3600
    return {"samples": len(pts), "age_h": age_h}


# ── Data fetching ────────────────────────────────────────────────────────────

def fetch_throughput_from_mon() -> dict | None:
    """Fetch throughput data from mon.declarant.by website."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return None

    try:
        resp = requests.get(
            "https://mon.declarant.by/zone/brest-bts",
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')

        dispatched_1h = None
        dispatched_24h = None

        for elem in soup.find_all(string=True):
            text = str(elem).strip()
            if "Направлено за последний час" in text:
                parent = elem.parent
                for el in parent.find_all(string=True):
                    match = re.search(r'\d+', str(el))
                    if match:
                        dispatched_1h = int(match.group())
                        break
            if "Направлено за последние 24 часа" in text:
                parent = elem.parent
                for el in parent.find_all(string=True):
                    match = re.search(r'\d+', str(el))
                    if match:
                        dispatched_24h = int(match.group())
                        break

        if dispatched_1h or dispatched_24h:
            return {
                "dispatched_1h": dispatched_1h,
                "dispatched_24h": dispatched_24h,
            }
        return None
    except Exception:
        return None


def fetch_queue() -> dict | None:
    """Fetch real-time queue from belarusborder.by (BTS electronic queue)."""
    try:
        resp = requests.get(
            BTS_API,
            params={"token": BTS_TOKEN, "checkpointId": BREST_CHECKPOINT_ID},
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        cars_live = len(data.get("carLiveQueue") or [])
        cars_prio = len(data.get("carPriority") or [])
        motos = (len(data.get("motorcycleLiveQueue") or [])
                 + len(data.get("motorcyclePriority") or []))

        # Case-insensitive lookup map for throughput fields
        data_ci = {k.lower().strip(): v for k, v in data.items()}

        def _pick_int(keys):
            for k in keys:
                v = data_ci.get(k.lower().strip())
                if v is None:
                    continue
                try:
                    iv = int(float(v))
                    if iv > 0:
                        return iv
                except (TypeError, ValueError):
                    pass
            return None

        # "Направлено за последний час" — current throughput (cars/hour)
        dispatched_1h = _pick_int([
            "направлено за последний час",
            "carSentLastHour", "sentLastHour", "passed1h",
        ])

        # "Направлено за последние 24 часа" — daily total
        dispatched_24h = _pick_int([
            "направлено за последние 24 часа",
            "carSentCount", "carPassedCount", "passedCount",
            "sentCount", "dispatchedCount", "carDispatched",
            "carSent", "passed24h", "sent24h",
        ])

        # Scalar fields for diagnostics (everything that is not a list/dict)
        scalar_fields = {
            k: v for k, v in data.items()
            if not isinstance(v, (list, dict))
        }

        return {
            "cars_total":        cars_live + cars_prio,
            "cars_live":         cars_live,
            "cars_prio":         cars_prio,
            "motos":             motos,
            "dispatched_1h":     dispatched_1h,
            "dispatched_24h":    dispatched_24h,
            "scalar_fields":     scalar_fields,
            "raw_live_queue":    data.get("carLiveQueue") or [],
            "raw_priority_queue": data.get("carPriority") or [],
        }
    except Exception:
        return None


def normalize_reg(reg: str) -> str:
    """Uppercase + strip spaces, dashes, dots → canonical form for comparison."""
    return re.sub(r'[\s\-.]', '', reg).upper()


def _item_matches(item, norm_reg: str) -> bool:
    """Check whether a queue item (dict or plain string) matches a normalized reg number."""
    if isinstance(item, dict):
        # Try every known field name
        for field in _CAR_REG_FIELDS:
            val = str(item.get(field) or "")
            if val and normalize_reg(val) == norm_reg:
                return True
        # Also try ALL string-valued fields as a fallback
        for val in item.values():
            if isinstance(val, str) and normalize_reg(val) == norm_reg:
                return True
    elif isinstance(item, str):
        return normalize_reg(item) == norm_reg
    return False


def find_car_in_queue(
    norm_reg: str,
    live_queue: list,
    priority_queue: list,
) -> tuple[int | None, bool]:
    """Return (position_1based, is_called).

    position is None if not in live queue.
    is_called is True when car is in the priority (called-to-checkpoint) queue.
    """
    for item in (priority_queue or []):
        if _item_matches(item, norm_reg):
            return None, True
    for i, item in enumerate(live_queue or []):
        if _item_matches(item, norm_reg):
            return i + 1, False
    return None, False


def format_queue(q: dict) -> str:
    lines = ["🚗 *Брест — очередь (выезд):*",
             f"  Легковые: *{q['cars_total']}* шт."]
    if q["cars_prio"]:
        lines.append(f"  _(в т.ч. приоритет: {q['cars_prio']})_")
    if q["motos"]:
        lines.append(f"  Мотоциклы: *{q['motos']}* шт.")
    lines += ["", "_(belarusborder.by — реальное время)_"]
    return "\n".join(lines)


# ── Background monitor ───────────────────────────────────────────────────────
# Ticks every TICK seconds. Always fetches for history/planner; per-user
# interval is checked individually for threshold/step notifications.

def monitor_loop():
    global _api_dispatched_24h, _api_dispatched_1h, _mon_last_fetch
    while True:
        time.sleep(TICK)
        now = time.time()

        # Always fetch so history & planner work even if no user has alerts on
        queue = fetch_queue()
        if queue is not None:
            add_history_point(now, queue["cars_total"])

        # Fetch throughput from mon.declarant.by every 5 minutes
        if now - _mon_last_fetch >= 300:
            mon = fetch_throughput_from_mon()
            if mon:
                if mon.get("dispatched_1h") is not None:
                    _api_dispatched_1h = mon["dispatched_1h"]
                if mon.get("dispatched_24h") is not None:
                    _api_dispatched_24h = mon["dispatched_24h"]
            _mon_last_fetch = now

        with settings_lock:
            snapshot = {cid: dict(cfg) for cid, cfg in settings.items()}

        # ── Per-user threshold / step alerts ─────────────────────────────────
        due_chats = [
            (cid, cfg) for cid, cfg in snapshot.items()
            if cfg.get("enabled")
            and now - cfg.get("last_checked", 0) >= cfg.get("interval", 5) * 60
        ]

        if queue is not None and due_chats:
            for chat_id, cfg in due_chats:
                update_chat_settings(chat_id, last_checked=now)

                if now < cfg.get("paused_until", 0):
                    continue

                cars_count = queue["cars_total"]
                updates = {}

                # ── Threshold alert (fires once when crossed, resets when drops below) ──
                threshold = cfg.get("threshold", 50)
                threshold_notified = cfg.get("threshold_notified", False)
                if cars_count >= threshold and not threshold_notified:
                    send_notification(
                        chat_id,
                        f"⚠️ *Внимание!* Очередь в Бресте достигла *{cars_count}* авто "
                        f"(порог: {threshold}).\n\n" + format_queue(queue),
                        parse_mode='Markdown'
                    )
                    updates["threshold_notified"] = True
                elif cars_count < threshold and threshold_notified:
                    updates["threshold_notified"] = False  # reset — ready to fire again

                # ── Step alert (fires when queue grew by `step` since last alert) ──
                step = cfg.get("step", 50)
                baseline = cfg.get("step_baseline")
                if step > 0:
                    if baseline is None:
                        # First check — just record current value, don't alert
                        updates["step_baseline"] = cars_count
                    else:
                        delta = cars_count - baseline
                        if delta >= step:
                            send_notification(
                                chat_id,
                                f"📈 *Очередь выросла на {delta} авто!*\n\n"
                                + format_queue(queue),
                                parse_mode='Markdown'
                            )
                            updates["step_baseline"] = cars_count
                        elif cars_count < baseline:
                            # Queue dropped — reset baseline to current low point
                            updates["step_baseline"] = cars_count

                if updates:
                    update_chat_settings(chat_id, **updates)

        # ── Planner check ─────────────────────────────────────────────────────
        if queue is None:
            continue

        cars_now = queue["cars_total"]

        throughput, tp_label = calc_throughput()

        for chat_id, cfg in snapshot.items():
            target_str = cfg.get("planner_target")
            if not target_str or cfg.get("planner_notified"):
                continue
            try:
                target_dt = datetime.datetime.strptime(target_str, "%Y-%m-%d %H:%M")
                target_ts = target_dt.timestamp()
            except Exception:
                continue

            hours_to_target = (target_ts - now) / 3600

            # Clean up expired targets (passed more than 2 h ago)
            if hours_to_target < -2:
                update_chat_settings(chat_id, planner_target=None, planner_notified=False)
                continue

            # Special case: queue is empty — ideal moment, alert immediately
            if cars_now == 0 and hours_to_target > 0:
                send_notification(
                    chat_id,
                    f"🟢 *Планировщик: очередь пуста — лучший момент для выезда!*\n\n"
                    f"Желаемый выезд: *{fmt_dt(target_dt)}*\n"
                    f"Сейчас в очереди: *0 авто* 🎉\n\n"
                    f"🔴 Зарегистрируйтесь в электронной очереди прямо сейчас!\n"
                    f"[belarusborder.by](https://belarusborder.by)",
                    parse_mode='Markdown',
                    disable_web_page_preview=True,
                )
                update_chat_settings(chat_id, planner_notified=True)
                continue

            if throughput is None or throughput <= 0:
                continue

            estimated_wait_h = cars_now / throughput

            # Alert when estimated wait ≥ time to target minus buffer
            if estimated_wait_h >= hours_to_target - PLANNER_ALERT_BUFFER:
                send_notification(
                    chat_id,
                    f"⏰ *Планировщик: пора вставать в очередь!*\n\n"
                    f"Желаемый выезд: *{fmt_dt(target_dt)}*\n"
                    f"Сейчас в очереди: *{cars_now}* авто\n"
                    f"Пропускная способность: *{tp_label}*\n"
                    f"Расчётное ожидание: *~{estimated_wait_h:.1f} ч*\n\n"
                    f"🔴 Зарегистрируйтесь в электронной очереди прямо сейчас!\n"
                    f"[belarusborder.by](https://belarusborder.by)",
                    parse_mode='Markdown',
                    disable_web_page_preview=True,
                )
                update_chat_settings(chat_id, planner_notified=True)

        # ── Car position tracking ──────────────────────────────────────────────
        raw_live = queue.get("raw_live_queue", [])
        raw_prio = queue.get("raw_priority_queue", [])

        for chat_id, cfg in snapshot.items():
            tracked = cfg.get("tracked_cars")
            if not tracked:
                continue
            updated = dict(tracked)
            changed = False
            for norm_reg, state in list(tracked.items()):
                pos, is_called = find_car_in_queue(norm_reg, raw_live, raw_prio)
                milestones_done = list(state.get("milestones_done", []))
                called_notified = state.get("called_notified", False)
                new_state = dict(state)

                if is_called and not called_notified:
                    send_notification(
                        chat_id,
                        f"🟢 *Ваш автомобиль вызван на пункт пропуска!*\n\n"
                        f"*{norm_reg}* — подъезжайте к пункту пропуска прямо сейчас!",
                        parse_mode='Markdown',
                    )
                    new_state["called_notified"] = True
                    changed = True
                elif pos is not None:
                    # Milestones already entered but not yet recorded
                    applicable = [m for m in CAR_MILESTONES
                                  if pos <= m and m not in milestones_done]
                    if applicable:
                        if not milestones_done:
                            # First ever detection — just report current position,
                            # no milestone fanfare; skip past all higher milestones silently.
                            send_notification(
                                chat_id,
                                f"🚗 *{norm_reg}* найден в очереди\n\n"
                                f"Текущая позиция: *{pos}*",
                                parse_mode='Markdown',
                            )
                        else:
                            # Car crossed into a new milestone zone
                            notify_ms = min(applicable)
                            send_notification(
                                chat_id,
                                f"🚗 *{norm_reg}* — *{pos}-е место* в очереди\n\n"
                                f"Рубеж ≤ {notify_ms}.",
                                parse_mode='Markdown',
                            )
                        milestones_done.extend(applicable)
                        changed = True
                    new_state["milestones_done"] = milestones_done
                updated[norm_reg] = new_state

            if changed:
                update_chat_settings(chat_id, tracked_cars=updated)


threading.Thread(target=monitor_loop, daemon=True).start()


# ── Handlers ─────────────────────────────────────────────────────────────────

@bot.message_handler(commands=['start'])
def start(message):
    chat_id = str(message.chat.id)
    get_chat_settings(chat_id)  # ensure defaults exist
    bot.reply_to(
        message,
        "👋 Привет! Я отслеживаю очередь на границе в *Бресте*.\n\n"
        "Команды:\n"
        "/check — текущая очередь\n"
        "/enable — включить авто-уведомления\n"
        "/disable — выключить уведомления\n"
        "/pause — заглушить уведомления на время\n"
        "/resume — снять паузу досрочно\n"
        "/set\\_threshold <число> — задать разовый порог\n"
        "/setstep — шаг уведомлений (каждые N авто)\n"
        "/setinterval — частота проверки (1 / 5 / 15 мин)\n"
        "/threshold — показать все настройки\n"
        "/planner — планировщик выезда\n"
        "/source — статус соединения с источником данных\n"
        "/help — справка",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['help'])
def help_cmd(message):
    bot.reply_to(
        message,
        "ℹ️ *Как работают уведомления:*\n\n"
        "Бот проверяет очередь с вашей частотой (1 / 5 / 15 мин) и присылает два вида уведомлений:\n\n"
        "• *Порог* — разовое оповещение, когда авто ≥ заданного числа.\n"
        "• *Шаг* — уведомление каждый раз, когда очередь вырастает на N авто (50 → 100 → 150…). "
        "При снижении счётчик сбрасывается.\n\n"
        "Команды:\n"
        "/enable — включить уведомления\n"
        "/disable — выключить уведомления\n"
        "/pause — заглушить на 1 / 2 / 4 / 8 / 24 ч (кнопки)\n"
        "/resume — снять паузу досрочно\n"
        "/set\\_threshold <число> — задать порог, напр. `/set_threshold 100`\n"
        "/setstep — задать шаг уведомлений\n"
        "/setinterval — частота проверки (1 / 5 / 15 мин)\n"
        "/threshold — показать все настройки\n"
        "/check — проверить очередь прямо сейчас\n"
        "/planner — планировщик: ввести дату выезда, бот пришлёт сигнал когда пора вставать в очередь\n"
        "/source — статус соединения с источником данных\n\n"
        "Данные: belarusborder.by (электронная очередь БТС, реальное время)",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['check'])
def check(message):
    bot.reply_to(message, "⏳ Запрашиваю данные...")
    queue = fetch_queue()
    if queue:
        bot.reply_to(message, format_queue(queue), parse_mode='Markdown')
    else:
        bot.reply_to(message, "❌ Не удалось получить данные. Попробуйте позже.")


@bot.message_handler(commands=['source'])
def source_cmd(message):
    import time as _time
    bot.reply_to(message, "🔍 Проверяю соединение с belarusborder.by...")
    t0 = _time.time()
    try:
        resp = requests.get(
            BTS_API,
            params={"token": BTS_TOKEN, "checkpointId": BREST_CHECKPOINT_ID},
            headers=HEADERS,
            timeout=15,
        )
        elapsed = _time.time() - t0
        if resp.status_code == 200:
            data = resp.json()
            cars = len(data.get("carLiveQueue") or []) + len(data.get("carPriority") or [])
            # All scalar fields from API (for finding the 24h dispatched field)
            scalar_fields = {
                k: v for k, v in data.items()
                if not isinstance(v, (list, dict))
            }
            fields_text = "\n".join(f"  `{k}`: {v}" for k, v in sorted(scalar_fields.items()))
            # Show first queue element to reveal the registration field name
            live_queue = data.get("carLiveQueue") or []
            first_item_text = ""
            if live_queue:
                first = live_queue[0]
                if isinstance(first, dict):
                    first_item_text = "\n\n*Первый элемент carLiveQueue:*\n" + "\n".join(
                        f"  `{k}`: `{v}`" for k, v in first.items() if not isinstance(v, (dict, list))
                    )
                else:
                    first_item_text = f"\n\n*carLiveQueue[0]*: `{str(first)[:300]}`"

            # Show structure of all other top-level keys (for finding throughput)
            other_keys_text = ""
            skip = {"carLiveQueue", "carPriority", "motorcycleLiveQueue", "motorcyclePriority"}
            other_lines = []
            for k, v in data.items():
                if k in skip:
                    continue
                if isinstance(v, list) and v:
                    first_el = v[0]
                    if isinstance(first_el, dict):
                        preview = ", ".join(f"{fk}={fv}" for fk, fv in first_el.items())
                        other_lines.append(f"  `{k}` (список): {{{preview}}}")
                    else:
                        other_lines.append(f"  `{k}` (список): `{str(first_el)[:120]}`")
                elif isinstance(v, dict):
                    # Show ALL fields of nested dicts
                    for fk, fv in v.items():
                        other_lines.append(f"  `{k}.{fk}`: `{fv}`")
                elif v is not None and v != [] and v != {}:
                    other_lines.append(f"  `{k}`: `{v}`")
            if other_lines:
                other_keys_text = "\n\n*Прочие поля API:*\n" + "\n".join(other_lines)

            bot.reply_to(
                message,
                f"✅ *belarusborder.by — доступен*\n\n"
                f"  Время ответа: *{elapsed:.1f} с*\n"
                f"  HTTP статус: *{resp.status_code}*\n"
                f"  Авто в очереди: *{cars}*\n\n"
                f"*Все поля API (не массивы):*\n{fields_text or '  (нет)'}"
                f"{first_item_text}"
                f"{other_keys_text}",
                parse_mode='Markdown'
            )
        else:
            bot.reply_to(
                message,
                f"⚠️ *belarusborder.by — ошибка*\n\n"
                f"  HTTP статус: *{resp.status_code}*\n"
                f"  Время: *{elapsed:.1f} с*",
                parse_mode='Markdown'
            )
    except requests.exceptions.ConnectTimeout:
        elapsed = _time.time() - t0
        bot.reply_to(
            message,
            f"🔴 *belarusborder.by — недоступен*\n\n"
            f"  Причина: таймаут соединения ({elapsed:.0f} с)\n"
            f"  Сервер не отвечает — возможно, заблокирован с текущего IP.\n\n"
            f"Попробуйте перенести бота на европейский хостинг.",
            parse_mode='Markdown'
        )
    except requests.exceptions.ConnectionError:
        elapsed = _time.time() - t0
        bot.reply_to(
            message,
            f"🔴 *belarusborder.by — недоступен*\n\n"
            f"  Причина: соединение отклонено ({elapsed:.1f} с)\n"
            f"  IP текущего сервера заблокирован.\n\n"
            f"Попробуйте перенести бота на европейский хостинг.",
            parse_mode='Markdown'
        )
    except Exception as e:
        elapsed = _time.time() - t0
        bot.reply_to(
            message,
            f"🔴 *belarusborder.by — ошибка*\n\n"
            f"  {str(e)[:100]}\n"
            f"  Время: *{elapsed:.1f} с*",
            parse_mode='Markdown'
        )


@bot.message_handler(commands=['debug_stats'])
def debug_stats(message):
    """Probe known belarusborder.by endpoints to find throughput data."""
    import time as _time
    chat_id = str(message.chat.id)
    bot.reply_to(message, "🔍 Проверяю эндпоинты для данных о пропускной способности…")
    candidates = [
        ("monitoring-new (текущий)", "https://belarusborder.by/info/monitoring-new"),
        ("monitoring",               "https://belarusborder.by/info/monitoring"),
        ("checkpoint",               "https://belarusborder.by/info/checkpoint"),
        ("statistics",               "https://belarusborder.by/info/statistics"),
        ("statistic",                "https://belarusborder.by/info/statistic"),
        ("monitoring-stats",         "https://belarusborder.by/info/monitoring-stats"),
        ("checkpoint-info",          "https://belarusborder.by/info/checkpoint-info"),
    ]
    params = {"token": BTS_TOKEN, "checkpointId": BREST_CHECKPOINT_ID}
    lines = []
    for label, url in candidates:
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                try:
                    d = r.json()
                    # Collect all scalar values including inside dicts (1 level deep)
                    scalars = {}
                    for k, v in d.items():
                        if not isinstance(v, (list, dict)):
                            scalars[k] = v
                        elif isinstance(v, dict):
                            for sk, sv in v.items():
                                if not isinstance(sv, (list, dict)):
                                    scalars[f"{k}.{sk}"] = sv
                    # Filter for fields that look like throughput numbers
                    interesting = {k: v for k, v in scalars.items()
                                   if isinstance(v, (int, float)) and v > 0
                                   or (isinstance(v, str) and any(w in k.lower() for w in
                                       ("sent", "pass", "direct", "час", "24", "напр")))}
                    top_keys = list(d.keys())[:8]
                    lines.append(f"✅ *{label}* ({r.status_code})\n"
                                 f"  Ключи: {top_keys}\n"
                                 f"  Числа: {interesting}")
                except Exception:
                    lines.append(f"✅ *{label}* ({r.status_code}) — не JSON")
            else:
                lines.append(f"❌ *{label}* ({r.status_code})")
        except Exception as e:
            lines.append(f"⚠️ *{label}* — {str(e)[:60]}")
    bot.send_message(chat_id, "\n\n".join(lines) or "Ничего не найдено.", parse_mode='Markdown')


@bot.message_handler(commands=['debug_mon'])
def debug_mon(message):
    """Test scraping of throughput data from mon.declarant.by."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        bot.reply_to(message, "❌ beautifulsoup4 не установлен.")
        return

    chat_id = str(message.chat.id)
    bot.reply_to(message, "🔍 Запрашиваю mon.declarant.by/zone/brest-bts…")

    url = "https://mon.declarant.by/zone/brest-bts"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        status = resp.status_code
    except Exception as e:
        bot.send_message(chat_id, f"❌ Запрос не прошёл: `{str(e)[:200]}`", parse_mode='Markdown')
        return

    if status != 200:
        bot.send_message(chat_id, f"❌ HTTP {status} от mon.declarant.by")
        return

    soup = BeautifulSoup(resp.content, 'html.parser')

    lines = [f"✅ *HTTP {status}* · размер {len(resp.content)} байт\n"]

    # Find relevant text nodes and show context
    keywords = ["Направлено за последний час", "Направлено за последние 24 часа",
                 "последний час", "24 час"]
    found_any = False
    for kw in keywords:
        for elem in soup.find_all(string=lambda t: t and kw in t):
            found_any = True
            parent = elem.parent
            parent_text = parent.get_text(separator=" ", strip=True)[:300]
            # Try to extract a number
            match = re.search(r'\d+', parent_text)
            num = match.group() if match else "число не найдено"
            lines.append(
                f"🔑 *{kw}*\n"
                f"  Тег: `{parent.name}`\n"
                f"  Текст: `{parent_text}`\n"
                f"  Число: *{num}*"
            )
            break  # one example per keyword is enough

    if not found_any:
        # Show first 600 chars of visible text so user can see what's there
        visible = soup.get_text(separator=" ", strip=True)[:600]
        lines.append(f"⚠️ Ключевые слова не найдены.\n\nВидимый текст страницы:\n`{visible}`")

    # Also show what fetch_throughput_from_mon() actually returned
    result = fetch_throughput_from_mon()
    if result:
        lines.append(
            f"\n✅ *fetch\\_throughput\\_from\\_mon()* вернул:\n"
            f"  направлено за посл. час: *{result.get('dispatched_1h')}*\n"
            f"  направлено за 24ч: *{result.get('dispatched_24h')}*"
        )
    else:
        lines.append("\n❌ *fetch\\_throughput\\_from\\_mon()* вернул `None`")

    bot.send_message(chat_id, "\n\n".join(lines), parse_mode='Markdown')


@bot.message_handler(commands=['enable'])
def enable(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    update_chat_settings(chat_id, enabled=True)
    bot.reply_to(
        message,
        f"✅ Уведомления *включены*.\n"
        f"Порог: *{cfg['threshold']}* авто · Шаг: *{cfg.get('step', 50)}* авто · "
        f"Интервал: *{ALLOWED_INTERVALS.get(cfg['interval'], str(cfg['interval'])+' мин')}*\n\n"
        f"Настройки: `/set_threshold`, `/setstep`, `/setinterval`",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['disable'])
def disable(message):
    chat_id = str(message.chat.id)
    update_chat_settings(chat_id, enabled=False)
    bot.reply_to(message, "🔕 Уведомления *отключены*.", parse_mode='Markdown')


THRESHOLD_PRESETS = [20, 50, 100, 150, 200, 300, 500]


def _threshold_keyboard(current):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=4)
    buttons = [
        telebot.types.InlineKeyboardButton(
            text=f"{'✅ ' if t == current else ''}{t}",
            callback_data=f"thr:{t}"
        )
        for t in THRESHOLD_PRESETS
    ]
    keyboard.add(*buttons)
    keyboard.add(telebot.types.InlineKeyboardButton("✏️ Своё число", callback_data="thr:custom"))
    return keyboard


@bot.message_handler(commands=['set_threshold'])
def set_threshold(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    current = cfg.get("threshold", 50)

    parts = message.text.strip().split()
    if len(parts) >= 2:
        try:
            value = int(parts[1])
            if value < 1 or value > 9999:
                raise ValueError
        except ValueError:
            bot.reply_to(message, "⚠️ Введите целое число от 1 до 9999.")
            return
        update_chat_settings(chat_id, threshold=value)
        status = "включены" if cfg["enabled"] else "выключены"
        bot.reply_to(
            message,
            f"✅ Порог установлен: *{value}* авто.\nУведомления: {status}.",
            parse_mode='Markdown'
        )
        return

    bot.reply_to(
        message,
        f"🎯 *Порог уведомления*\n\n"
        f"Разовое оповещение придёт, когда очередь достигнет выбранного числа авто.\n"
        f"Сейчас: *{current}* авто\n\n"
        f"Выберите порог:",
        reply_markup=_threshold_keyboard(current),
        parse_mode='Markdown'
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("thr:"))
def handle_threshold_callback(call):
    chat_id = str(call.message.chat.id)
    payload = call.data.split(":", 1)[1]

    if payload == "custom":
        bot.answer_callback_query(call.id)
        sent = bot.send_message(chat_id, "✏️ Введите любое число от 1 до 9999:")
        bot.register_next_step_handler(sent, _receive_custom_threshold)
        return

    try:
        value = int(payload)
        if value not in THRESHOLD_PRESETS:
            raise ValueError
    except ValueError:
        bot.answer_callback_query(call.id, "Неверное значение.")
        return

    update_chat_settings(chat_id, threshold=value)
    bot.edit_message_text(
        f"🎯 *Порог уведомления*\n\n"
        f"Разовое оповещение придёт, когда очередь достигнет выбранного числа авто.\n\n"
        f"✅ Установлено: *{value} авто*",
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        reply_markup=_threshold_keyboard(value),
        parse_mode='Markdown'
    )
    bot.answer_callback_query(call.id, f"Порог: {value} авто")


def _receive_custom_threshold(message):
    chat_id = str(message.chat.id)
    try:
        value = int(message.text.strip())
        if value < 1 or value > 9999:
            raise ValueError
    except ValueError:
        msg = bot.reply_to(message, "⚠️ Введите целое число от 1 до 9999:")
        bot.register_next_step_handler(msg, _receive_custom_threshold)
        return
    update_chat_settings(chat_id, threshold=value)
    cfg = get_chat_settings(chat_id)
    status = "включены" if cfg["enabled"] else "выключены"
    bot.reply_to(
        message,
        f"✅ Порог установлен: *{value}* авто.\nУведомления: {status}.",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['threshold'])
def show_threshold(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    status = "✅ включены" if cfg["enabled"] else "🔕 выключены"
    interval_label = ALLOWED_INTERVALS.get(cfg["interval"], f"{cfg['interval']} мин")
    paused_until = cfg.get("paused_until", 0)
    now = time.time()
    if paused_until > now:
        remaining_min = int((paused_until - now) / 60)
        h, m = divmod(remaining_min, 60)
        pause_str = f"⏸ пауза ещё ~{h}ч {m}мин" if h else f"⏸ пауза ещё ~{m}мин"
    else:
        pause_str = "нет"
    bot.reply_to(
        message,
        f"📊 *Текущие настройки:*\n"
        f"  Порог:       *{cfg['threshold']}* авто (разовое)\n"
        f"  Шаг:         *{cfg.get('step', 50)}* авто (повторяемое)\n"
        f"  Интервал:    *{interval_label}*\n"
        f"  Уведомления: {status}\n"
        f"  Пауза:       {pause_str}",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['setinterval'])
def set_interval(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    current = cfg.get("interval", 5)

    keyboard = telebot.types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    for mins, label in ALLOWED_INTERVALS.items():
        marker = "✅ " if mins == current else ""
        buttons.append(
            telebot.types.InlineKeyboardButton(
                text=f"{marker}{label}",
                callback_data=f"interval:{mins}"
            )
        )
    keyboard.add(*buttons)
    bot.reply_to(
        message,
        "🕐 Выберите частоту проверки очереди:",
        reply_markup=keyboard
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("interval:"))
def handle_interval_callback(call):
    chat_id = str(call.message.chat.id)
    try:
        mins = int(call.data.split(":")[1])
        if mins not in ALLOWED_INTERVALS:
            raise ValueError
    except (IndexError, ValueError):
        bot.answer_callback_query(call.id, "Неверное значение.")
        return

    update_chat_settings(chat_id, interval=mins, last_checked=0)
    label = ALLOWED_INTERVALS[mins]
    cfg = get_chat_settings(chat_id)

    # Rebuild keyboard with updated checkmark
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    for m, l in ALLOWED_INTERVALS.items():
        marker = "✅ " if m == mins else ""
        buttons.append(
            telebot.types.InlineKeyboardButton(
                text=f"{marker}{l}",
                callback_data=f"interval:{m}"
            )
        )
    keyboard.add(*buttons)

    bot.edit_message_text(
        f"🕐 Выберите частоту проверки очереди:\n\n"
        f"✅ Установлено: *{label}*",
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )
    bot.answer_callback_query(call.id, f"Интервал: {label}")


@bot.message_handler(commands=['pause'])
def pause_cmd(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    paused_until = cfg.get("paused_until", 0)
    now = time.time()

    keyboard = telebot.types.InlineKeyboardMarkup(row_width=3)
    keyboard.add(*[
        telebot.types.InlineKeyboardButton(
            text=label,
            callback_data=f"pause:{hours}"
        )
        for hours, label in PAUSE_OPTIONS
    ])

    if paused_until > now:
        remaining_min = int((paused_until - now) / 60)
        h, m = divmod(remaining_min, 60)
        current = f"⏸ сейчас на паузе ещё ~{h}ч {m}мин." if h else f"⏸ сейчас на паузе ещё ~{m}мин."
    else:
        current = "Уведомления активны."

    bot.reply_to(
        message,
        f"⏸ *Пауза уведомлений*\n\n{current}\n\nНа сколько заглушить?",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("pause:"))
def handle_pause_callback(call):
    chat_id = str(call.message.chat.id)
    try:
        hours = int(call.data.split(":")[1])
        if hours not in dict(PAUSE_OPTIONS):
            raise ValueError
    except (IndexError, ValueError):
        bot.answer_callback_query(call.id, "Неверное значение.")
        return

    until = time.time() + hours * 3600
    update_chat_settings(chat_id, paused_until=until)
    label = dict(PAUSE_OPTIONS)[hours]

    bot.edit_message_text(
        f"⏸ *Пауза установлена на {label}.*\n\n"
        f"Уведомления возобновятся автоматически.\n"
        f"Снять досрочно: /resume",
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        parse_mode='Markdown'
    )
    bot.answer_callback_query(call.id, f"Пауза на {label}")


@bot.message_handler(commands=['reset'])
def reset_cmd(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    ids = cfg.get("notification_msg_ids", [])
    deleted = 0
    for mid in ids:
        try:
            bot.delete_message(int(chat_id), mid)
            deleted += 1
        except Exception:
            pass
    # Also reset car tracking milestones (keep the car list, just clear progress)
    cfg2 = get_chat_settings(chat_id)
    tracked = cfg2.get("tracked_cars", {})
    reset_tracked = {r: {"milestones_done": [], "called_notified": False} for r in tracked}
    update_chat_settings(
        chat_id,
        notification_msg_ids=[],
        step_baseline=None,
        threshold_notified=False,
        planner_notified=False,
        tracked_cars=reset_tracked,
    )
    bot.reply_to(
        message,
        f"🗑 Удалено *{deleted}* уведомлений. Состояние сброшено — счётчики начнут с нуля.",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['resume'])
def resume_cmd(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    if cfg.get("paused_until", 0) > time.time():
        update_chat_settings(chat_id, paused_until=0)
        bot.reply_to(message, "▶️ Пауза снята. Уведомления возобновлены.", parse_mode='Markdown')
    else:
        bot.reply_to(message, "ℹ️ Уведомления и так активны, паузы нет.")


STEP_OPTIONS = [10, 25, 50, 100, 200]


@bot.message_handler(commands=['setstep'])
def set_step(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    current = cfg.get("step", 50)

    keyboard = telebot.types.InlineKeyboardMarkup(row_width=3)
    buttons = [
        telebot.types.InlineKeyboardButton(
            text=f"{'✅ ' if s == current else ''}{s} авто",
            callback_data=f"step:{s}"
        )
        for s in STEP_OPTIONS
    ]
    keyboard.add(*buttons)
    bot.reply_to(
        message,
        "📈 *Шаг уведомлений*\n\n"
        "Уведомление придёт каждый раз, когда очередь вырастает на выбранное число авто.\n"
        "Например, при шаге 50: оповещения на 50, 100, 150, 200…\n\n"
        "Выберите шаг:",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("step:"))
def handle_step_callback(call):
    chat_id = str(call.message.chat.id)
    try:
        step_val = int(call.data.split(":")[1])
        if step_val not in STEP_OPTIONS:
            raise ValueError
    except (IndexError, ValueError):
        bot.answer_callback_query(call.id, "Неверное значение.")
        return

    update_chat_settings(chat_id, step=step_val, step_baseline=None)

    keyboard = telebot.types.InlineKeyboardMarkup(row_width=3)
    buttons = [
        telebot.types.InlineKeyboardButton(
            text=f"{'✅ ' if s == step_val else ''}{s} авто",
            callback_data=f"step:{s}"
        )
        for s in STEP_OPTIONS
    ]
    keyboard.add(*buttons)

    bot.edit_message_text(
        f"📈 *Шаг уведомлений*\n\n"
        f"Уведомление придёт каждый раз, когда очередь вырастает на выбранное число авто.\n"
        f"Например, при шаге 50: оповещения на 50, 100, 150, 200…\n\n"
        f"✅ Установлено: *{step_val} авто*",
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )
    bot.answer_callback_query(call.id, f"Шаг: {step_val} авто")


# ── Planner ───────────────────────────────────────────────────────────────────

_planner_pending: dict = {}   # chat_id -> {"date": "YYYY-MM-DD"} (ephemeral)


def _parse_date(text: str) -> datetime.date | None:
    text = text.strip()
    today = datetime.date.today()
    for fmt in ("%d.%m.%Y", "%d.%m"):
        try:
            d = datetime.datetime.strptime(text, fmt)
            if fmt == "%d.%m":
                year = today.year if (d.month, d.day) >= (today.month, today.day) else today.year + 1
                d = d.replace(year=year)
            return d.date()
        except ValueError:
            pass
    return None


def _planner_status_text(cfg: dict) -> str:
    target_str = cfg.get("planner_target")
    if not target_str:
        return "📅 *Планировщик* не настроен."
    try:
        dt = datetime.datetime.strptime(target_str, "%Y-%m-%d %H:%M")
    except Exception:
        return "📅 *Планировщик* не настроен."
    notified = cfg.get("planner_notified", False)
    status = "✅ уведомление отправлено" if notified else "⏳ ожидаю подходящего момента"
    tp, tp_label = calc_throughput()
    tp_str = tp_label if tp else "накапливается история…"
    return (
        f"📅 *Планировщик активен*\n\n"
        f"  Желаемый выезд: *{fmt_dt(dt)}*\n"
        f"  Статус: {status}\n"
        f"  Пропускная способность: {tp_str}"
    )


def _planner_main_keyboard(has_target: bool) -> telebot.types.InlineKeyboardMarkup:
    kb = telebot.types.InlineKeyboardMarkup(row_width=1)
    kb.add(telebot.types.InlineKeyboardButton("📅 Установить дату и время выезда", callback_data="planner:set"))
    if has_target:
        kb.add(telebot.types.InlineKeyboardButton("❌ Отменить план", callback_data="planner:cancel"))
    return kb


def _date_keyboard() -> telebot.types.InlineKeyboardMarkup:
    today = datetime.date.today()
    kb = telebot.types.InlineKeyboardMarkup(row_width=3)
    options = []
    labels = ["Сегодня", "Завтра", "Послезавтра"]
    for i, label in enumerate(labels):
        d = today + datetime.timedelta(days=i)
        options.append(telebot.types.InlineKeyboardButton(
            f"{label} ({d.strftime('%d.%m')})",
            callback_data=f"planner_date:{d.isoformat()}"
        ))
    kb.add(*options)
    kb.add(telebot.types.InlineKeyboardButton("✏️ Другая дата (DD.MM или DD.MM.YYYY)", callback_data="planner_date:custom"))
    return kb


@bot.message_handler(commands=['planner'])
def planner_cmd(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    bot.reply_to(
        message,
        _planner_status_text(cfg),
        reply_markup=_planner_main_keyboard(bool(cfg.get("planner_target"))),
        parse_mode='Markdown'
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("planner:"))
def handle_planner_callback(call):
    chat_id = str(call.message.chat.id)
    action = call.data.split(":", 1)[1]
    bot.answer_callback_query(call.id)

    if action == "cancel":
        update_chat_settings(chat_id, planner_target=None, planner_notified=False)
        _planner_pending.pop(chat_id, None)
        bot.edit_message_text(
            "❌ *Планировщик отменён.*",
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            parse_mode='Markdown'
        )

    elif action == "set":
        bot.edit_message_text(
            "📅 *Шаг 1 из 2 — Выберите дату выезда:*",
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=_date_keyboard(),
            parse_mode='Markdown'
        )


@bot.callback_query_handler(func=lambda c: c.data.startswith("planner_date:"))
def handle_planner_date(call):
    chat_id = str(call.message.chat.id)
    payload = call.data.split(":", 1)[1]
    bot.answer_callback_query(call.id)

    if payload == "custom":
        sent = bot.send_message(chat_id, "✏️ Введите дату выезда (например: *20.06* или *20.06.2025*):", parse_mode='Markdown')
        bot.register_next_step_handler(sent, _receive_planner_date)
        return

    try:
        date = datetime.date.fromisoformat(payload)
    except Exception:
        bot.send_message(chat_id, "⚠️ Неверная дата.")
        return

    _planner_pending[chat_id] = {"date": date.isoformat()}
    sent = bot.send_message(
        chat_id,
        f"🕐 *Шаг 2 из 2 — Введите желаемое время выезда* ({date.strftime('%d.%m.%Y')}):\n\nФормат: *ЧЧ:ММ* (например: `14:30`)",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(sent, _receive_planner_time)


def _receive_planner_date(message):
    chat_id = str(message.chat.id)
    date = _parse_date(message.text or "")
    if date is None:
        msg = bot.reply_to(message, "⚠️ Не распознал дату. Введите в формате *DD.MM* или *DD.MM.YYYY*:", parse_mode='Markdown')
        bot.register_next_step_handler(msg, _receive_planner_date)
        return
    if date < datetime.date.today():
        msg = bot.reply_to(message, "⚠️ Дата уже прошла. Введите будущую дату:")
        bot.register_next_step_handler(msg, _receive_planner_date)
        return
    _planner_pending[chat_id] = {"date": date.isoformat()}
    sent = bot.reply_to(
        message,
        f"🕐 *Шаг 2 из 2 — Введите желаемое время выезда* ({date.strftime('%d.%m.%Y')}):\n\nФормат: *ЧЧ:ММ* (например: `14:30`)",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(sent, _receive_planner_time)


def _receive_planner_time(message):
    chat_id = str(message.chat.id)
    text = (message.text or "").strip()
    try:
        t = datetime.datetime.strptime(text, "%H:%M").time()
    except ValueError:
        msg = bot.reply_to(message, "⚠️ Не распознал время. Введите в формате *ЧЧ:ММ* (например: `14:30`):", parse_mode='Markdown')
        bot.register_next_step_handler(msg, _receive_planner_time)
        return

    pending = _planner_pending.pop(chat_id, None)
    if not pending:
        bot.reply_to(message, "⚠️ Что-то пошло не так. Попробуйте /planner заново.")
        return

    date = datetime.date.fromisoformat(pending["date"])
    target_dt = datetime.datetime.combine(date, t)
    if target_dt <= datetime.datetime.now():
        msg = bot.reply_to(message, "⚠️ Это время уже прошло. Введите другое время:")
        bot.register_next_step_handler(msg, _receive_planner_time)
        _planner_pending[chat_id] = pending
        return

    target_str = target_dt.strftime("%Y-%m-%d %H:%M")
    update_chat_settings(chat_id, planner_target=target_str, planner_notified=False)

    tp, tp_label = calc_throughput()
    tp_str = (f"\n\nПропускная способность: *{tp_label}* — данных достаточно 👍"
              if tp else
              "\n\n⚠️ Истории очереди пока мало (накапливается). Точность расчёта улучшится через несколько часов работы бота.")

    bot.reply_to(
        message,
        f"✅ *Планировщик активирован!*\n\n"
        f"Желаемый выезд: *{fmt_dt(target_dt)}*\n\n"
        f"Бот рассчитает, когда нужно встать в электронную очередь, и пришлёт уведомление.{tp_str}\n\n"
        f"Отменить: /planner → «Отменить план»",
        parse_mode='Markdown'
    )


# ── Car tracking ─────────────────────────────────────────────────────────────

_cars_pending: dict = {}  # chat_id → pending state (currently unused, reserved)


def _cars_status_text(cfg: dict, queue: dict | None) -> str:
    tracked = cfg.get("tracked_cars", {})
    if not tracked:
        return (
            "🚗 *Отслеживание автомобилей*\n\n"
            "Добавьте номер — бот будет присылать уведомления когда\n"
            "автомобиль окажется на позиции 300, 200, 100, 50, 25, 10\n"
            "и когда его вызовут на пункт пропуска."
        )
    raw_live = (queue or {}).get("raw_live_queue", [])
    raw_prio = (queue or {}).get("raw_priority_queue", [])
    lines = ["🚗 *Отслеживаемые автомобили:*\n"]
    for norm_reg, state in tracked.items():
        pos, is_called = find_car_in_queue(norm_reg, raw_live, raw_prio)
        done = state.get("milestones_done", [])
        if is_called:
            status = "🟢 вызван на пункт пропуска"
        elif pos is not None:
            next_ms = next((m for m in CAR_MILESTONES if m >= pos and m not in done), None)
            status = f"позиция *{pos}*" + (f" → след. уведомление ≤{next_ms}" if next_ms else "")
        else:
            status = "❓ не найден в очереди"
        lines.append(f"  *{norm_reg}* — {status}")
    return "\n".join(lines)


def _cars_keyboard(cfg: dict) -> telebot.types.InlineKeyboardMarkup:
    tracked = cfg.get("tracked_cars", {})
    kb = telebot.types.InlineKeyboardMarkup(row_width=1)
    if len(tracked) < MAX_TRACKED_CARS:
        kb.add(telebot.types.InlineKeyboardButton("➕ Добавить автомобиль", callback_data="cars:add"))
    for norm_reg in tracked:
        kb.add(telebot.types.InlineKeyboardButton(f"❌ Удалить {norm_reg}", callback_data=f"cars:del:{norm_reg}"))
    return kb


@bot.message_handler(commands=['cars'])
def cars_cmd(message):
    chat_id = str(message.chat.id)
    cfg = get_chat_settings(chat_id)
    queue = fetch_queue()
    bot.reply_to(
        message,
        _cars_status_text(cfg, queue),
        reply_markup=_cars_keyboard(cfg),
        parse_mode='Markdown'
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("cars:"))
def handle_cars_callback(call):
    chat_id = str(call.message.chat.id)
    parts = call.data.split(":", 2)
    action = parts[1] if len(parts) > 1 else ""
    bot.answer_callback_query(call.id)

    cfg = get_chat_settings(chat_id)

    if action == "add":
        sent = bot.send_message(
            chat_id,
            "✏️ Введите номер автомобиля\n(например: *1234 AB-7* или *AB 1234-7*):",
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(sent, _receive_car_number)
        return

    if action == "del" and len(parts) == 3:
        norm_reg = parts[2]
        tracked = dict(cfg.get("tracked_cars", {}))
        if norm_reg in tracked:
            del tracked[norm_reg]
            update_chat_settings(chat_id, tracked_cars=tracked)
            cfg = get_chat_settings(chat_id)

    queue = fetch_queue()
    try:
        bot.edit_message_text(
            _cars_status_text(cfg, queue),
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=_cars_keyboard(cfg),
            parse_mode='Markdown'
        )
    except Exception:
        pass


def _receive_car_number(message):
    chat_id = str(message.chat.id)
    raw = (message.text or "").strip()
    norm = normalize_reg(raw)

    if len(norm) < 3 or len(norm) > 15:
        msg = bot.reply_to(
            message,
            "⚠️ Не похоже на номер авто. Попробуйте ещё раз\n(например: *1234 AB-7*):",
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, _receive_car_number)
        return

    cfg = get_chat_settings(chat_id)
    tracked = dict(cfg.get("tracked_cars", {}))

    if len(tracked) >= MAX_TRACKED_CARS:
        bot.reply_to(message, f"⚠️ Можно отслеживать не более {MAX_TRACKED_CARS} автомобилей. Удалите один через /cars.")
        return

    if norm in tracked:
        bot.reply_to(message, f"ℹ️ *{norm}* уже в списке отслеживания.", parse_mode='Markdown')
        return

    tracked[norm] = {"milestones_done": [], "called_notified": False}
    update_chat_settings(chat_id, tracked_cars=tracked)

    queue = fetch_queue()
    raw_live = (queue or {}).get("raw_live_queue", [])
    raw_prio = (queue or {}).get("raw_priority_queue", [])
    pos, is_called = find_car_in_queue(norm, raw_live, raw_prio)

    if is_called:
        pos_str = "🟢 уже вызван на пункт пропуска!"
    elif pos is not None:
        pos_str = f"сейчас на позиции *{pos}*"
    else:
        pos_str = "в очереди *не найден* (возможно ещё не зарегистрирован)"

    bot.reply_to(
        message,
        f"✅ *{norm}* добавлен.\n\n"
        f"Статус: {pos_str}\n\n"
        f"Уведомления придут на позициях:\n"
        f"300 → 200 → 100 → 50 → 25 → 10 → *вызван*",
        parse_mode='Markdown',
        reply_markup=_cars_keyboard(get_chat_settings(chat_id))
    )



@bot.message_handler(func=lambda m: True)
def fallback(message):
    bot.reply_to(message, "Напишите /check чтобы узнать очередь, или /help для справки.")


if __name__ == '__main__':
    bot.set_my_commands([
        telebot.types.BotCommand("check",         "🚗 Текущая очередь"),
        telebot.types.BotCommand("enable",        "✅ Включить уведомления"),
        telebot.types.BotCommand("disable",       "🔕 Выключить уведомления"),
        telebot.types.BotCommand("pause",         "⏸ Пауза уведомлений"),
        telebot.types.BotCommand("resume",        "▶️ Снять паузу"),
        telebot.types.BotCommand("setstep",       "📈 Шаг уведомлений"),
        telebot.types.BotCommand("setinterval",   "🕐 Частота проверки"),
        telebot.types.BotCommand("set_threshold", "🎯 Задать порог"),
        telebot.types.BotCommand("threshold",     "📊 Мои настройки"),
        telebot.types.BotCommand("cars",          "🚗 Отслеживать авто в очереди"),
        telebot.types.BotCommand("planner",       "📅 Планировщик выезда"),
        telebot.types.BotCommand("reset",         "🗑 Удалить уведомления и сбросить счётчики"),
        telebot.types.BotCommand("source",        "🔌 Статус источника данных"),
        telebot.types.BotCommand("help",          "ℹ️ Справка"),
    ])
    print("✅ Бот запущен. Напишите /start в Телеграме.")
    bot.infinity_polling(logger_level=None)
