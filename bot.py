import os
import re
import json
import time
import threading
import requests
import telebot

TOKEN = os.environ['TELEGRAM_BOT_TOKEN']

bot = telebot.TeleBot(TOKEN)

# belarusborder.by — electronic queue API (real-time, operated by Beltamozhservice)
BTS_API = "https://belarusborder.by/info/monitoring-new"
BTS_TOKEN = "test"
BREST_CHECKPOINT_ID = "a9173a85-3fc0-424c-84f0-defa632481e4"

SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "settings.json")
ALLOWED_INTERVALS = {1: "1 минута", 5: "5 минут", 15: "15 минут"}
TICK = 60  # main loop ticks every 60 s; per-user interval is checked individually

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
    "step": 50,           # notify every N-car increase
    "notified_step": 0,   # last step bucket we notified at (cars // step)
    "paused_until": 0,    # unix timestamp; 0 = not paused
}

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


# ── Data fetching ────────────────────────────────────────────────────────────

GPK_URL = "https://gpk.gov.by/situation-at-the-border/"
GPK_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'ru-RU,ru',
}


def _gpk_parse(html: str, title: str) -> str:
    m = re.search(
        r'<td[^>]*title="' + re.escape(title) + r'"[^>]*>\s*(.*?)\s*</td>',
        html, re.DOTALL
    )
    if not m:
        return "?"
    raw = re.sub(r'<[^>]+>', '', m.group(1)).strip()
    if not raw or raw == '-':
        return "0"
    n = re.search(r'\((\d+)\)', raw)
    return n.group(1) if n else raw


def _fetch_bts() -> dict | None:
    """Try belarusborder.by real-time API (short timeout — it may be unavailable)."""
    try:
        resp = requests.get(
            BTS_API,
            params={"token": BTS_TOKEN, "checkpointId": BREST_CHECKPOINT_ID},
            headers=HEADERS,
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()
        cars_live = len(data.get("carLiveQueue") or [])
        cars_prio = len(data.get("carPriority") or [])
        motos = (len(data.get("motorcycleLiveQueue") or [])
                 + len(data.get("motorcyclePriority") or []))
        return {
            "cars_total": cars_live + cars_prio,
            "cars_live":  cars_live,
            "cars_prio":  cars_prio,
            "motos":      motos,
            "trucks":     None,
            "buses":      None,
            "source":     "bts",
        }
    except Exception:
        return None


def _fetch_gpk() -> dict | None:
    """Fallback: gpk.gov.by HTML (updates ~every 2 h but always accessible)."""
    try:
        resp = requests.get(GPK_URL, headers=GPK_HEADERS, timeout=12)
        resp.raise_for_status()
        html = resp.text
        cars_str = _gpk_parse(html, "Брест: выезд легковых автомобилей")
        return {
            "cars_total": int(cars_str) if cars_str.isdigit() else 0,
            "cars_live":  int(cars_str) if cars_str.isdigit() else 0,
            "cars_prio":  0,
            "motos":      0,
            "trucks":     _gpk_parse(html, "Брест: выезд грузовых автомобилей"),
            "buses":      _gpk_parse(html, "Брест: выезд автобусов"),
            "source":     "gpk",
        }
    except Exception:
        return None


def fetch_queue() -> dict | None:
    """Try BTS real-time API first; fall back to GPK on any error."""
    return _fetch_bts() or _fetch_gpk()


def format_queue(q: dict) -> str:
    if q["source"] == "bts":
        lines = ["🚗 *Брест — очередь (выезд):*",
                 f"  Легковые: *{q['cars_total']}* шт."]
        if q["cars_prio"]:
            lines.append(f"  _(в т.ч. приоритет: {q['cars_prio']})_")
        if q["motos"]:
            lines.append(f"  Мотоциклы: *{q['motos']}* шт.")
        lines += ["", "_(belarusborder.by — реальное время)_"]
    else:
        lines = ["🚗 *Брест — очередь (выезд):*",
                 f"  Легковые: *{q['cars_total']}* шт."]
        if q["trucks"] is not None:
            lines.append(f"  Грузовики: *{q['trucks']}* шт.")
        if q["buses"] is not None:
            lines.append(f"  Автобусы:  *{q['buses']}* шт.")
        lines += ["", "_(gpk.gov.by — обновление ~раз в 2 ч)_"]
    return "\n".join(lines)


# ── Background monitor ───────────────────────────────────────────────────────
# Ticks every TICK seconds; each user is checked according to their own interval.

def monitor_loop():
    while True:
        time.sleep(TICK)
        now = time.time()

        with settings_lock:
            snapshot = {cid: dict(cfg) for cid, cfg in settings.items()}

        # Group enabled users by who is due for a check, to avoid redundant fetches
        due_chats = []
        for chat_id, cfg in snapshot.items():
            if not cfg.get("enabled"):
                continue
            interval_sec = cfg.get("interval", 5) * 60
            if now - cfg.get("last_checked", 0) >= interval_sec:
                due_chats.append((chat_id, cfg))

        if not due_chats:
            continue

        queue = fetch_queue()
        fetch_time = time.time()

        for chat_id, cfg in due_chats:
            update_chat_settings(chat_id, last_checked=fetch_time)

            # Skip if notifications are paused
            if fetch_time < cfg.get("paused_until", 0):
                continue

            if queue is None:
                continue
            cars_count = queue["cars_total"]

            updates = {}

            # ── Threshold alert ──────────────────────────────────────────
            threshold = cfg.get("threshold", 50)
            if cars_count >= threshold:
                try:
                    bot.send_message(
                        int(chat_id),
                        f"⚠️ *Внимание!* Очередь в Бресте ≥ порога *{threshold}* авто.\n\n"
                        + format_queue(queue),
                        parse_mode='Markdown'
                    )
                except Exception:
                    pass

            # ── Step alert ────────────────────────────────────────────────
            step = cfg.get("step", 50)
            notified_step = cfg.get("notified_step", 0)
            if step > 0:
                current_bucket = cars_count // step
                if current_bucket > notified_step:
                    # Notify for every newly crossed boundary
                    for bucket in range(notified_step + 1, current_bucket + 1):
                        milestone = bucket * step
                        try:
                            bot.send_message(
                                int(chat_id),
                                f"📈 *Очередь достигла {milestone} авто!*\n\n"
                                + format_queue(queue),
                                parse_mode='Markdown'
                            )
                        except Exception:
                            pass
                    updates["notified_step"] = current_bucket
                elif current_bucket < notified_step:
                    # Cars dropped — reset tracker so the next rise triggers again
                    updates["notified_step"] = current_bucket

            if updates:
                update_chat_settings(chat_id, **updates)


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
        "/check — проверить очередь прямо сейчас\n\n"
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


@bot.message_handler(commands=['set_threshold'])
def set_threshold(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(message, "⚠️ Укажите число, например: `/set_threshold 30`", parse_mode='Markdown')
        return
    try:
        value = int(parts[1])
        if value < 1 or value > 9999:
            raise ValueError
    except ValueError:
        bot.reply_to(message, "⚠️ Введите целое число от 1 до 9999.")
        return
    update_chat_settings(chat_id, threshold=value)
    cfg = get_chat_settings(chat_id)
    status = "включены" if cfg["enabled"] else "выключены"
    bot.reply_to(
        message,
        f"✅ Порог установлен: *{value}* легковых авто.\n"
        f"Уведомления сейчас: {status}.",
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

    update_chat_settings(chat_id, step=step_val, notified_step=0)

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


@bot.message_handler(func=lambda m: True)
def fallback(message):
    bot.reply_to(message, "Напишите /check чтобы узнать очередь, или /help для справки.")


if __name__ == '__main__':
    print("✅ Бот запущен. Напишите /start в Телеграме.")
    bot.infinity_polling(logger_level=None)
