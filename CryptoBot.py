import asyncio
import logging
import os
import random
from datetime import datetime, timezone

import aiosqlite
import httpx
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import Message, TelegramObject, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.client.default import DefaultBotProperties
from aiohttp import web

# ==========================================
# НАСТРОЙКИ
# ==========================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")

ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
CHECK_INTERVAL = 10
# ==========================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
DB_PATH = "alerts.db"

_client = None

def get_client():
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=10)
    return _client

async def close_client():
    global _client
    if _client:
        await _client.aclose()
        _client = None

def get_headers():
    return {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Cache-Control": "no-cache", "Pragma": "no-cache"}

# --- ЛОГИРОВАНИЕ ОШИБОК В БАЗУ ---
async def log_error_to_db(error_text: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO errors (error_text, created_at) VALUES (?, ?)", (error_text, datetime.now(timezone.utc)))
        await db.commit()
    log.error(f"Error saved to DB: {error_text}")

# --- БИРЖИ ---
async def get_price(symbol: str) -> float:
    rnd = random.randint(1, 999999)
    try:
        r = await get_client().get(f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200: return float(r.json()["result"]["list"][0]["lastPrice"])
    except Exception: pass
    try:
        r = await get_client().get(f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200: return float(r.json()["price"])
    except Exception: pass
    try:
        okx_sym = "TON-USDT" if symbol == "TONUSDT" else symbol.replace("USDT", "-USDT")
        r = await get_client().get(f"https://www.okx.com/api/v5/market/ticker?instId={okx_sym}&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200: return float(r.json()["data"][0]["last"])
    except Exception: pass
    try:
        mexc_sym = "GRAMUSDT" if symbol == "TONUSDT" else symbol
        r = await get_client().get(f"https://api.mexc.com/api/v3/ticker/price?symbol={mexc_sym}&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200: return float(r.json()["price"])
    except Exception: pass
    try:
        gate_sym = symbol.replace("USDT", "_USDT")
        r = await get_client().get(f"https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={gate_sym}&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200: return float(r.json()[0]["last"])
    except Exception: pass
    
    await log_error_to_db(f"Не удалось получить цену для {symbol}")
    raise Exception("Все биржи недоступны или монета не найдена")

async def get_funding(symbol: str) -> dict:
    rnd = random.randint(1, 999999)
    try:
        r = await get_client().get(f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200:
            data = r.json()["result"]["list"][0]
            return {"mark": float(data["markPrice"]), "funding_pct": float(data["fundingRate"]) * 100, "next_funding_ms": int(data["nextFundingTime"])}
    except Exception: pass
    try:
        r = await get_client().get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200:
            d = r.json()
            return {"mark": float(d["markPrice"]), "funding_pct": float(d["lastFundingRate"]) * 100, "next_funding_ms": int(d["nextFundingTime"])}
    except Exception: pass
    try:
        okx_sym = "TON-USDT" if symbol == "TONUSDT" else symbol.replace("USDT", "-USDT")
        r = await get_client().get(f"https://www.okx.com/api/v5/public/funding-rate?instId={okx_sym}&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200:
            data = r.json()["data"][0]
            return {"mark": float(data["markPx"]), "funding_pct": float(data["fundingRate"]) * 100, "next_funding_ms": int(data["fundingTime"])}
    except Exception: pass
    try:
        gate_sym = symbol.replace("USDT", "_USDT")
        r = await get_client().get(f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{gate_sym}?t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200:
            d = r.json()
            return {"mark": float(d["mark_price"]), "funding_pct": float(d["funding_rate"]) * 100, "next_funding_ms": int(d["funding_next_apply"]) * 1000}
    except Exception: pass
    try:
        mexc_sym = "GRAMUSDT" if symbol == "TONUSDT" else symbol
        r = await get_client().get(f"https://api.mexc.com/api/v3/ticker/price?symbol={mexc_sym}&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200: 
            return {"mark": float(r.json()["price"]), "funding_pct": 0.0, "next_funding_ms": 0}
    except Exception: pass
    
    await log_error_to_db(f"Не удалось получить фандинг для {symbol}")
    raise Exception("Не удалось получить данные о фандинге ни с одной биржи")

async def get_long_short_ratio(symbol: str) -> str:
    rnd = random.randint(1, 999999)
    lsr_text = "Long/Short: данные недоступны"
    
    try:
        r = await get_client().get(f"https://api.bybit.com/v5/market/account-ratio?category=linear&symbol={symbol}&period=5min&t={rnd}", headers=get_headers(), timeout=3)
        if r.status_code == 200:
            data = r.json()["result"]["list"][0]
            lsr_text = f"Long/Short (Bybit): {float(data['buyRatio'])*100:.1f}% лонгов / {float(data['sellRatio'])*100:.1f}% шортов"
    except Exception: pass

    if "недоступны" in lsr_text:
        try:
            r = await get_client().get(f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={symbol}&period=5m&limit=1&t={rnd}", headers=get_headers(), timeout=3)
            if r.status_code == 200:
                data = r.json()[0]
                lsr_text = f"Long/Short (Binance): {float(data['longAccount'])*100:.1f}% лонгов / {float(data['shortAccount'])*100:.1f}% шортов"
        except Exception: pass

    return lsr_text

# --- БАЗА ДАННЫХ ---
SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, kind TEXT NOT NULL, symbol TEXT NOT NULL, op TEXT NOT NULL, threshold REAL NOT NULL, triggered INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS trackers (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, symbol TEXT NOT NULL, base_price REAL NOT NULL, step TEXT NOT NULL, last_notified REAL NOT NULL);
CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS errors (id INTEGER PRIMARY KEY AUTOINCREMENT, error_text TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()

async def add_user(user_id, username, first_name):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)", (user_id, username, first_name))
        await db.commit()

async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, username, first_name FROM users")
        return await cur.fetchall()

async def add_alert(user_id, kind, symbol, op, threshold) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("INSERT INTO alerts (user_id, kind, symbol, op, threshold) VALUES (?, ?, ?, ?, ?)", (user_id, kind, symbol, op, threshold))
        await db.commit()
        return cur.lastrowid

async def get_active_alerts(user_id=None):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if user_id is None: cur = await db.execute("SELECT * FROM alerts WHERE triggered = 0")
        else: cur = await db.execute("SELECT * FROM alerts WHERE triggered = 0 AND user_id = ?", (user_id,))
        return await cur.fetchall()

async def mark_triggered(alert_id, user_id=None):
    async with aiosqlite.connect(DB_PATH) as db:
        if user_id is None: await db.execute("UPDATE alerts SET triggered = 1 WHERE id = ?", (alert_id,))
        else: await db.execute("UPDATE alerts SET triggered = 1 WHERE id = ? AND user_id = ?", (alert_id, user_id))
        await db.commit()

async def add_tracker(user_id, symbol, base_price, step) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("INSERT INTO trackers (user_id, symbol, base_price, step, last_notified) VALUES (?, ?, ?, ?, ?)", (user_id, symbol, base_price, step, base_price))
        await db.commit()
        return cur.lastrowid

async def get_trackers(user_id=None):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if user_id is None: cur = await db.execute("SELECT * FROM trackers")
        else: cur = await db.execute("SELECT * FROM trackers WHERE user_id = ?", (user_id,))
        return await cur.fetchall()

async def update_tracker_last(tr_id, price):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE trackers SET last_notified = ? WHERE id = ?", (price, tr_id))
        await db.commit()

async def del_tracker(tr_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM trackers WHERE id = ? AND user_id = ?", (tr_id, user_id))
        await db.commit()

async def reset_db_ids():
    async with aiosqlite.connect(DB_PATH) as db:
        count_alerts = (await (await db.execute("SELECT COUNT(*) FROM alerts WHERE triggered = 0")).fetchone())[0]
        if count_alerts == 0: await db.execute("DELETE FROM sqlite_sequence WHERE name='alerts'")
        count_trackers = (await (await db.execute("SELECT COUNT(*) FROM trackers")).fetchone())[0]
        if count_trackers == 0: await db.execute("DELETE FROM sqlite_sequence WHERE name='trackers'")
        await db.commit()

# --- ХЕЛПЕРЫ ---
def norm_symbol(raw: str) -> str:
    s = raw.upper()
    if s in ("GOLD", "ЗОЛОТО", "XAU"): return "PAXGUSDT"
    if s in ("GRAM", "TON"): return "TONUSDT"
    return s if s.endswith("USDT") else s + "USDT"

def check_op(value, op, threshold):
    return value > threshold if op == ">" else value < threshold

def fmt_ts(ms):
    if ms == 0: return "Неизвестно"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# --- КЛАВИАТУРЫ ---
def price_menu():
    kb = [
        [InlineKeyboardButton(text="🟡 BTC", callback_data="price_BTC"), InlineKeyboardButton(text="🔵 ETH", callback_data="price_ETH")],
        [InlineKeyboardButton(text="💎 GRAM", callback_data="price_GRAM"), InlineKeyboardButton(text="🟣 SOL", callback_data="price_SOL")],
        [InlineKeyboardButton(text="🥇 Золото", callback_data="price_GOLD")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def back_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="back_to_help")]])

HELP_TEXT = (
    "криптопенис эрегирует!\n\n"
    "Цены:\n"
    "/price BTC — цена любой монеты\n\n"
    "Фандинг (ставка за перенос):\n"
    "/funding BTC — узнать фандинг и соотношение лонг/шорт\n"
    "(Если фандинг положительный — лонги платят шортам. Если отрицательный — наоборот. Высокий фандинг = рынок перегрет)\n\n"
    "Алерты (уведомления):\n"
    "/alert price BTC > 80000 — пришлет пуш, если цена пересечет уровень\n"
    "/alert funding BTC > 0.05 — пуш по фандингу\n"
    "/alerts — список активных алертов\n"
    "/del 1 — удалить алерт по номеру\n\n"
    "Трекеры (следят за изменением):\n"
    "/track BTC 81520 5% — следить за изменением на 5%\n"
    "(Если не знаешь цену, пиши 0 вместо 81520, бот сам возьмет текущую)\n"
    "Шаги: 10s (мин. слежка - каждые 10 сек), 1m (1 мин), 1h (1 час), 5% (5%), 100 (на 100)\n"
    "/trackers — список трекеров\n"
    "/deltrack 1 — удалить трекер\n\n"
    "Узнать цену:"
)

# --- МИДЛВАРЬ ДЛЯ ЮЗЕРОВ ---
class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        user = data.get("event_from_user")
        if user and not user.is_bot:
            await add_user(user.id, user.username, user.first_name)
        return await handler(event, data)

dp.message.middleware(UserMiddleware())
dp.callback_query.middleware(UserMiddleware())

# --- ХЭНДЛЕРЫ ---
@dp.message(Command("start"))
@dp.message(Command("help"))
async def cmd_start(m: Message):
    await m.answer(HELP_TEXT, reply_markup=price_menu())

@dp.message(Command("admin"))
async def cmd_admin(m: Message):
    if m.from_user.id != ADMIN_ID: return
    users = await get_all_users()
    text = f"Пользователей: {len(users)}\n\nПоследние 10:\n"
    for u in users[-10:]:
        text += f"{u[0]} @{u[1]} ({u[2]})\n"
    text += "\nКоманды:\n/users - весь список\n/broadcast текст - рассылка\n/send ID текст - лично\n/errors - ошибки"
    await m.answer(text)

@dp.message(Command("users"))
async def cmd_users(m: Message):
    if m.from_user.id != ADMIN_ID: return
    users = await get_all_users()
    text = "Все пользователи:\n"
    for u in users:
        text += f"{u[0]} @{u[1]} ({u[2]})\n"
    await m.answer(text)

@dp.message(Command("broadcast"))
async def cmd_broadcast(m: Message, command: CommandObject):
    if m.from_user.id != ADMIN_ID: return
    if not command.args: return await m.answer("Формат: /broadcast текст")
    users = await get_all_users()
    count = 0
    for u in users:
        try:
            await bot.send_message(u[0], command.args)
            count += 1
            await asyncio.sleep(0.1)
        except Exception: pass
    await m.answer(f"Отправлено {count} юзерам.")

@dp.message(Command("send"))
async def cmd_send(m: Message, command: CommandObject):
    if m.from_user.id != ADMIN_ID: return
    if not command.args: return await m.answer("Формат: /send ID текст")
    args = command.args.split(maxsplit=1)
    if len(args) < 2: return await m.answer("Формат: /send ID текст")
    try:
        await bot.send_message(int(args[0]), args[1])
        await m.answer("Отправлено!")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@dp.message(Command("errors"))
async def cmd_errors(m: Message):
    if m.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT error_text, created_at FROM errors ORDER BY id DESC LIMIT 10")
        rows = await cur.fetchall()
    if not rows: return await m.answer("Ошибок нет!")
    text = "Последние 10 ошибок:\n\n"
    for r in rows:
        text += f"{r[1]}: {r[0]}\n"
    await m.answer(text)

# --- КОЛБЭКИ ---
@dp.callback_query(F.data.startswith("price_"))
async def callback_price(c: CallbackQuery):
    sym_name = c.data.split("_")[1]
    sym = "PAXGUSDT" if sym_name == "GOLD" else ("TONUSDT" if sym_name == "GRAM" else f"{sym_name}USDT")
    try:
        p = await get_price(sym)
        await c.message.edit_text(f"{sym_name}: {p}", reply_markup=back_menu())
    except Exception as e:
        await c.message.edit_text(f"Ошибка: {e}", reply_markup=back_menu())
    await c.answer()

@dp.callback_query(F.data == "back_to_help")
async def callback_back(c: CallbackQuery):
    await c.message.edit_text(HELP_TEXT, reply_markup=price_menu())
    await c.answer()

@dp.message(Command("price"))
async def cmd_price(m: Message):
    parts = m.text.split()
    if len(parts) < 2: return await m.answer("Формат: /price BTC", reply_markup=back_menu())
    sym = norm_symbol(parts[1])
    try: await m.answer(f"{sym}: {await get_price(sym)}", reply_markup=back_menu())
    except Exception as e: await m.answer(f"Ошибка: {e}", reply_markup=back_menu())

@dp.message(Command("funding"))
async def cmd_funding(m: Message):
    parts = m.text.split()
    if len(parts) < 2: return await m.answer("Формат: /funding BTC", reply_markup=back_menu())
    sym = norm_symbol(parts[1])
    try:
        f = await get_funding(sym)
        ratio = await get_long_short_ratio(sym)
        f_text = "Лонги платят шортам" if f['funding_pct'] > 0 else ("Шорты платят лонгам" if f['funding_pct'] < 0 else "Ставка 0")
        await m.answer(f"{sym}\nЦена: {f['mark']}\nФандинг: {f['funding_pct']:.4f}% ({f_text})\nВремя: {fmt_ts(f['next_funding_ms'])}\n\n{ratio}", reply_markup=back_menu())
    except Exception as e: await m.answer(f"Ошибка: {e}", reply_markup=back_menu())

@dp.message(Command("alert"))
async def cmd_alert(m: Message):
    parts = m.text.split()
    if len(parts) != 5: return await m.answer("Формат: /alert price BTC > 3000", reply_markup=back_menu())
    kind, sym_raw, op, val = parts[1].lower(), parts[2], parts[3], parts[4]
    if kind not in ("price", "funding"): return await m.answer("kind: price или funding", reply_markup=back_menu())
    if op not in (">", "<"): return await m.answer("op: > или <", reply_markup=back_menu())
    try: threshold = float(val)
    except ValueError: return await m.answer("value должно быть числом", reply_markup=back_menu())
    sym = norm_symbol(sym_raw)
    aid = await add_alert(m.from_user.id, kind, sym, op, threshold)
    await m.answer(f"Алерт #{aid} создан: {kind} {sym} {op} {threshold}", reply_markup=back_menu())

@dp.message(Command("alerts"))
async def cmd_alerts(m: Message):
    rows = await get_active_alerts(m.from_user.id)
    if not rows: return await m.answer("Активных алертов нет", reply_markup=back_menu())
    await m.answer("\n".join([f"#{r['id']} {r['kind']} {r['symbol']} {r['op']} {r['threshold']}" for r in rows]), reply_markup=back_menu())

@dp.message(Command("del"))
async def cmd_del(m: Message):
    parts = m.text.split()
    if len(parts) != 2: return await m.answer("Формат: /del <id>", reply_markup=back_menu())
    await mark_triggered(int(parts[1]), m.from_user.id)
    await reset_db_ids()
    await m.answer(f"Алерт #{parts[1]} удален", reply_markup=back_menu())

@dp.message(Command("track"))
async def cmd_track(m: Message):
    parts = m.text.split()
    if len(parts) != 4: return await m.answer("Формат: /track BTC 81520 5% (если не знаешь цену, пиши 0)", reply_markup=back_menu())
    sym_raw, base_price_str, step = parts[1], parts[2], parts[3].lower()
    try: base_price = float(base_price_str)
    except: return await m.answer("Базовая цена должна быть числом", reply_markup=back_menu())
    sym = norm_symbol(sym_raw)
    if base_price == 0:
        try: base_price = await get_price(sym)
        except Exception as e: return await m.answer(f"Ошибка получения цены: {e}", reply_markup=back_menu())
    tid = await add_tracker(m.from_user.id, sym, base_price, step)
    await m.answer(f"Трекер #{tid} создан!\nСлежу за {sym} от {base_price}, шаг: {step}", reply_markup=back_menu())

@dp.message(Command("trackers"))
async def cmd_trackers(m: Message):
    rows = await get_trackers(m.from_user.id)
    if not rows: return await m.answer("Нет активных трекеров", reply_markup=back_menu())
    lines = [f"#{r['id']} {r['symbol']} от {r['base_price']}, шаг {r['step']}" for r in rows]
    await m.answer("Твои трекеры:\n" + "\n".join(lines) + "\n\nУдалить: /deltrack <id>", reply_markup=back_menu())

@dp.message(Command("deltrack"))
async def cmd_deltrack(m: Message):
    parts = m.text.split()
    if len(parts) != 2: return await m.answer("Формат: /deltrack <id>", reply_markup=back_menu())
    await del_tracker(int(parts[1]), m.from_user.id)
    await reset_db_ids()
    await m.answer(f"Трекер #{parts[1]} удален", reply_markup=back_menu())

# --- ФОНОВЫЙ ЦИКЛ ---
async def checker_loop():
    log.info("Checker loop started")
    while True:
        try:
            for a in await get_active_alerts():
                try:
                    if a["kind"] == "price": value = await get_price(a["symbol"])
                    else: value = (await get_funding(a["symbol"]))["funding_pct"]
                    if check_op(value, a["op"], a["threshold"]):
                        await bot.send_message(a["user_id"], f"Алерт #{a['id']}: {a['kind']} {a['symbol']} = {value}")
                        await mark_triggered(a["id"])
                        await reset_db_ids()
                except Exception as e: 
                    await log_error_to_db(f"Alert {a['id']} failed: {e}")

            for t in await get_trackers():
                try:
                    curr_price = await get_price(t["symbol"])
                    last_price = t["last_notified"]
                    step = t["step"]
                    notify = False
                    
                    if step.endswith("s") or step.endswith("m") or step.endswith("h"):
                        if curr_price != last_price: notify = True
                    elif step.endswith("%"):
                        pct = float(step.replace("%", "")) / 100
                        if abs(curr_price - last_price) / last_price >= pct: notify = True
                    else:
                        try:
                            flat_step = float(step)
                            if abs(curr_price - last_price) >= flat_step: notify = True
                        except: pass

                    if notify:
                        await bot.send_message(t["user_id"], f"{t['symbol']}: {curr_price} (шаг: {step})")
                        await update_tracker_last(t["id"], curr_price)
                except Exception as e: 
                    await log_error_to_db(f"Tracker {t['id']} failed: {e}")
                
        except Exception: 
            await log_error_to_db(f"checker_loop failed")
        await asyncio.sleep(CHECK_INTERVAL)

# --- ЗАПУСК ---
async def handle_ping(request):
    return web.Response(text="Bot is alive!")

async def start_dummy_server():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()

async def main():
    await init_db()
    asyncio.create_task(checker_loop())
    asyncio.create_task(start_dummy_server())
    try: await dp.start_polling(bot)
    finally: await close_client(); await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
