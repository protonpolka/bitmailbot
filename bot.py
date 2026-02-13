import os
import logging
import asyncio
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.filters import CommandStart
from aiogram.fsm.storage.memory import MemoryStorage

from database import Database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

db = Database()


# ==================== КЛАВИАТУРЫ ====================

def user_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📧 Получить почту")],
            [KeyboardButton(text="📋 Мои почты")],
        ],
        resize_keyboard=True
    )


def admin_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📤 Загрузить почты"), KeyboardButton(text="👥 Пользователи")],
            [KeyboardButton(text="⚙️ Лимит"), KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="📧 Получить почту")],
            [KeyboardButton(text="📋 Мои почты")],
        ],
        resize_keyboard=True
    )


def kb(user_id: int):
    return admin_kb() if user_id == ADMIN_ID else user_kb()


# ==================== /start ====================

@router.message(CommandStart())
async def cmd_start(message: Message):
    uid = message.from_user.id
    await db.add_user(uid, message.from_user.username or "", message.from_user.full_name)

    if uid == ADMIN_ID:
        await message.answer(
            "👋 Привет, админ!\n\n"
            "📤 <b>Загрузить почты</b> — загрузить .txt файл\n"
            "👥 <b>Пользователи</b> — кто брал почты\n"
            "⚙️ <b>Лимит</b> — лимит почт/день\n"
            "📊 <b>Статистика</b> — общая инфо",
            parse_mode="HTML",
            reply_markup=admin_kb()
        )
    else:
        await message.answer(
            "👋 Привет! Я бот для выдачи почт.\n"
            "Нажми «📧 Получить почту»",
            reply_markup=user_kb()
        )


# ==================== ПОЛУЧИТЬ ПОЧТУ ====================

@router.message(F.text == "📧 Получить почту")
async def get_mail(message: Message):
    uid = message.from_user.id
    await db.add_user(uid, message.from_user.username or "", message.from_user.full_name)

    daily_limit = await db.get_daily_limit()
    today_count = await db.get_user_today_count(uid)

    if today_count >= daily_limit:
        await message.answer(
            f"⛔ Дневной лимит исчерпан ({daily_limit} почт/день).\nПопробуйте завтра!",
            reply_markup=kb(uid)
        )
        return

    mail = await db.take_mail(uid)
    if mail is None:
        await message.answer("😔 Свободных почт нет. Попробуйте позже.", reply_markup=kb(uid))
        return

    remaining = daily_limit - today_count - 1
    await message.answer(
        f"✅ Ваша почта:\n\n<code>{mail}</code>\n\n📌 Осталось сегодня: {remaining}/{daily_limit}",
        parse_mode="HTML",
        reply_markup=kb(uid)
    )


# ==================== МОИ ПОЧТЫ ====================

@router.message(F.text == "📋 Мои почты")
async def my_mails(message: Message):
    uid = message.from_user.id
    rows = await db.get_user_mails(uid)

    if not rows:
        await message.answer("У вас пока нет полученных почт.", reply_markup=kb(uid))
        return

    text = "📋 <b>Ваши почты:</b>\n\n"
    for row in rows[:20]:
        date_str = row['used_at'].strftime("%d.%m.%Y %H:%M")
        text += f"• <code>{row['mail']}</code> — {date_str}\n"

    if len(rows) > 20:
        text += f"\n... и ещё {len(rows) - 20}"

    await message.answer(text, parse_mode="HTML", reply_markup=kb(uid))


# ==================== АДМИН: ЗАГРУЗИТЬ ПОЧТЫ ====================

@router.message(F.text == "📤 Загрузить почты")
async def upload_prompt(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer(
        "📎 Отправьте .txt файл.\nФормат: <code>почта:пароль</code> на каждой строке.",
        parse_mode="HTML"
    )


@router.message(F.document)
async def handle_document(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    doc = message.document
    if not doc.file_name.endswith(".txt"):
        await message.answer("❌ Нужен .txt файл")
        return

    wait_msg = await message.answer("⏳ Загружаю...")

    file = await bot.download(doc)
    content = file.read().decode("utf-8", errors="ignore")
    lines = [line.strip() for line in content.splitlines() if line.strip() and ":" in line]

    if not lines:
        await wait_msg.edit_text("❌ Файл пустой или неверный формат.")
        return

    added, duplicates = await db.add_mails_bulk(lines)
    available = await db.count_available_mails()

    await wait_msg.edit_text(
        f"✅ Загружено: <b>{added}</b>\n"
        f"⚠️ Дубликаты: {duplicates}\n"
        f"📦 Всего доступно: {available}",
        parse_mode="HTML"
    )


# ==================== АДМИН: СТАТИСТИКА ====================

@router.message(F.text == "📊 Статистика")
async def stats(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    available = await db.count_available_mails()
    used = await db.count_used_mails()
    total_users = await db.count_users()
    daily_limit = await db.get_daily_limit()

    await message.answer(
        f"📊 <b>Статистика:</b>\n\n"
        f"📦 Доступно: {available}\n"
        f"✅ Выдано: {used}\n"
        f"👥 Пользователей: {total_users}\n"
        f"⚙️ Лимит: {daily_limit}/день",
        parse_mode="HTML",
        reply_markup=admin_kb()
    )


# ==================== АДМИН: ЛИМИТ ====================

@router.message(F.text == "⚙️ Лимит")
async def limit_settings(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    current = await db.get_daily_limit()
    buttons = []
    for val in [1, 2, 3, 5, 10, 20]:
        label = f"{'✅ ' if val == current else ''}{val}"
        buttons.append(InlineKeyboardButton(text=label, callback_data=f"lim_{val}"))

    keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons[:3], buttons[3:]])
    await message.answer(
        f"⚙️ Текущий лимит: <b>{current}</b>/день\nВыберите новый:",
        parse_mode="HTML",
        reply_markup=keyboard
    )


@router.callback_query(F.data.startswith("lim_"))
async def set_limit_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа")
        return

    val = int(callback.data.split("_")[1])
    await db.set_daily_limit(val)
    await callback.answer(f"✅ Лимит: {val}")
    await callback.message.edit_text(f"✅ Лимит установлен: <b>{val}</b>/день", parse_mode="HTML")


# ==================== АДМИН: ПОЛЬЗОВАТЕЛИ ====================

@router.message(F.text == "👥 Пользователи")
async def users_list(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    users = await db.get_active_users()
    if not users:
        await message.answer("Пока никто не брал почты.", reply_markup=admin_kb())
        return

    buttons = []
    for u in users:
        name = f"@{u['username']}" if u['username'] else u['full_name'] or str(u['user_id'])
        buttons.append([InlineKeyboardButton(
            text=f"{name} ({u['cnt']} почт)",
            callback_data=f"usr_{u['user_id']}"
        )])

    await message.answer(
        "👥 <b>Пользователи:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("usr_"))
async def user_periods(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔")
        return

    uid = int(callback.data.split("_")[1])
    info = await db.get_user_info(uid)
    name = f"@{info['username']}" if info['username'] else info['full_name'] or str(uid)

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    months = await db.get_user_active_months(uid)

    buttons = [
        [
            InlineKeyboardButton(text="📅 Сегодня", callback_data=f"pd_{uid}_d_{today}"),
            InlineKeyboardButton(text="📅 Вчера", callback_data=f"pd_{uid}_d_{yesterday}"),
        ],
        [InlineKeyboardButton(text="📅 Всё время", callback_data=f"pd_{uid}_a")],
    ]

    month_btns = [InlineKeyboardButton(text=f"📆 {m}", callback_data=f"pd_{uid}_m_{m}") for m in months]
    for i in range(0, len(month_btns), 2):
        buttons.append(month_btns[i:i+2])

    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_users")])

    await callback.message.edit_text(
        f"👤 <b>{name}</b>\nВыберите период:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("pd_"))
async def show_period_mails(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔")
        return

    parts = callback.data.split("_")
    # pd_{uid}_{type}_{value}
    uid = int(parts[1])
    ptype = parts[2]

    if ptype == "d":
        date = parts[3]
        rows = await db.get_user_mails_by_date(uid, date)
        title = f"📅 Почты за {date}"
    elif ptype == "m":
        month = parts[3]
        rows = await db.get_user_mails_by_month(uid, month)
        title = f"📆 Почты за {month}"
    elif ptype == "a":
        rows = await db.get_user_mails(uid)
        title = "📅 Все почты"
    else:
        return

    if not rows:
        await callback.answer("Нет почт за этот период")
        return

    text = f"<b>{title}</b> ({len(rows)} шт.)\n\n"
    for row in rows[:50]:
        date_str = row['used_at'].strftime("%d.%m.%Y %H:%M")
        text += f"• <code>{row['mail']}</code> — {date_str}\n"

    if len(rows) > 50:
        text += f"\n... и ещё {len(rows) - 50}"

    text = text[:4000]

    buttons = [[InlineKeyboardButton(text="🔙 Назад", callback_data=f"usr_{uid}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "back_users")
async def back_to_users(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return

    users = await db.get_active_users()
    if not users:
        await callback.message.edit_text("Пока никто не брал почты.")
        return

    buttons = []
    for u in users:
        name = f"@{u['username']}" if u['username'] else u['full_name'] or str(u['user_id'])
        buttons.append([InlineKeyboardButton(
            text=f"{name} ({u['cnt']} почт)",
            callback_data=f"usr_{u['user_id']}"
        )])

    await callback.message.edit_text(
        "👥 <b>Пользователи:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


# ==================== ЗАПУСК ====================

async def main():
    await db.connect()
    logger.info("БД подключена, бот запускается...")
    try:
        await dp.start_polling(bot)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
