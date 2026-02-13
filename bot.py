import os
import logging
import asyncio
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
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

def main_menu_kb(user_id: int):
    buttons = [
        [InlineKeyboardButton(text="📧 Получить почту", callback_data="get_mail")],
        [InlineKeyboardButton(text="📋 Мои почты", callback_data="my_mails")],
    ]
    if user_id == ADMIN_ID:
        buttons.append([InlineKeyboardButton(text="🔐 Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def home_kb(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")]
    ])


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Загрузить почты", callback_data="upload")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="users")],
        [InlineKeyboardButton(text="⚙️ Лимит почт/день", callback_data="limit")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
    ])


def back_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="admin")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
    ])


# ==================== /start ====================

@router.message(CommandStart())
async def cmd_start(message: Message):
    uid = message.from_user.id
    await db.add_user(uid, message.from_user.username or "", message.from_user.full_name)

    role = "👑 Админ" if uid == ADMIN_ID else "👤 Пользователь"
    await message.answer(
        f"👋 <b>Добро пожаловать!</b>\n\n"
        f"Ваша роль: {role}\n\n"
        f"Выберите действие:",
        parse_mode="HTML",
        reply_markup=main_menu_kb(uid)
    )


# ==================== ГЛАВНОЕ МЕНЮ ====================

@router.callback_query(F.data == "home")
async def go_home(callback: CallbackQuery):
    uid = callback.from_user.id
    role = "👑 Админ" if uid == ADMIN_ID else "👤 Пользователь"
    await callback.message.edit_text(
        f"🏠 <b>Главное меню</b>\n\n"
        f"Ваша роль: {role}\n\n"
        f"Выберите действие:",
        parse_mode="HTML",
        reply_markup=main_menu_kb(uid)
    )


# ==================== ПОЛУЧИТЬ ПОЧТУ ====================

@router.callback_query(F.data == "get_mail")
async def get_mail(callback: CallbackQuery):
    uid = callback.from_user.id
    await db.add_user(uid, callback.from_user.username or "", callback.from_user.full_name)

    daily_limit = await db.get_daily_limit()
    today_count = await db.get_user_today_count(uid)

    if today_count >= daily_limit:
        await callback.message.edit_text(
            f"⛔ <b>Лимит исчерпан</b>\n\n"
            f"Вы получили <b>{today_count}</b> из <b>{daily_limit}</b> почт сегодня.\n"
            f"Возвращайтесь завтра!",
            parse_mode="HTML",
            reply_markup=home_kb(uid)
        )
        return

    mail = await db.take_mail(uid)
    if mail is None:
        # Уведомляем пользователя
        admin_link = f"tg://user?id={ADMIN_ID}"
        await callback.message.edit_text(
            "😔 <b>Почты закончились</b>\n\n"
            "К сожалению, свободных почт сейчас нет.\n"
            f"Напишите <a href='{admin_link}'>админу</a> и попросите пополнить.",
            parse_mode="HTML",
            reply_markup=home_kb(uid)
        )
        # Уведомляем админа
        user_name = callback.from_user.username
        display = f"@{user_name}" if user_name else callback.from_user.full_name or f"ID:{uid}"
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🚨 <b>Почты закончились!</b>\n\n"
                f"Пользователь {display} попытался получить почту,\n"
                f"но свободных почт больше нет.\n\n"
                f"Загрузите новый .txt файл.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📤 Загрузить почты", callback_data="upload")],
                ])
            )
        except Exception:
            pass
        return

    # Проверяем остаток почт в базе и уведомляем админа
    LOW_STOCK_THRESHOLD = 10
    available = await db.count_available_mails()
    if available == LOW_STOCK_THRESHOLD:
        try:
            await bot.send_message(
                ADMIN_ID,
                f"⚠️ <b>Мало почт!</b>\n\n"
                f"Осталось всего <b>{available}</b> свободных почт.\n"
                f"Рекомендуется загрузить новые.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📤 Загрузить почты", callback_data="upload")],
                ])
            )
        except Exception:
            pass

    remaining = daily_limit - today_count - 1

    buttons = []
    if remaining > 0:
        buttons.append([InlineKeyboardButton(text=f"📧 Получить ещё ({remaining} осталось)", callback_data="get_mail")])
    buttons.append([InlineKeyboardButton(text="📋 Мои почты", callback_data="my_mails")])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")])

    await callback.message.edit_text(
        f"✅ <b>Почта получена!</b>\n\n"
        f"📧 <code>{mail}</code>\n\n"
        f"Использовано сегодня: <b>{today_count + 1}</b> из <b>{daily_limit}</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


# ==================== МОИ ПОЧТЫ ====================

@router.callback_query(F.data == "my_mails")
async def my_mails(callback: CallbackQuery):
    uid = callback.from_user.id
    rows = await db.get_user_mails(uid)

    if not rows:
        await callback.message.edit_text(
            "📋 <b>Мои почты</b>\n\n"
            "У вас пока нет полученных почт.\n"
            "Нажмите кнопку ниже чтобы получить первую!",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📧 Получить почту", callback_data="get_mail")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
            ])
        )
        return

    text = f"📋 <b>Мои почты</b> — всего <b>{len(rows)}</b>\n\n"
    for row in rows[:20]:
        date_str = row['used_at'].strftime("%d.%m.%Y %H:%M")
        text += f"📧 <code>{row['mail']}</code>\n   └ {date_str}\n\n"

    if len(rows) > 20:
        text += f"<i>... и ещё {len(rows) - 20}</i>"

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📧 Получить ещё", callback_data="get_mail")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ])
    )


# ==================== АДМИН-ПАНЕЛЬ ====================

@router.callback_query(F.data == "admin")
async def admin_panel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    available = await db.count_available_mails()
    used = await db.count_used_mails()
    limit = await db.get_daily_limit()

    await callback.message.edit_text(
        f"🔐 <b>Админ-панель</b>\n\n"
        f"📦 Доступно почт: <b>{available}</b>\n"
        f"✅ Выдано всего: <b>{used}</b>\n"
        f"⚙️ Лимит: <b>{limit}</b>/день\n\n"
        f"Выберите действие:",
        parse_mode="HTML",
        reply_markup=admin_kb()
    )


# ==================== ЗАГРУЗИТЬ ПОЧТЫ ====================

@router.callback_query(F.data == "upload")
async def upload_prompt(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    await callback.message.edit_text(
        "📤 <b>Загрузка почт</b>\n\n"
        "Отправьте <b>.txt файл</b> в этот чат.\n\n"
        "Формат — каждая строка:\n"
        "<code>email@example.com:password</code>\n\n"
        "Дубликаты будут автоматически пропущены.",
        parse_mode="HTML",
        reply_markup=back_admin_kb()
    )


@router.message(F.document)
async def handle_document(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    doc = message.document
    if not doc.file_name.endswith(".txt"):
        await message.answer(
            "❌ <b>Неверный формат файла</b>\n\n"
            "Поддерживается только <b>.txt</b>\n"
            "Переименуйте файл и попробуйте снова.",
            parse_mode="HTML",
            reply_markup=back_admin_kb()
        )
        return

    wait_msg = await message.answer("⏳ Обрабатываю файл...")

    file = await bot.download(doc)
    content = file.read().decode("utf-8", errors="ignore")
    lines = [line.strip() for line in content.splitlines() if line.strip() and ":" in line]

    if not lines:
        await wait_msg.edit_text(
            "❌ <b>Файл пустой или неверный формат</b>\n\n"
            "Не найдено строк в формате <code>почта:пароль</code>\n"
            "Проверьте содержимое файла.",
            parse_mode="HTML",
            reply_markup=back_admin_kb()
        )
        return

    added, duplicates = await db.add_mails_bulk(lines)
    available = await db.count_available_mails()

    await wait_msg.edit_text(
        f"✅ <b>Загрузка завершена!</b>\n\n"
        f"📥 Новых почт добавлено: <b>{added}</b>\n"
        f"⚠️ Дубликатов пропущено: <b>{duplicates}</b>\n\n"
        f"📦 Всего доступно сейчас: <b>{available}</b>",
        parse_mode="HTML",
        reply_markup=back_admin_kb()
    )


# ==================== СТАТИСТИКА ====================

@router.callback_query(F.data == "stats")
async def stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    available = await db.count_available_mails()
    used = await db.count_used_mails()
    total_users = await db.count_users()
    active_users = len(await db.get_active_users())
    daily_limit = await db.get_daily_limit()
    today_given = await db.count_today_given()

    await callback.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"<b>Почты:</b>\n"
        f"   📦 Доступно: <b>{available}</b>\n"
        f"   ✅ Выдано всего: <b>{used}</b>\n"
        f"   📅 Выдано сегодня: <b>{today_given}</b>\n\n"
        f"<b>Пользователи:</b>\n"
        f"   👥 Всего: <b>{total_users}</b>\n"
        f"   👤 Брали почту: <b>{active_users}</b>\n\n"
        f"<b>Настройки:</b>\n"
        f"   ⚙️ Лимит: <b>{daily_limit}</b> почт/день",
        parse_mode="HTML",
        reply_markup=back_admin_kb()
    )


# ==================== ЛИМИТ ====================

@router.callback_query(F.data == "limit")
async def limit_menu(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    current = await db.get_daily_limit()
    values = [1, 2, 3, 5, 10, 20, 50]

    buttons = []
    row = []
    for val in values:
        label = f"✅ {val}" if val == current else str(val)
        row.append(InlineKeyboardButton(text=label, callback_data=f"lim_{val}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="◀️ Админ-панель", callback_data="admin")])

    await callback.message.edit_text(
        f"⚙️ <b>Настройка лимита</b>\n\n"
        f"Сейчас каждый пользователь может получить\n"
        f"<b>{current}</b> почт в день.\n\n"
        f"Выберите новое значение:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("lim_"))
async def set_limit(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    val = int(callback.data.split("_")[1])
    old = await db.get_daily_limit()
    await db.set_daily_limit(val)

    values = [1, 2, 3, 5, 10, 20, 50]
    buttons = []
    row = []
    for v in values:
        label = f"✅ {v}" if v == val else str(v)
        row.append(InlineKeyboardButton(text=label, callback_data=f"lim_{v}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="◀️ Админ-панель", callback_data="admin")])

    if old == val:
        await callback.answer(f"Лимит уже {val}")
    else:
        await callback.answer(f"✅ Лимит изменён: {old} → {val}")

    await callback.message.edit_text(
        f"⚙️ <b>Настройка лимита</b>\n\n"
        f"✅ Лимит установлен: <b>{val}</b> почт/день\n\n"
        f"Можете выбрать другое значение:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


# ==================== ПОЛЬЗОВАТЕЛИ ====================

@router.callback_query(F.data == "users")
async def users_list(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    users = await db.get_active_users()
    if not users:
        await callback.message.edit_text(
            "👥 <b>Пользователи</b>\n\n"
            "Ещё никто не получал почты.",
            parse_mode="HTML",
            reply_markup=back_admin_kb()
        )
        return

    text = f"👥 <b>Пользователи</b> — <b>{len(users)}</b> чел.\n\n"
    text += "Нажмите на пользователя для деталей:\n"

    buttons = []
    for u in users:
        name = f"@{u['username']}" if u['username'] else u['full_name'] or f"ID:{u['user_id']}"
        buttons.append([InlineKeyboardButton(
            text=f"👤 {name} — {u['cnt']} почт",
            callback_data=f"usr_{u['user_id']}"
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Админ-панель", callback_data="admin")])

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


# ==================== ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ ====================

@router.callback_query(F.data.startswith("usr_"))
async def user_profile(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    uid = int(callback.data.split("_")[1])
    info = await db.get_user_info(uid)
    if not info:
        await callback.answer("Пользователь не найден")
        return

    name = f"@{info['username']}" if info['username'] else info['full_name'] or f"ID:{uid}"
    all_mails = await db.get_user_mails(uid)
    today_count = await db.get_user_today_count(uid)
    months = await db.get_user_active_months(uid)

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    text = (
        f"👤 <b>{name}</b>\n\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"📧 Всего получено: <b>{len(all_mails)}</b>\n"
        f"📅 Сегодня: <b>{today_count}</b>\n\n"
        f"Выберите период:"
    )

    buttons = [
        [
            InlineKeyboardButton(text="📅 Сегодня", callback_data=f"pd_{uid}_d_{today}"),
            InlineKeyboardButton(text="📅 Вчера", callback_data=f"pd_{uid}_d_{yesterday}"),
        ],
        [InlineKeyboardButton(text="📋 За всё время", callback_data=f"pd_{uid}_a")],
    ]

    if months:
        month_row = []
        for m in months[:6]:
            month_row.append(InlineKeyboardButton(text=f"📆 {m}", callback_data=f"pd_{uid}_m_{m}"))
            if len(month_row) == 2:
                buttons.append(month_row)
                month_row = []
        if month_row:
            buttons.append(month_row)

    buttons.append([InlineKeyboardButton(text="◀️ Пользователи", callback_data="users")])

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


# ==================== ПОЧТЫ ПО ПЕРИОДУ ====================

@router.callback_query(F.data.startswith("pd_"))
async def period_mails(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    parts = callback.data.split("_")
    uid = int(parts[1])
    ptype = parts[2]

    info = await db.get_user_info(uid)
    name = f"@{info['username']}" if info['username'] else info['full_name'] or f"ID:{uid}"

    if ptype == "d":
        date_str = parts[3]
        rows = await db.get_user_mails_by_date(uid, date_str)
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if date_str == today:
            period = "сегодня"
        elif date_str == yesterday:
            period = "вчера"
        else:
            period = date_str
        title = f"📅 Почты за {period}"
    elif ptype == "m":
        month = parts[3]
        rows = await db.get_user_mails_by_month(uid, month)
        title = f"📆 Почты за {month}"
    elif ptype == "a":
        rows = await db.get_user_mails(uid)
        title = "📋 Все почты"
    else:
        return

    if not rows:
        await callback.answer("Нет почт за этот период", show_alert=True)
        return

    text = f"👤 <b>{name}</b>\n{title} — <b>{len(rows)}</b> шт.\n\n"

    for row in rows[:40]:
        d = row['used_at'].strftime("%d.%m.%Y %H:%M")
        text += f"📧 <code>{row['mail']}</code>\n   └ {d}\n\n"

    if len(rows) > 40:
        text += f"<i>... и ещё {len(rows) - 40}</i>"

    text = text[:4000]

    buttons = [
        [InlineKeyboardButton(text=f"◀️ {name}", callback_data=f"usr_{uid}")],
        [InlineKeyboardButton(text="◀️ Пользователи", callback_data="users")],
    ]

    await callback.message.edit_text(
        text,
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
