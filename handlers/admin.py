import json
import logging
import os
import time

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import Config
from db import Database
from keyboards import admin_menu_keyboard, main_menu_keyboard
from tariffs import build_buy_text, get_all_active, get_by_id
from utils.helpers import notify_user, replace_message

logger = logging.getLogger(__name__)
router = Router()


class TariffEditFSM(StatesGroup):
    choosing = State()
    field = State()
    value = State()


TARIFF_FIELDS = {
    "name": ("Название", str),
    "price_rub": ("Цена (руб)", int),
    "duration_days": ("Дней", int),
    "ip_limit": ("Устройств", int),
    "traffic_gb": ("Трафик ГБ", float),
    "sort": ("Порядок", int),
    "description": ("Описание", str),
}


def is_admin(user_id: int) -> bool:
    return user_id in Config.ADMIN_USER_IDS


def tariffs_list_keyboard(plans):
    rows = []
    for plan in plans:
        status = "✅" if plan.get("active", True) else "❌"
        rows.append([
            InlineKeyboardButton(text=f"{status} {plan.get('name', plan['id'])}", callback_data=f"tedit:{plan['id']}"),
            InlineKeyboardButton(text="🔀", callback_data=f"ttoggle:{plan['id']}"),
        ])
    rows.append([InlineKeyboardButton(text="➕ Добавить тариф", callback_data="tadd")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def tariff_fields_keyboard(plan_id: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"✏️ {label}", callback_data=f"tfield:{plan_id}:{field}")]
        for field, (label, _) in TARIFF_FIELDS.items()
    ]
    rows.append([InlineKeyboardButton(text="🗑 Удалить тариф", callback_data=f"tdelete:{plan_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="tlist")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def save_tariffs(plans) -> None:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
        "tarifs.json",
    )
    with open(path, "w", encoding="utf-8") as file:
        json.dump({"plans": plans}, file, ensure_ascii=False, indent=2)
    from tariffs.loader import load_tariffs
    load_tariffs()


@router.message(F.text == "🛠️ Админ меню")
async def admin_menu(message: Message, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await replace_message(
            user_id,
            "⛔ У вас нет прав администратора.",
            reply_markup=main_menu_keyboard(False),
            delete_user_msg=message,
            bot=bot,
        )
        return
    await replace_message(
        user_id,
        "🛠️ <b>Админ панель</b>\n\nВыберите действие:",
        reply_markup=admin_menu_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(F.text == "📊 Статистика")
async def admin_stats(message: Message, db: Database, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    total_users = await db.get_total_users()
    subscribed = len(await db.get_subscribed_user_ids())
    banned = await db.get_banned_users_count()
    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"✅ Активных VPN: {subscribed}\n"
        f"⛔ Заблокировано: {banned}"
    )
    await replace_message(user_id, text, reply_markup=admin_menu_keyboard(), delete_user_msg=message, bot=bot)


@router.message(F.text == "💸 Запросы на вывод")
async def admin_withdraw_requests(message: Message, db: Database, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    requests = await db.get_pending_withdraw_requests()
    if not requests:
        await replace_message(
            user_id,
            "💸 Нет активных запросов на вывод.",
            reply_markup=admin_menu_keyboard(),
            delete_user_msg=message,
            bot=bot,
        )
        return

    await replace_message(
        user_id,
        "💸 Активные запросы на вывод:",
        reply_markup=admin_menu_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )
    for request in requests:
        text = (
            f"📋 <b>Запрос #{request['id']}</b>\n"
            f"👤 Пользователь: <code>{request['user_id']}</code>\n"
            f"💰 Сумма: {request['amount']} ₽\n"
            f"🕐 Создан: {request['created_at']}"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"withdraw_accept:{request['id']}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"withdraw_reject:{request['id']}"),
            ]]
        )
        await bot.send_message(user_id, text, reply_markup=keyboard)


@router.message(F.text == "📦 Создать тестовую подписку")
async def admin_test_subscription(message: Message, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    plans = get_all_active()
    text = build_buy_text(plans)
    keyboard = [[InlineKeyboardButton(text=plan.get("name", plan.get("id")), callback_data=f"test:{plan.get('id')}")] for plan in plans]
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")])
    await replace_message(
        user_id,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "back_to_admin")
async def back_to_admin(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    if is_admin(user_id):
        await callback.message.delete()
        await replace_message(
            user_id,
            "🛠️ <b>Админ панель</b>\n\nВыберите действие:",
            reply_markup=admin_menu_keyboard(),
            bot=bot,
        )
    await callback.answer()


@router.callback_query(F.data.startswith("withdraw_accept:"))
async def withdraw_accept(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    request_id = int(callback.data.split(":", 1)[1])
    request = await db.get_withdraw_request(request_id)
    success = await db.process_withdraw_request(request_id, accept=True)
    if success:
        await callback.message.edit_text(callback.message.text + "\n\n✅ <b>ВЫВОД ПОДТВЕРЖДЁН</b>", parse_mode="HTML")
        if request:
            await notify_user(
                int(request["user_id"]),
                (
                    "✅ <b>Ваш запрос на вывод подтверждён.</b>\n\n"
                    f"🆔 Запрос: <code>{request_id}</code>\n"
                    f"💰 Сумма: <b>{float(request['amount']):.2f} ₽</b>"
                ),
                bot=callback.bot,
            )
        await callback.answer("Вывод подтверждён")
        return
    await callback.answer("Ошибка обработки запроса", show_alert=True)


@router.callback_query(F.data.startswith("withdraw_reject:"))
async def withdraw_reject(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    request_id = int(callback.data.split(":", 1)[1])
    request = await db.get_withdraw_request(request_id)
    success = await db.process_withdraw_request(request_id, accept=False)
    if success:
        await callback.message.edit_text(callback.message.text + "\n\n❌ <b>ВЫВОД ОТКЛОНЁН</b>", parse_mode="HTML")
        if request:
            await notify_user(
                int(request["user_id"]),
                (
                    "❌ <b>Ваш запрос на вывод отклонён.</b>\n\n"
                    f"🆔 Запрос: <code>{request_id}</code>\n"
                    "Средства остались на вашем балансе."
                ),
                bot=callback.bot,
            )
        await callback.answer("Вывод отклонён")
        return
    await callback.answer("Ошибка обработки запроса", show_alert=True)


@router.message(F.text == "📋 Тарифы")
async def admin_tariffs_list(message: Message, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    from tariffs.loader import TARIFFS_ALL
    await replace_message(
        user_id,
        "📋 <b>Редактор тарифов</b>\n\nВыберите тариф для редактирования:",
        reply_markup=tariffs_list_keyboard(list(TARIFFS_ALL)),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "tlist")
async def tariffs_list_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    from tariffs.loader import TARIFFS_ALL
    await callback.message.edit_text(
        "📋 <b>Редактор тарифов</b>\n\nВыберите тариф для редактирования:",
        reply_markup=tariffs_list_keyboard(list(TARIFFS_ALL)),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tedit:"))
async def tariff_edit_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    plan_id = callback.data.split(":", 1)[1]
    plan = get_by_id(plan_id)
    if not plan:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    lines = [f"✏️ <b>Тариф: {plan.get('name', plan_id)}</b>\n"]
    for key, (label, _) in TARIFF_FIELDS.items():
        lines.append(f"{label}: <b>{plan.get(key, '—')}</b>")
    await callback.message.edit_text("\n".join(lines), reply_markup=tariff_fields_keyboard(plan_id), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data.startswith("ttoggle:"))
async def tariff_toggle(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    from tariffs.loader import TARIFFS_ALL
    plan_id = callback.data.split(":", 1)[1]
    plans = list(TARIFFS_ALL)
    for plan in plans:
        if plan.get("id") == plan_id:
            plan["active"] = not plan.get("active", True)
            await callback.answer("Тариф включён" if plan["active"] else "Тариф выключен")
            break
    save_tariffs(plans)
    from tariffs.loader import TARIFFS_ALL as reloaded_plans
    await callback.message.edit_text(
        "📋 <b>Редактор тарифов</b>\n\nВыберите тариф для редактирования:",
        reply_markup=tariffs_list_keyboard(list(reloaded_plans)),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("tfield:"))
async def tariff_field_select(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    _, plan_id, field = callback.data.split(":", 2)
    label = TARIFF_FIELDS.get(field, (field,))[0]
    await state.set_state(TariffEditFSM.value)
    await state.update_data(plan_id=plan_id, field=field, msg_id=callback.message.message_id)
    await callback.message.edit_text(
        f"✏️ Введите новое значение для поля <b>{label}</b>:\n(отправьте /cancel для отмены)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data=f"tedit:{plan_id}")]]
        ),
    )
    await callback.answer()


@router.message(TariffEditFSM.value)
async def tariff_field_value(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    if message.text == "/cancel":
        await state.clear()
        await message.delete()
        return

    data = await state.get_data()
    plan_id = data["plan_id"]
    field = data["field"]
    _, cast = TARIFF_FIELDS[field]
    try:
        value = cast(message.text.strip())
    except (TypeError, ValueError):
        await message.answer("❌ Неверный формат. Попробуйте ещё раз.")
        return

    from tariffs.loader import TARIFFS_ALL
    plans = list(TARIFFS_ALL)
    for plan in plans:
        if plan.get("id") == plan_id:
            plan[field] = value
            break
    save_tariffs(plans)
    await state.clear()
    try:
        await message.delete()
    except Exception as exc:
        logger.debug("Не удалось удалить сообщение редактирования тарифа: %s", exc)

    plan = get_by_id(plan_id)
    lines = [f"✅ Сохранено!\n\n✏️ <b>Тариф: {plan.get('name', plan_id)}</b>\n"]
    for key, (label, _) in TARIFF_FIELDS.items():
        lines.append(f"{label}: <b>{plan.get(key, '—')}</b>")
    await bot.send_message(user_id, "\n".join(lines), reply_markup=tariff_fields_keyboard(plan_id), parse_mode="HTML")


@router.callback_query(F.data.startswith("tdelete:"))
async def tariff_delete(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    from tariffs.loader import TARIFFS_ALL
    plan_id = callback.data.split(":", 1)[1]
    save_tariffs([plan for plan in TARIFFS_ALL if plan.get("id") != plan_id])
    from tariffs.loader import TARIFFS_ALL as reloaded_plans
    await callback.message.edit_text(
        "🗑 Тариф удалён.\n\n📋 <b>Редактор тарифов</b>:",
        reply_markup=tariffs_list_keyboard(list(reloaded_plans)),
        parse_mode="HTML",
    )
    await callback.answer("Удалено")


@router.callback_query(F.data == "tadd")
async def tariff_add(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    new_id = f"plan_{int(time.time())}"
    from tariffs.loader import TARIFFS_ALL
    plans = list(TARIFFS_ALL)
    plans.append(
        {
            "id": new_id,
            "name": "Новый тариф",
            "active": False,
            "price_rub": 0,
            "duration_days": 30,
            "ip_limit": 1,
            "traffic_gb": 50,
            "sort": 999,
            "description": "",
        }
    )
    save_tariffs(plans)
    await callback.message.edit_text(
        "➕ Тариф создан (выключен). Отредактируйте его:",
        reply_markup=tariff_fields_keyboard(new_id),
        parse_mode="HTML",
    )
    await callback.answer()
