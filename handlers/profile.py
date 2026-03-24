import logging
from datetime import datetime, timedelta
from typing import Optional

from aiogram import Router, F, Bot
from aiogram.types import (
    CallbackQuery, Message,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.enums import ParseMode

from config import Config
from db import Database
from tariffs import get_by_id, format_traffic, format_duration
from keyboards import subscriptions_keyboard
from utils.helpers import replace_message, get_visible_plans
from services.subscriptions import create_subscription, is_active_subscription, get_subscription_status
from services.panel import PanelAPI

logger = logging.getLogger(__name__)
router = Router()


@router.message(F.text == "📦 Подписки")
async def subscriptions_menu(message: Message, db: Database, panel: PanelAPI):
    user_id = message.from_user.id
    await db.add_user(user_id)

    active = await is_active_subscription(user_id, db=db, panel=panel)
    user_data = await db.get_user(user_id)

    if not active and user_data.get("trial_used") == 0 and user_data.get("trial_declined") == 0:
        trial_plan = get_by_id("trial")
        if trial_plan and trial_plan.get("active"):
            text = (
                "🎁 <b>Пробный период!</b>\n\n"
                "Новым пользователям доступен пробный тариф:\n"
                f"✅ <b>{trial_plan.get('name', 'Пробный')}</b>\n"
                f"📦 Трафик: {format_traffic(trial_plan.get('traffic_gb', 10))}\n"
                f"📱 Устройств: до {trial_plan.get('ip_limit', 1)}\n"
                f"⏱ Срок: {format_duration(trial_plan.get('duration_days', 3))}\n\n"
                "Хотите попробовать?"
            )
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Попробовать", callback_data="trial_accept")],
                    [InlineKeyboardButton(text="❌ Отказаться", callback_data="trial_decline")],
                ]
            )
            await replace_message(user_id, text, reply_markup=keyboard, delete_user_msg=message, bot=message.bot)
            return

    await show_available_tariffs(user_id, active, db=db, panel=panel, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "trial_accept")
async def trial_accept(callback: CallbackQuery, db: Database, panel: PanelAPI):
    user_id = callback.from_user.id

    user_data = await db.get_user(user_id)
    if user_data.get("trial_used"):
        await callback.answer("❌ Пробный период уже использован.", show_alert=True)
        return

    trial_plan = get_by_id("trial")
    if not trial_plan or not trial_plan.get("active"):
        await callback.answer("❌ Пробный тариф недоступен.", show_alert=True)
        return

    try:
        await callback.message.delete()
    except Exception as e:
        logger.warning("Не удалось удалить сообщение trial: %s", e)

    vpn_url = await create_subscription(
        user_id,
        trial_plan,
        db=db,
        panel=panel,
        plan_suffix=" (пробный)",
    )

    if vpn_url:
        await db.update_user(user_id, trial_used=1)
        await db.set_has_subscription(user_id)

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])

        await callback.message.answer(
            f"✅ <b>Пробный период активирован!</b>\n\n"
            f"🔗 Ваша VPN-ссылка:\n<code>{vpn_url}</code>\n\n"
            "Приятного использования! 🎉",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
    else:
        await callback.message.answer(
            "❌ Не удалось активировать пробный период. Попробуйте позже или обратитесь в поддержку."
        )


@router.callback_query(F.data == "trial_decline")
async def trial_decline(callback: CallbackQuery, db: Database, panel: PanelAPI):
    user_id = callback.from_user.id
    await db.update_user(user_id, trial_declined=1)
    try:
        await callback.message.delete()
    except Exception as e:
        logger.warning("Не удалось удалить сообщение trial_decline: %s", e)
    active = await is_active_subscription(user_id, db=db, panel=panel)
    await show_available_tariffs(user_id, active, db=db, panel=panel, bot=callback.bot)
    await callback.answer()


async def show_available_tariffs(
    user_id: int,
    has_active_subscription: bool,
    db: Database,
    panel: PanelAPI,
    bot: Optional[Bot] = None,
    user_msg: Optional[Message] = None,
):
    """Показывает текущий тариф (если есть) и список платных тарифов."""
    status = await get_subscription_status(user_id, db=db, panel=panel)
    user_data = status["user"]
    active = bool(has_active_subscription and status["active"])

    if active:
        plan_text = user_data.get("plan_text", "Неизвестно")
        ip_limit = user_data.get("ip_limit", 0)
        traffic_gb = user_data.get("traffic_gb", 0)
        expiry_dt = status.get("expiry_dt")
        expiry_str = expiry_dt.strftime("%d.%m.%Y %H:%M") if expiry_dt else "неизвестно"
        freeze_line = ""
        if status.get("is_frozen") and status.get("frozen_until"):
            freeze_line = f"Статус: ❄️ заморожена до {status['frozen_until'].strftime('%d.%m.%Y %H:%M')}\n"
        text = (
            "📦 <b>Ваша подписка</b>\n\n"
            f"Тариф: <b>{plan_text}</b>\n"
            f"Устройств: до {ip_limit}\n"
            f"Трафик: {format_traffic(traffic_gb)}\n"
            f"{freeze_line}"
            f"Срок действия: до {expiry_str}\n\n"
            "⬇️ <b>Доступные тарифы:</b>\n"
        )
    else:
        text = "📦 <b>Доступные тарифы:</b>\n"

    plans = await get_visible_plans(user_id, for_admin=False, db=db)
    if not plans:
        text += "Тарифы временно недоступны."
    else:
        for idx, plan in enumerate(plans, 1):
            price = plan.get("price_rub", 0)
            duration = int(plan.get("duration_days", 30))
            if duration == 10:
                price_line = f"{price} ₽/мес"
            else:
                price_line = f"{price} ₽ / {duration} дней"
            text += (
                f"{idx}. <b>{plan.get('name')}</b> - {price_line}\n"
                f"   ➤ {plan.get('ip_limit')} устройств, {format_traffic(plan.get('traffic_gb'))}\n"
            )

    await replace_message(user_id, text, reply_markup=subscriptions_keyboard(active), delete_user_msg=user_msg, bot=bot)


@router.callback_query(F.data == "back_to_subscriptions")
async def back_to_subscriptions(callback: CallbackQuery, db: Database, panel: PanelAPI):
    user_id = callback.from_user.id
    active = await is_active_subscription(user_id, db=db, panel=panel)
    await callback.message.delete()
    await show_available_tariffs(user_id, active, db=db, panel=panel, bot=callback.bot)
    await callback.answer()


@router.message(F.text == "⏸ Заморозить подписку")
async def freeze_subscription(message: Message, db: Database, panel: PanelAPI):
    user_id = message.from_user.id
    status = await get_subscription_status(user_id, db=db, panel=panel)

    if not status.get("active"):
        await message.answer("❌ Заморозка доступна только при активной подписке.")
        return

    if status.get("is_frozen") and status.get("frozen_until"):
        until_text = status["frozen_until"].strftime("%d.%m.%Y %H:%M")
        await message.answer(f"❄️ Подписка уже заморожена до <b>{until_text}</b>.", parse_mode="HTML")
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="7 дней", callback_data="freeze:7"),
         InlineKeyboardButton(text="14 дней", callback_data="freeze:14"),
         InlineKeyboardButton(text="30 дней", callback_data="freeze:30")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="freeze:cancel")],
    ])
    await message.answer(
        "⏸ <b>Заморозка подписки</b>\n\n"
        "На сколько дней заморозить?\n"
        "Текущая реализация компенсирует паузу продлением срока подписки и помечает её как замороженную.",
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("freeze:"))
async def freeze_callback(callback: CallbackQuery, db: Database, panel: PanelAPI):
    user_id = callback.from_user.id
    action = callback.data.split(":")[1]

    if action == "cancel":
        await callback.message.delete()
        await callback.answer()
        return

    status = await get_subscription_status(user_id, db=db, panel=panel)
    if not status.get("active"):
        await callback.message.edit_text("❌ Активная подписка не найдена. Заморозка недоступна.")
        await callback.answer()
        return

    if status.get("is_frozen") and status.get("frozen_until"):
        until_text = status["frozen_until"].strftime("%d.%m.%Y %H:%M")
        await callback.message.edit_text(
            f"❄️ Подписка уже заморожена до <b>{until_text}</b>.",
            parse_mode="HTML",
        )
        await callback.answer()
        return

    try:
        days = int(action)
    except (TypeError, ValueError):
        await callback.answer("Некорректный срок", show_alert=True)
        return

    if days not in {7, 14, 30}:
        await callback.answer("Недопустимый срок заморозки", show_alert=True)
        return

    base_email = f"user_{user_id}@{Config.PANEL_EMAIL_DOMAIN}"
    success = await panel.extend_client_expiry(base_email, days)
    if success:
        frozen_until_dt = datetime.utcnow() + timedelta(days=days)
        await db.set_frozen(user_id, frozen_until_dt.strftime("%Y-%m-%d %H:%M:%S"))
        await callback.message.edit_text(
            f"❄️ Подписка помечена как замороженная на <b>{days} дней</b>.\n"
            f"Статус заморозки действует до <b>{frozen_until_dt.strftime('%d.%m.%Y %H:%M')}</b>.\n\n"
            "Срок подписки уже компенсирован продлением в панели.",
            parse_mode="HTML",
        )
    else:
        await callback.message.edit_text("❌ Не удалось заморозить подписку. Попробуйте позже.")
    await callback.answer()


@router.message(F.text == "▶️ Разморозить подписку")
async def unfreeze_subscription(message: Message, db: Database, panel: PanelAPI):
    user_id = message.from_user.id
    status = await get_subscription_status(user_id, db=db, panel=panel)

    if not status.get("frozen_until"):
        await message.answer("ℹ️ Подписка сейчас не заморожена.")
        return

    await db.clear_frozen(user_id)
    if status.get("active"):
        await message.answer(
            "✅ Подписка разморожена.\nДоступ снова считается активным сразу, а компенсированные дни уже сохранены."
        )
    else:
        await message.answer("ℹ️ Статус заморозки очищен, активная подписка не найдена.")
