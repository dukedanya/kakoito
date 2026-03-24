import logging
import time
from typing import Optional

from aiogram import Router, F
from aiogram.types import (
    CallbackQuery, Message,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.enums import ParseMode

from config import Config
from tariffs import (
    get_by_id, is_trial_plan, format_traffic, format_duration,
    build_buy_text,
)
from keyboards import back_keyboard, subscriptions_keyboard
from utils.helpers import (
    replace_message, get_visible_plans,
)
from services.subscriptions import create_subscription, is_active_subscription
from services.payment_flow import process_successful_payment, reject_pending_payment
from services.antifraud import guard_payment_creation

logger = logging.getLogger(__name__)
router = Router()


async def show_plans_list(
    user_id: int,
    db,
    bot,
    message_id: Optional[int] = None,
    user_msg: Optional[Message] = None,
):
    plans = await get_visible_plans(user_id, for_admin=False, db=db)
    if not plans:
        text = "❌ Нет доступных тарифов."
        if message_id:
            await bot.edit_message_text(text, chat_id=user_id, message_id=message_id)
        else:
            await replace_message(user_id, text, reply_markup=back_keyboard(), delete_user_msg=user_msg, bot=bot)
        return

    text = build_buy_text(plans)
    keyboard = []
    for plan in plans:
        name = plan.get("name", plan.get("id"))
        keyboard.append([InlineKeyboardButton(text=name, callback_data=f"buy:{plan.get('id')}")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_subscriptions")])

    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    if message_id:
        await bot.edit_message_text(text, chat_id=user_id, message_id=message_id, reply_markup=markup)
    else:
        await replace_message(user_id, text, reply_markup=markup, delete_user_msg=user_msg, bot=bot)


@router.message(F.text.in_(["💰 Оформить подписку", "💰 Продлить подписку"]))
async def buy_subscription_menu(message: Message, db):
    await show_plans_list(message.from_user.id, db=db, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "open_buy_menu")
async def open_buy_menu_callback(callback: CallbackQuery, db):
    await show_plans_list(callback.from_user.id, db=db, bot=callback.bot, message_id=callback.message.message_id)
    await callback.answer()


@router.callback_query(F.data.startswith("buy:"))
async def buy_plan(callback: CallbackQuery, db, itpay, panel):
    user_id = callback.from_user.id
    plan_id = callback.data.split(":", 1)[1]
    plan = get_by_id(plan_id)

    if not plan or not plan.get("active", True):
        await callback.answer("❌ Тариф не найден или недоступен", show_alert=True)
        return
    if is_trial_plan(plan):
        await callback.answer("⚠️ Пробный тариф оформляется отдельно.", show_alert=True)
        return

    existing_payment = await db.get_user_pending_payment(user_id, plan_id=plan_id, statuses=["pending", "processing"])
    if existing_payment:
        existing_status = existing_payment.get("status")
        if existing_status == "processing":
            await callback.answer("⏳ Этот платёж уже обрабатывается. Подождите немного.", show_alert=True)
            return

        existing_itpay_id = existing_payment.get("itpay_id")
        pay_url = ""
        if existing_itpay_id:
            remote_payment = await itpay.get_payment(existing_itpay_id)
            qr_urls = (remote_payment or {}).get("payment_qr_urls") or {}
            pay_url = qr_urls.get("desktop") or qr_urls.get("android") or qr_urls.get("ios") or ""

            if remote_payment and itpay.is_success_status(remote_payment):
                result = await process_successful_payment(
                    payment=existing_payment,
                    db=db,
                    panel=panel,
                    bot=callback.bot,
                    admin_context=f"Manual status check by user {user_id}",
                )
                if result.get("ok"):
                    await callback.answer("✅ Оплата уже получена. Подписка активирована.", show_alert=True)
                    return
            elif remote_payment and itpay.is_failed_status(remote_payment):
                await reject_pending_payment(
                    payment=existing_payment,
                    db=db,
                    bot=callback.bot,
                    admin_context=f"User reopened expired payment {user_id}",
                )
                existing_payment = None

        if existing_payment:
            duration_existing = int(plan.get("duration_days", 30))
            price_line_existing = f"{plan.get('price_rub', 0)} руб/мес" if duration_existing == 30 else f"{plan.get('price_rub', 0)} руб/{duration_existing} дней"
            text = (
                "💳 <b>У вас уже есть незавершённый платёж</b>\n\n"
                f"📦 Тариф: <b>{plan.get('name', plan_id)}</b>\n"
                f"💰 Сумма: <b>{price_line_existing}</b>\n"
                f"🧾 ID: <code>{existing_payment['payment_id']}</code>\n\n"
                "Используйте текущую ссылку оплаты или проверьте статус после оплаты."
            )
            inline = []
            if pay_url:
                inline.append([InlineKeyboardButton(text="💳 Открыть оплату", url=pay_url)])
            inline.append([InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_payment:{existing_payment['payment_id']}")])
            inline.append([InlineKeyboardButton(text="⬅️ К тарифам", callback_data="back_to_subscriptions")])
            await callback.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=inline),
                parse_mode="HTML",
            )
            await callback.answer()
            return

    can_create, guard_reason = await guard_payment_creation(user_id, db=db, bot=callback.bot)
    if not can_create:
        await callback.answer(f"⚠️ {guard_reason}", show_alert=True)
        return

    amount = plan.get("price_rub", 0)
    payment_id = f"pay_{user_id}_{int(time.time())}"
    plan_name = plan.get("name", plan_id)

    # --- ИСПРАВЛЕНО: передаём user_id и plan_id ---
    itpay_payment = await itpay.create_payment(
        amount=amount,
        client_payment_id=payment_id,
        user_id=user_id,
        plan_id=plan_id,
        description=f"Подписка: {plan_name}",
        success_url=Config.TG_CHANNEL or None,
    )
    if not itpay_payment:
        await callback.answer("❌ Ошибка создания платежа, попробуйте позже", show_alert=True)
        return

    itpay_id = itpay_payment.get("id", "")
    qr_urls = itpay_payment.get("payment_qr_urls") or {}
    pay_url = (
        qr_urls.get("desktop")
        or qr_urls.get("android")
        or qr_urls.get("ios")
        or ""
    )

    await db.add_pending_payment(
        payment_id=payment_id,
        user_id=user_id,
        plan_id=plan_id,
        amount=amount,
        msg_id=callback.message.message_id,
    )
    await db.set_pending_payment_itpay_id(payment_id, itpay_id)

    duration = int(plan.get("duration_days", 30))
    price_line = f"{amount} руб/мес" if duration == 30 else f"{amount} руб/{duration} дней"

    text = (
        "💳 <b>Оплата подписки</b>\n\n"
        f"📦 Тариф: <b>{plan_name}</b>\n"
        f"💰 Сумма: <b>{price_line}</b>\n\n"
        "Нажмите кнопку ниже для перехода к оплате через СБП.\n"
        "После оплаты подписка активируется <b>автоматически</b>."
    )

    inline = []
    if pay_url:
        inline.append([InlineKeyboardButton(text="💳 Оплатить через СБП", url=pay_url)])
    inline.append([InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_payment:{payment_id}")])
    inline.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_payment")])

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=inline),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "cancel_payment")
async def cancel_payment(callback: CallbackQuery, db):
    await show_plans_list(callback.from_user.id, db=db, bot=callback.bot, message_id=callback.message.message_id)
    await callback.answer()


@router.callback_query(F.data.startswith("test:"))
async def test_plan(callback: CallbackQuery, db, panel):
    user_id = callback.from_user.id
    if user_id not in Config.ADMIN_USER_IDS:
        await callback.answer("⛔ Только для администраторов!", show_alert=True)
        return

    plan_id = callback.data.split(":", 1)[1]
    plan = get_by_id(plan_id)
    if not plan:
        await callback.answer("❌ Тариф не найден", show_alert=True)
        return

    vpn_url = await create_subscription(user_id, plan, db=db, panel=panel)
    if vpn_url:
        text = (
            "✅ <b>Тестовая подписка создана!</b>\n\n"
            f"Тариф: <b>{plan.get('name', plan_id)} (тест)</b>\n"
            f"IP-адреса: <b>до {plan.get('ip_limit', 0)}</b>\n"
            f"Трафик: <b>{format_traffic(plan.get('traffic_gb', 0))}</b>\n"
            f"Срок: <b>{format_duration(int(plan.get('duration_days', 30)))}</b>\n\n"
            f"URL:\n<code>{vpn_url}</code>\n\n"
            "Клиент: <b>Happ</b>\n"
            'iOS/macOS — <a href="https://apps.apple.com/ru/app/happ-proxy-utility-plus/id6746188973">App Store</a>\n'
            'Android — <a href="https://play.google.com/store/apps/details?id=com.happproxy">Google Play</a>\n'
            'Windows — <a href="https://github.com/Happ-proxy/happ-desktop/releases/latest/download/setup-Happ.x64.exe">Скачать</a>'
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]]
        )
        await callback.message.edit_text(text, reply_markup=keyboard)
    else:
        await callback.answer("❌ Ошибка создания тестовой подписки", show_alert=True)
    await callback.answer()


@router.callback_query(F.data.startswith("check_payment:"))
async def check_payment_status(callback: CallbackQuery, db, itpay, panel):
    payment_id = callback.data.split(":", 1)[1]
    payment = await db.get_pending_payment(payment_id)
    if not payment:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return

    owner_id = int(payment.get("user_id", 0) or 0)
    if callback.from_user.id != owner_id and callback.from_user.id not in Config.ADMIN_USER_IDS:
        await callback.answer("⛔ Этот платёж вам не принадлежит", show_alert=True)
        return

        await callback.answer("❌ Платёж не найден", show_alert=True)
        return

    status = payment.get("status")
    if status == "accepted":
        await callback.answer("✅ Платёж уже подтверждён", show_alert=True)
        return
    if status == "rejected":
        await callback.answer("❌ Платёж уже отклонён", show_alert=True)
        return
    if status == "processing":
        await callback.answer("⏳ Платёж сейчас обрабатывается", show_alert=True)
        return

    itpay_id = payment.get("itpay_id")
    if not itpay_id:
        await callback.answer("⚠️ Для этого платежа ещё не получен внешний ID", show_alert=True)
        return

    remote_payment = await itpay.get_payment(itpay_id)
    if not remote_payment:
        await callback.answer("⏳ Платёж ещё не найден в системе оплаты. Попробуйте чуть позже.", show_alert=True)
        return

    remote_status = itpay.extract_status(remote_payment)
    if itpay.is_success_status(remote_payment):
        result = await process_successful_payment(
            payment=payment,
            db=db,
            panel=panel,
            bot=callback.bot,
            admin_context=f"Manual status check by user {callback.from_user.id}",
        )
        if result.get("ok"):
            await callback.answer("✅ Оплата подтверждена, подписка активирована.", show_alert=True)
            return
        await callback.answer(f"⏳ Платёж получен, но активация ещё не завершена: {result.get('reason', 'unknown')}", show_alert=True)
        return

    if itpay.is_failed_status(remote_payment):
        await reject_pending_payment(
            payment=payment,
            db=db,
            bot=callback.bot,
            admin_context=f"Manual failed status check by user {callback.from_user.id}: {remote_status}",
        )
        await callback.answer("❌ Платёж не был завершён.", show_alert=True)
        return

    await callback.answer(f"⏳ Платёж ещё ожидает подтверждения ({remote_status or 'pending'}).", show_alert=True)
