import logging
from typing import Any, Dict, Optional

from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from db import Database
from services.panel import PanelAPI
from services.subscriptions import (
    create_subscription,
    get_remaining_active_days,
    reward_referrer_days,
    reward_referrer_percent,
)
from tariffs import format_duration, format_traffic, get_by_id
from utils.helpers import notify_admins, notify_user

logger = logging.getLogger(__name__)


def main_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]]
    )


def support_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Поддержка", url=Config.SUPPORT_URL)],
            [InlineKeyboardButton(text="Главное меню", callback_data="main_menu")],
        ]
    )


async def resolve_bonus_days_for_user(user_data: Optional[Dict[str, Any]], db: Database) -> int:
    if not user_data:
        return 0
    ref_by = user_data.get("ref_by")
    ref_rewarded = user_data.get("ref_rewarded")
    if not ref_by or ref_rewarded:
        return 0

    referrer = await db.get_user(ref_by)
    if referrer and referrer.get("ref_system_type") == 1:
        return Config.REF_BONUS_DAYS
    return 0


async def apply_referral_reward(user_id: int, amount: float, user_data: Optional[Dict[str, Any]], db: Database, panel: PanelAPI) -> None:
    if not user_data:
        return

    ref_by = user_data.get("ref_by")
    ref_rewarded = user_data.get("ref_rewarded")
    if not ref_by or ref_rewarded:
        return

    referrer = await db.get_user(ref_by)
    if referrer:
        if referrer.get("ref_system_type") == 1:
            await reward_referrer_days(ref_by, Config.REF_BONUS_DAYS, db=db, panel=panel)
        else:
            await reward_referrer_percent(user_id, amount, db=db)

    await db.mark_ref_rewarded(user_id)


async def process_successful_payment(
    *,
    payment: Dict[str, Any],
    db: Database,
    panel: PanelAPI,
    bot=None,
    admin_context: Optional[str] = None,
) -> Dict[str, Any]:
    payment_id = payment["payment_id"]
    user_id = int(payment["user_id"])
    plan_id = payment["plan_id"]
    amount = float(payment.get("amount", 0) or 0)
    plan = get_by_id(plan_id)

    if not plan:
        logger.error("process_successful_payment: plan %s not found for %s", plan_id, payment_id)
        return {"ok": False, "reason": "plan_not_found", "payment_id": payment_id, "user_id": user_id}

    current_payment = await db.get_pending_payment(payment_id)
    if current_payment:
        current_status = current_payment.get("status")
        if current_status == "accepted":
            logger.info("process_successful_payment: payment %s already accepted", payment_id)
            return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": user_id, "plan": plan}
        if current_status == "rejected":
            logger.warning("process_successful_payment: payment %s already rejected", payment_id)
            return {"ok": False, "reason": "already_rejected", "payment_id": payment_id, "user_id": user_id, "plan": plan}
        if current_status == "processing":
            logger.info("process_successful_payment: payment %s is already being processed", payment_id)
            return {"ok": False, "reason": "already_processing", "payment_id": payment_id, "user_id": user_id, "plan": plan}

    claimed = await db.claim_pending_payment(payment_id)
    if not claimed:
        refreshed = await db.get_pending_payment(payment_id)
        refreshed_status = (refreshed or {}).get("status")
        logger.warning("process_successful_payment: could not claim payment %s current_status=%s", payment_id, refreshed_status)
        if refreshed_status == "accepted":
            return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": user_id, "plan": plan}
        if refreshed_status == "processing":
            return {"ok": False, "reason": "already_processing", "payment_id": payment_id, "user_id": user_id, "plan": plan}
        return {"ok": False, "reason": "claim_failed", "payment_id": payment_id, "user_id": user_id, "plan": plan}

    user_data = await db.get_user(user_id)
    bonus_days_for_user = await resolve_bonus_days_for_user(user_data, db)
    carried_days = await get_remaining_active_days(user_id, panel)
    pending_bonus_days = await db.get_bonus_days_pending(user_id)
    vpn_url = await create_subscription(
        user_id,
        plan,
        db=db,
        panel=panel,
        extra_days=bonus_days_for_user,
        preserve_active_days=True,
    )
    if not vpn_url:
        await db.mark_payment_error(payment_id, "subscription_create_failed")
        await db.release_processing_payment(payment_id, error_text="subscription_create_failed")
        logger.error("process_successful_payment: failed to create subscription user=%s plan=%s", user_id, plan_id)
        return {
            "ok": False,
            "reason": "subscription_create_failed",
            "payment_id": payment_id,
            "user_id": user_id,
            "plan": plan,
        }

    try:
        await apply_referral_reward(user_id, amount, user_data, db, panel)
    except Exception as ref_error:
        logger.error("process_successful_payment: referral reward failed payment=%s user=%s: %s", payment_id, user_id, ref_error)
        await db.mark_payment_error(payment_id, f"referral_reward_failed: {ref_error}")

    status_updated = await db.update_payment_status(payment_id, "accepted", allowed_current_statuses=["processing"])
    if not status_updated:
        logger.warning("process_successful_payment: status for %s was not updated to accepted", payment_id)

    result = {
        "ok": True,
        "payment_id": payment_id,
        "user_id": user_id,
        "plan_id": plan_id,
        "plan": plan,
        "amount": amount,
        "vpn_url": vpn_url,
        "bonus_days_for_user": bonus_days_for_user,
        "carried_days": carried_days,
        "pending_bonus_days": pending_bonus_days,
        "msg_id": payment.get("msg_id"),
    }

    if bot:
        notify_text = (
            "✅ <b>Платёж подтверждён!</b>\n\n"
            f"📦 Тариф: <b>{plan.get('name', plan_id)}</b>\n"
            f"📱 Устройств: <b>до {plan.get('ip_limit', 0)}</b>\n"
            f"📊 Трафик: <b>{format_traffic(plan.get('traffic_gb', 0))}</b>\n"
            f"⏳ Срок: <b>{format_duration(int(plan.get('duration_days', 30)) + bonus_days_for_user)}</b>\n"
            f"🔗 URL: <code>{vpn_url}</code>\n\n"
            "Спасибо за покупку! 🎉"
        )
        try:
            msg_id = payment.get("msg_id")
            if msg_id:
                await bot.edit_message_text(
                    notify_text,
                    chat_id=user_id,
                    message_id=msg_id,
                    parse_mode=ParseMode.HTML,
                    reply_markup=main_menu_inline(),
                )
            else:
                await bot.send_message(
                    user_id,
                    notify_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=main_menu_inline(),
                )
        except Exception as e:
            logger.warning("process_successful_payment: failed to notify user %s: %s", user_id, e)
            try:
                await notify_user(user_id, notify_text, reply_markup=main_menu_inline(), bot=bot)
            except Exception as notify_error:
                logger.warning("process_successful_payment: fallback notify failed %s: %s", user_id, notify_error)

        if bonus_days_for_user > 0:
            try:
                await notify_user(
                    user_id,
                    f"🎁 Вам начислено <b>+{bonus_days_for_user} дней</b> бесплатно по реферальной программе!",
                    bot=bot,
                )
            except Exception as e:
                logger.warning("process_successful_payment: bonus notify failed user=%s: %s", user_id, e)

    context_line = f"\n📍 {admin_context}" if admin_context else ""
    await notify_admins(
        f"✅ <b>Оплата подтверждена</b>\n"
        f"👤 <code>{user_id}</code>\n"
        f"📦 {plan.get('name', plan_id)}\n"
        f"💰 {amount} ₽{context_line}"
    )
    return result


async def reject_pending_payment(
    *,
    payment: Dict[str, Any],
    db: Database,
    bot=None,
    reason_text: Optional[str] = None,
    admin_context: Optional[str] = None,
) -> Dict[str, Any]:
    payment_id = payment["payment_id"]
    user_id = int(payment["user_id"])

    current_payment = await db.get_pending_payment(payment_id)
    if current_payment:
        current_status = current_payment.get("status")
        if current_status == "rejected":
            logger.info("reject_pending_payment: payment %s already rejected", payment_id)
            return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": user_id}
        if current_status == "accepted":
            logger.warning("reject_pending_payment: payment %s already accepted", payment_id)
            return {"ok": False, "reason": "already_accepted", "payment_id": payment_id, "user_id": user_id}
        if current_status == "processing":
            logger.info("reject_pending_payment: payment %s is already being processed", payment_id)
            return {"ok": False, "reason": "already_processing", "payment_id": payment_id, "user_id": user_id}

    claimed = await db.claim_pending_payment(payment_id)
    if not claimed:
        refreshed = await db.get_pending_payment(payment_id)
        refreshed_status = (refreshed or {}).get("status")
        logger.warning("reject_pending_payment: could not claim payment %s current_status=%s", payment_id, refreshed_status)
        if refreshed_status == "rejected":
            return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": user_id}
        if refreshed_status == "processing":
            return {"ok": False, "reason": "already_processing", "payment_id": payment_id, "user_id": user_id}
        return {"ok": False, "reason": "claim_failed", "payment_id": payment_id, "user_id": user_id}

    status_updated = await db.update_payment_status(payment_id, "rejected", allowed_current_statuses=["processing"])
    if not status_updated:
        logger.warning("reject_pending_payment: status for %s was not updated to rejected", payment_id)

    if bot:
        text = reason_text or (
            "❌ <b>Ваш платеж отклонен!</b>\n\n"
            "Пожалуйста, проверьте:\n"
            "1. Правильность суммы платежа\n"
            "2. Наличие комментария к платежу\n"
            "3. Актуальность данных карты\n\n"
            "Если вы уверены, что все сделали правильно, свяжитесь с поддержкой."
        )
        try:
            msg_id = payment.get("msg_id")
            if msg_id:
                await bot.edit_message_text(
                    text,
                    chat_id=user_id,
                    message_id=msg_id,
                    parse_mode=ParseMode.HTML,
                    reply_markup=support_inline(),
                )
            else:
                await bot.send_message(
                    user_id,
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=support_inline(),
                )
        except Exception as e:
            logger.warning("reject_pending_payment: failed to notify user %s: %s", user_id, e)
            try:
                await notify_user(user_id, text, reply_markup=support_inline(), bot=bot)
            except Exception as notify_error:
                logger.warning("reject_pending_payment: fallback notify failed %s: %s", user_id, notify_error)

    context_line = f"\n📍 {admin_context}" if admin_context else ""
    await notify_admins(
        f"❌ <b>Оплата отклонена</b>\n👤 <code>{user_id}</code>\n💳 <code>{payment_id}</code>{context_line}"
    )
    return {"ok": True, "payment_id": payment_id, "user_id": user_id}
