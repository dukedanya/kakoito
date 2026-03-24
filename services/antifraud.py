import logging
from typing import Optional

from aiogram import Bot

from config import Config
from db import Database
from utils.helpers import notify_admins

logger = logging.getLogger(__name__)


async def guard_payment_creation(user_id: int, db: Database, bot: Optional[Bot] = None) -> tuple[bool, str]:
    recent_count = await db.count_user_payments_created_since(user_id, Config.PAYMENT_CREATE_COOLDOWN_SEC)
    if Config.PAYMENT_CREATE_COOLDOWN_SEC and recent_count > 0:
        await db.add_antifraud_event(user_id, "payment_cooldown", details=f"recent_count={recent_count}")
        return False, f"Создание платежей слишком частое. Подождите {Config.PAYMENT_CREATE_COOLDOWN_SEC} сек."

    pending_count = await db.count_user_pending_payments(user_id)
    if pending_count >= Config.MAX_PENDING_PAYMENTS_PER_USER:
        await db.add_antifraud_event(user_id, "too_many_pending_payments", details=f"pending_count={pending_count}", severity="high")
        if pending_count >= Config.MAX_PENDING_PAYMENTS_PER_USER + 2:
            await notify_admins(
                f"⚠️ Suspicious activity\n\nuser_id: <code>{user_id}</code>\nслишком много pending payments: {pending_count}",
                bot=bot,
            )
        return False, "Слишком много незавершённых платежей. Завершите или дождитесь обработки текущих."

    return True, ""


async def note_trial_abuse(user_id: int, db: Database, reason: str, bot: Optional[Bot] = None) -> None:
    await db.add_antifraud_event(user_id, "trial_abuse", details=reason, severity="high")
    count = await db.count_antifraud_events(user_id, "trial_abuse", since_hours=24)
    if count >= 2:
        await notify_admins(
            f"⚠️ Suspicious activity\n\nuser_id: <code>{user_id}</code>\nподозрение на abuse пробника\nПричина: {reason}",
            bot=bot,
        )
