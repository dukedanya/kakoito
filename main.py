import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.callback_answer import CallbackAnswerMiddleware

from config import Config
from db import Database
from services.panel import PanelAPI
from services.itpay import ItpayAPI
from services.webhook import start_webhook_server, stop_webhook_server
from services.health import HealthAlertState, collect_health_snapshot, emit_health_alerts
from services.migrations import apply_migrations
from tariffs.loader import load_tariffs
from middlewares.ban import ban_middleware
from handlers import start, profile, buy, payment_admin, referral, admin, inline, admin_health
from utils.helpers import set_bot

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - formatting only
        payload = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    level = getattr(logging, Config.LOG_LEVEL, logging.INFO)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if Config.LOG_TO_FILE:
        handlers.append(
            logging.FileHandler(
                os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log"),
                encoding="utf-8",
            )
        )

    formatter: logging.Formatter
    if Config.LOG_JSON:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    for handler in handlers:
        handler.setFormatter(formatter)
        root.addHandler(handler)


configure_logging()
logger = logging.getLogger(__name__)


def _handle_loop_exception(loop: asyncio.AbstractEventLoop, context: dict) -> None:
    message = context.get("message", "Unhandled asyncio loop exception")
    exc = context.get("exception")
    if exc:
        logger.exception(message, exc_info=exc)
    else:
        logger.error(message)


def _log_startup_summary() -> None:
    summary = Config.startup_summary()
    safe_summary = ", ".join(f"{k}={v}" for k, v in summary.items())
    logger.info("Startup config: %s", safe_summary)


def validate_runtime_or_raise() -> None:
    errors = Config.validate_startup()
    if errors:
        for err in errors:
            logger.critical("Startup validation failed: %s", err)
        raise RuntimeError("Invalid configuration. Fix .env before starting the bot.")


def _install_process_exception_hooks() -> None:
    def _sys_hook(exc_type, exc_value, exc_tb):
        logger.critical("Unhandled top-level exception", exc_info=(exc_type, exc_value, exc_tb))

    sys.excepthook = _sys_hook



def _log_task_result(task: asyncio.Task) -> None:
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    except Exception as exc:  # pragma: no cover
        logger.error("task callback failed: %s", exc)
        return

    if exc:
        logger.exception("Background task %s crashed", task.get_name(), exc_info=exc)


def create_background_task(coro, *, name: str) -> asyncio.Task:
    task = asyncio.create_task(coro, name=name)
    task.add_done_callback(_log_task_result)
    return task


async def cancel_background_tasks(tasks: list[asyncio.Task]) -> None:
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


db    = Database(Config.DATA_FILE)
panel = PanelAPI()
itpay = ItpayAPI()
health_alert_state = HealthAlertState()


async def check_expired_subscriptions() -> None:
    from services.subscriptions import is_active_subscription
    while True:
        try:
            users = await db.get_all_subscribers()
            for user in users:
                try:
                    await is_active_subscription(int(user["user_id"]), db=db, panel=panel)
                except Exception as user_error:
                    logger.error(f"check_expired_subscriptions user={user.get('user_id')}: {user_error}")
            await asyncio.sleep(Config.EXPIRED_CHECK_INTERVAL_SEC)
        except Exception as e:
            logger.error(f"check_expired_subscriptions: {e}")
            await asyncio.sleep(60)




async def reconcile_itpay_payments() -> None:
    from services.payment_flow import process_successful_payment, reject_pending_payment

    while True:
        try:
            await asyncio.sleep(Config.PAYMENT_RECONCILE_INTERVAL_SEC)
            pending = await db.get_all_pending_payments(statuses=["pending"])
            for payment in pending:
                itpay_id = payment.get("itpay_id")
                if not itpay_id:
                    continue

                remote_payment = await itpay.get_payment(itpay_id)
                if not remote_payment:
                    continue

                remote_status = ItpayAPI.extract_status(remote_payment)
                if ItpayAPI.is_success_status(remote_payment):
                    result = await process_successful_payment(
                        payment=payment,
                        db=db,
                        panel=panel,
                        bot=None,
                        admin_context=f"ITPAY reconcile status={remote_status}",
                    )
                    if not result.get("ok") and result.get("reason") != "already_processing":
                        logger.warning(
                            "reconcile_itpay_payments: activation failed payment=%s status=%s reason=%s",
                            payment.get("payment_id"),
                            remote_status,
                            result.get("reason"),
                        )
                elif ItpayAPI.is_failed_status(remote_payment):
                    result = await reject_pending_payment(
                        payment=payment,
                        db=db,
                        bot=None,
                        reason_text=(
                            "❌ <b>Платёж не был завершён.</b>\n\n"
                            "Если деньги всё же списались, напишите в поддержку — мы проверим вручную."
                        ),
                        admin_context=f"ITPAY reconcile status={remote_status}",
                    )
                    if not result.get("ok") and result.get("reason") != "already_processing":
                        logger.warning(
                            "reconcile_itpay_payments: reject failed payment=%s status=%s reason=%s",
                            payment.get("payment_id"),
                            remote_status,
                            result.get("reason"),
                        )
        except Exception as e:
            logger.error(f"reconcile_itpay_payments: {e}")
            await asyncio.sleep(Config.PAYMENT_RECONCILE_INTERVAL_SEC)




async def recover_stuck_processing_payments() -> None:
    while True:
        try:
            released = await db.reclaim_stale_processing_payments(timeout_minutes=Config.STALE_PROCESSING_TIMEOUT_MIN)
            if released:
                logger.warning("recover_stuck_processing_payments: released %s stale processing payments", released)
            await asyncio.sleep(Config.STALE_PROCESSING_RECOVERY_INTERVAL_SEC)
        except Exception as e:
            logger.error(f"recover_stuck_processing_payments: {e}")
            await asyncio.sleep(120)



async def health_monitor(bot: Bot) -> None:
    while True:
        try:
            snapshot = await collect_health_snapshot(db, panel, itpay)
            await emit_health_alerts(snapshot=snapshot, state=health_alert_state, bot=bot)
            await asyncio.sleep(Config.HEALTHCHECK_INTERVAL_SEC)
        except Exception as e:
            logger.error(f"health_monitor: {e}")
            await asyncio.sleep(60)

async def cleanup_old_payments() -> None:
    while True:
        try:
            deleted = await db.cleanup_old_pending_payments(days=30)
            if deleted:
                logger.info(f"Удалено старых платежей: {deleted}")
            await asyncio.sleep(259200)
        except Exception as e:
            logger.error(f"cleanup_old_payments: {e}")
            await asyncio.sleep(3600)



async def remind_unpaid_referrals() -> None:
    """Напоминание рефералам, которые не купили подписку через 24ч."""
    from utils.helpers import notify_user
    while True:
        try:
            await asyncio.sleep(3600)  # проверяем каждый час
            users = await db.get_all_users()
            now = datetime.utcnow()
            for user in users:
                if user.get("ref_by") and not user.get("ref_rewarded") and not user.get("has_subscription"):
                    joined = user.get("join_date")
                    if joined:
                        try:
                            join_dt = datetime.fromisoformat(str(joined))
                        except (TypeError, ValueError) as exc:
                            logger.debug("remind_unpaid_referrals: invalid join_date for %s: %s", user.get("user_id"), exc)
                            continue
                        diff = (now - join_dt).total_seconds()
                        # Отправляем один раз — через 24ч после регистрации
                        if 86400 <= diff <= 90000:
                            await notify_user(
                                user["user_id"],
                                "👋 Привет! Вы пришли по реферальной ссылке.\n\n"
                                "Купите подписку и получите бонусные дни! 🎁\n"
                                "Нажмите /start чтобы начать."
                            )
        except Exception as e:
            logger.error(f"remind_unpaid_referrals: {e}")
            await asyncio.sleep(3600)


async def check_expiry_notifications() -> None:
    """Напоминания за 3 дня, 1 день и 1 час до истечения подписки."""
    from utils.helpers import notify_user
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    from services.subscriptions import get_subscription_status

    kb_renew = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Продлить подписку", callback_data="open_buy_menu")],
        [InlineKeyboardButton(text="📦 Моя подписка", callback_data="back_to_subscriptions")],
    ])

    while True:
        try:
            await asyncio.sleep(1800)  # каждые 30 минут
            users = await db.get_all_subscribers()

            for user in users:
                uid = user["user_id"]
                status = await get_subscription_status(uid, db=db, panel=panel)
                if not status.get("active"):
                    continue
                if status.get("is_frozen"):
                    continue

                expiry_dt = status.get("expiry_dt")
                if not expiry_dt:
                    continue

                diff_sec = (expiry_dt.timestamp() - time.time())
                if diff_sec <= 0:
                    continue

                if diff_sec <= 3600 and not user.get("notified_1h"):
                    await notify_user(uid, "⏰ <b>До истечения подписки остался 1 час!</b>\n\nНажмите кнопку ниже, чтобы продлить без потери оставшихся дней.", reply_markup=kb_renew)
                    await db.update_user(uid, notified_1h=1)
                elif diff_sec <= 86400 and not user.get("notified_1d"):
                    await notify_user(uid, "⚠️ <b>До истечения подписки остался 1 день!</b>\n\nПродлите заранее — оставшиеся дни сохранятся.", reply_markup=kb_renew)
                    await db.update_user(uid, notified_1d=1)
                elif diff_sec <= 259200 and not user.get("notified_3d"):
                    await notify_user(uid, "📅 <b>До истечения подписки осталось 3 дня.</b>\n\nВы можете продлить прямо сейчас, не теряя текущий остаток.", reply_markup=kb_renew)
                    await db.update_user(uid, notified_3d=1)

        except Exception as e:
            logger.error(f"check_expiry_notifications: {e}")
            await asyncio.sleep(1800)

async def main() -> None:
    validate_runtime_or_raise()
    _install_process_exception_hooks()
    _log_startup_summary()
    load_tariffs()
    await db.connect()
    if Config.MIGRATIONS_AUTO_APPLY:
        applied = await apply_migrations(db, BASE_DIR)
        if applied:
            logger.info("Applied %s pending migrations", applied)
    await panel.start()

    bot = Bot(
        token=Config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_handle_loop_exception)

    dp = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(ban_middleware)
    dp.callback_query.middleware(ban_middleware)
    dp.callback_query.middleware(CallbackAnswerMiddleware())

    dp.include_router(start.router)
    dp.include_router(profile.router)
    dp.include_router(buy.router)
    dp.include_router(payment_admin.router)
    dp.include_router(referral.router)
    dp.include_router(admin.router)
    dp.include_router(inline.router)
    dp.include_router(admin_health.router)

    # Глобальные зависимости для хендлеров
    dp["db"]    = db
    dp["panel"] = panel
    dp["itpay"] = itpay
    dp["bot"]   = bot

    background_tasks = [
        create_background_task(check_expired_subscriptions(), name="check_expired_subscriptions"),
        create_background_task(cleanup_old_payments(), name="cleanup_old_payments"),
        create_background_task(reconcile_itpay_payments(), name="reconcile_itpay_payments"),
        create_background_task(recover_stuck_processing_payments(), name="recover_stuck_processing_payments"),
        create_background_task(remind_unpaid_referrals(), name="remind_unpaid_referrals"),
        create_background_task(check_expiry_notifications(), name="check_expiry_notifications"),
        create_background_task(health_monitor(bot), name="health_monitor"),
    ]
    webhook_runner = await start_webhook_server(bot, db, panel)

    me = await bot.get_me()
    set_bot(bot, me.username)  # legacy fallback for helper-ы without explicit bot
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Бот запущен: @%s", me.username or "unknown")

    try:
        await dp.start_polling(bot)
    finally:
        await cancel_background_tasks(background_tasks)
        await stop_webhook_server(webhook_runner)
        await db.close()
        await panel.close()
        await itpay.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())

