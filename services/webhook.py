import json
import logging

from aiohttp import web

from config import Config
from services.itpay import ItpayAPI
from services.payment_flow import process_successful_payment
from utils.helpers import notify_admins

logger = logging.getLogger(__name__)


async def itpay_webhook_handler(request: web.Request) -> web.Response:
    raw_body = await request.read()
    signature = request.headers.get("itpay-signature", "")

    if Config.ITPAY_WEBHOOK_SECRET:
        if not signature:
            logger.warning("ITPAY webhook: missing signature header")
            return web.Response(status=403, text="missing signature")
        if not ItpayAPI.verify_webhook_signature(Config.ITPAY_WEBHOOK_SECRET, raw_body, signature):
            logger.warning("ITPAY webhook: неверная подпись")
            return web.Response(status=403, text="invalid signature")

    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        logger.warning("ITPAY webhook: bad json: %s", exc)
        return web.Response(status=400, text="bad json")

    event_type = body.get("type", "")
    data = body.get("data") or {}
    itpay_id = data.get("id", "")
    logger.info("ITPAY webhook: %s, id=%s", event_type, itpay_id)

    if event_type not in ("payment.pay", "payment.completed"):
        return web.json_response({"status": 0})

    bot = request.app["bot"]
    db = request.app["db"]
    panel = request.app["panel"]

    payment = await db.get_pending_payment_by_itpay_id(itpay_id)
    if not payment:
        client_payment_id = data.get("client_payment_id", "")
        if client_payment_id:
            payment = await db.get_pending_payment(client_payment_id)

    if not payment:
        metadata = data.get("metadata") or {}
        user_id_meta = metadata.get("user_id")
        plan_id_meta = metadata.get("plan_id")
        client_payment_id = data.get("client_payment_id", "")
        if user_id_meta and plan_id_meta and client_payment_id:
            logger.warning("ITPAY webhook: платёж %s не найден в БД, восстанавливаем из metadata", itpay_id)
            payment = {
                "payment_id": client_payment_id,
                "user_id": int(user_id_meta),
                "plan_id": plan_id_meta,
                "amount": float(data.get("amount", 0) or 0),
                "status": "pending",
                "msg_id": None,
            }
        else:
            logger.error("ITPAY webhook: платёж %s не найден нигде", itpay_id)
            return web.json_response({"status": 0})

    if payment.get("status") != "pending":
        logger.info(
            "ITPAY webhook: платёж %s уже обработан со статусом %s",
            payment.get("payment_id"),
            payment.get("status"),
        )
        return web.json_response({"status": 0})

    result = await process_successful_payment(
        payment=payment,
        db=db,
        panel=panel,
        bot=bot,
        admin_context="ITPAY webhook",
    )

    if result.get("ok"):
        return web.json_response({"status": 0})

    if result.get("reason") == "already_processing":
        logger.info("ITPAY webhook: payment %s is already processing", payment.get("payment_id"))
        return web.Response(status=202, text="already processing")

    payment_id = payment["payment_id"]
    user_id = payment["user_id"]
    plan_id = payment["plan_id"]
    reason = result.get("reason", "unknown_error")

    try:
        await bot.send_message(
            user_id,
            "⚠️ Платёж получен, но активация ещё не завершена. Мы уже разбираемся, ничего повторно оплачивать не нужно.",
            parse_mode="HTML",
        )
    except Exception as exc:
        logger.warning("ITPAY webhook: не удалось уведомить пользователя %s: %s", user_id, exc)

    await notify_admins(
        f"⚠️ <b>Проблема активации после webhook</b>\n"
        f"💳 Платёж: <code>{payment_id}</code>\n"
        f"👤 Пользователь: <code>{user_id}</code>\n"
        f"📦 План: <b>{plan_id}</b>\n"
        f"🧩 Причина: <code>{reason}</code>",
        bot=bot,
    )
    logger.error("ITPAY webhook: activation failed payment=%s user=%s plan=%s reason=%s", payment_id, user_id, plan_id, reason)
    return web.Response(status=500, text="activation failed")


async def start_webhook_server(bot, db, panel) -> web.AppRunner:
    app = web.Application()
    app["bot"] = bot
    app["db"] = db
    app["panel"] = panel
    app.router.add_post("/itpay/webhook", itpay_webhook_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    logger.info("ITPAY webhook: 0.0.0.0:8080/itpay/webhook")
    return runner


async def stop_webhook_server(runner: web.AppRunner | None) -> None:
    if runner is None:
        return
    await runner.cleanup()
