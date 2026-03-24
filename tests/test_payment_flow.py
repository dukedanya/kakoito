import sys
import types

# Lightweight stubs so tests can run without full runtime dependencies installed.
if "aiosqlite" not in sys.modules:
    fake_aiosqlite = types.ModuleType("aiosqlite")
    class _Connection: ...
    fake_aiosqlite.Connection = _Connection
    fake_aiosqlite.Row = dict
    async def _connect(*args, **kwargs):
        return None
    fake_aiosqlite.connect = _connect
    sys.modules["aiosqlite"] = fake_aiosqlite

if "aiogram" not in sys.modules:
    aiogram = types.ModuleType("aiogram")
    enums = types.ModuleType("aiogram.enums")
    types_mod = types.ModuleType("aiogram.types")
    enums.ParseMode = type("ParseMode", (), {"HTML": "HTML"})
    class InlineKeyboardButton:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
    class InlineKeyboardMarkup:
        def __init__(self, inline_keyboard=None):
            self.inline_keyboard = inline_keyboard or []
    types_mod.InlineKeyboardButton = InlineKeyboardButton
    types_mod.InlineKeyboardMarkup = InlineKeyboardMarkup
    exceptions_mod = types.ModuleType("aiogram.exceptions")
    class TelegramBadRequest(Exception):
        pass
    class Bot:
        pass
    class Message:
        pass
    class CallbackQuery:
        pass
    exceptions_mod.TelegramBadRequest = TelegramBadRequest
    types_mod.Message = Message
    types_mod.CallbackQuery = CallbackQuery
    aiogram.Bot = Bot
    aiogram.enums = enums
    aiogram.types = types_mod
    aiogram.exceptions = exceptions_mod
    sys.modules["aiogram"] = aiogram
    sys.modules["aiogram.enums"] = enums
    sys.modules["aiogram.types"] = types_mod
    sys.modules["aiogram.exceptions"] = exceptions_mod

import unittest
from unittest.mock import AsyncMock, patch

from services.payment_flow import process_successful_payment


class FakeDB:
    def __init__(self):
        self.status = "pending"
        self.payment_errors = []
        self.subscription_written = None
        self.ref_rewarded = False

    async def get_pending_payment(self, payment_id):
        return {"payment_id": payment_id, "status": self.status}

    async def claim_pending_payment(self, payment_id):
        if self.status != "pending":
            return False
        self.status = "processing"
        return True

    async def get_user(self, user_id):
        return {"user_id": user_id, "ref_by": None, "ref_rewarded": 0}

    async def get_bonus_days_pending(self, user_id):
        return 0

    async def set_subscription(self, **kwargs):
        self.subscription_written = kwargs
        return True

    async def clear_bonus_days_pending(self, user_id):
        return True

    async def reset_expiry_notifications(self, user_id):
        return True

    async def clear_frozen(self, user_id):
        return True

    async def mark_payment_error(self, payment_id, error_text):
        self.payment_errors.append((payment_id, error_text))
        return True

    async def release_processing_payment(self, payment_id, error_text=None):
        self.status = "pending"
        self.payment_errors.append((payment_id, error_text))
        return True

    async def update_payment_status(self, payment_id, status, allowed_current_statuses=None):
        self.status = status
        return True

    async def mark_ref_rewarded(self, user_id):
        self.ref_rewarded = True
        return True


class FakePanel:
    def __init__(self, *, should_create=True):
        self.should_create = should_create

    async def delete_client(self, base_email):
        return True

    async def create_client(self, **kwargs):
        if not self.should_create:
            return None
        return {"subId": "sub-123"}


class PaymentFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_successful_payment_accepts_and_sets_subscription(self):
        db = FakeDB()
        panel = FakePanel()
        payment = {"payment_id": "p1", "user_id": 101, "plan_id": "plan-basic", "amount": 199.0}
        plan = {"id": "plan-basic", "name": "Basic", "traffic_gb": 50, "ip_limit": 2, "duration_days": 30}

        with patch("services.payment_flow.get_by_id", return_value=plan), \
             patch("services.payment_flow.notify_admins", new=AsyncMock()), \
             patch("services.subscriptions.notify_user", new=AsyncMock()), \
             patch("services.subscriptions.notify_admins", new=AsyncMock()):
            result = await process_successful_payment(payment=payment, db=db, panel=panel, bot=None)

        self.assertTrue(result["ok"])
        self.assertEqual(db.status, "accepted")
        self.assertEqual(db.subscription_written["user_id"], 101)
        self.assertIn("vpn_url", result)

    async def test_process_successful_payment_releases_processing_on_subscription_error(self):
        db = FakeDB()
        panel = FakePanel(should_create=False)
        payment = {"payment_id": "p2", "user_id": 202, "plan_id": "plan-basic", "amount": 199.0}
        plan = {"id": "plan-basic", "name": "Basic", "traffic_gb": 50, "ip_limit": 2, "duration_days": 30}

        with patch("services.payment_flow.get_by_id", return_value=plan), \
             patch("services.payment_flow.notify_admins", new=AsyncMock()), \
             patch("services.subscriptions.notify_user", new=AsyncMock()), \
             patch("services.subscriptions.notify_admins", new=AsyncMock()):
            result = await process_successful_payment(payment=payment, db=db, panel=panel, bot=None)

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "subscription_create_failed")
        self.assertEqual(db.status, "pending")
        self.assertTrue(any(err == "subscription_create_failed" for _, err in db.payment_errors))


if __name__ == "__main__":
    unittest.main()
