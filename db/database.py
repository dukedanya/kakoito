import asyncio
import logging
import os
import re
import secrets
import string
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiosqlite
from config import Config
logger = logging.getLogger(__name__)


def generate_ref_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
        self.lock = asyncio.Lock()

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self.init_db()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()

    async def init_db(self) -> None:
        if not self.conn:
            return
        async with self.lock:
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL
                )
                """
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                "INSERT OR IGNORE INTO schema_version(version) VALUES (0)"
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    banned BOOLEAN DEFAULT FALSE,
                    ban_reason TEXT DEFAULT '',
                    ref_code TEXT,
                    ref_by INTEGER,
                    ref_rewarded INTEGER DEFAULT 0,
                    bonus_days_pending INTEGER DEFAULT 0,
                    trial_used INTEGER DEFAULT 0,
                    trial_declined INTEGER DEFAULT 0,
                    has_subscription INTEGER DEFAULT 0,
                    plan_text TEXT DEFAULT '',
                    ip_limit INTEGER DEFAULT 0,
                    traffic_gb INTEGER DEFAULT 0,
                    vpn_url TEXT DEFAULT ''
                )
                """
            )
            await self.conn.commit()

            # Проверяем и добавляем колонки, если их нет
            cursor = await self.conn.execute("PRAGMA table_info(users)")
            columns = [row[1] for row in await cursor.fetchall()]
            if "ref_system_type" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN ref_system_type INTEGER DEFAULT 1")
            if "ref_rewarded_count" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN ref_rewarded_count INTEGER DEFAULT 0")
            if "frozen_until" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN frozen_until TIMESTAMP DEFAULT NULL")
            if "notified_3d" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN notified_3d INTEGER DEFAULT 0")
            if "notified_1d" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN notified_1d INTEGER DEFAULT 0")
            if "notified_1h" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN notified_1h INTEGER DEFAULT 0")
            if "balance" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN balance REAL DEFAULT 0")
            await self.conn.commit()

            await self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_ref_code ON users(ref_code)"
            )
            await self.conn.commit()

            # Таблица запросов на вывод
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS withdraw_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
                """
            )
            await self.conn.commit()


            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ref_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    ref_user_id INTEGER NOT NULL,
                    amount REAL DEFAULT 0,
                    bonus_days INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.commit()

            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS antifraud_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    severity TEXT DEFAULT 'warning',
                    details TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.commit()

            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_payments (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    plan_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    msg_id INTEGER,
                    itpay_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    processing_started_at TIMESTAMP,
                    activation_attempts INTEGER DEFAULT 0,
                    last_error TEXT DEFAULT ''
                )
            """)
            await self.conn.commit()

            cursor = await self.conn.execute("PRAGMA table_info(pending_payments)")
            pending_columns = [row[1] for row in await cursor.fetchall()]
            if "processing_started_at" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN processing_started_at TIMESTAMP")
            if "activation_attempts" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN activation_attempts INTEGER DEFAULT 0")
            if "last_error" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN last_error TEXT DEFAULT ''")
            await self.conn.commit()
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_payments_status_created ON pending_payments(status, created_at)")
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_payments_user_status ON pending_payments(user_id, status)")
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_antifraud_events_user_created ON antifraud_events(user_id, created_at)")
            await self.conn.commit()

    async def get_schema_version(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute("SELECT version FROM schema_version LIMIT 1") as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def set_schema_version(self, version: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute("UPDATE schema_version SET version = ?", (version,))
            await self.conn.commit()
        return cursor.rowcount > 0

    async def get_applied_migration_versions(self) -> list[int]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute("SELECT version FROM schema_migrations ORDER BY version") as c:
                rows = await c.fetchall()
        return [int(r[0]) for r in rows]

    async def record_migration(self, version: int, name: str) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self.conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, name) VALUES (?, ?)",
                (version, name),
            )
            await self.conn.execute("UPDATE schema_version SET version = CASE WHEN version < ? THEN ? ELSE version END", (version, version))
            await self.conn.commit()
        return True

    async def executescript(self, script: str) -> None:
        if not self.conn:
            return
        async with self.lock:
            await self.conn.executescript(script)
            await self.conn.commit()

    async def add_antifraud_event(self, user_id: int, event_type: str, details: str = "", severity: str = "warning") -> int:
        if not self.conn:
            return 0
        async with self.lock:
            cursor = await self.conn.execute(
                "INSERT INTO antifraud_events (user_id, event_type, severity, details) VALUES (?, ?, ?, ?)",
                (user_id, event_type, severity, details[:500]),
            )
            await self.conn.commit()
        return int(cursor.lastrowid or 0)

    async def count_antifraud_events(self, user_id: int, event_type: str, since_hours: int = 24) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM antifraud_events WHERE user_id = ? AND event_type = ? AND created_at >= datetime('now', '-' || ? || ' hours')",
                (user_id, event_type, since_hours),
            ) as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def get_recent_antifraud_events(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM antifraud_events ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def add_user(self, user_id: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                await self.conn.execute(
                    "INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,)
                )
                await self.conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
                return False

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_user_by_ref_code(self, ref_code: str) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM users WHERE ref_code = ?", (ref_code,)
            ) as cursor:
                row = await cursor.fetchone()
        return dict(row) if row else None

    async def update_user(self, user_id: int, **kwargs) -> bool:
        if not self.conn or not kwargs:
            return False
        set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        values = list(kwargs.values())
        values.append(user_id)
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    f"UPDATE users SET {set_clause} WHERE user_id = ?", values
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Ошибка обновления пользователя {user_id}: {e}")
                return False

    async def get_total_users(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute("SELECT COUNT(*) FROM users") as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def get_banned_users_count(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE banned = TRUE"
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def get_banned_user_ids(self) -> List[int]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT user_id FROM users WHERE banned = TRUE"
            ) as cursor:
                rows = await cursor.fetchall()
        return [int(row[0]) for row in rows]

    async def get_subscribed_user_ids(self) -> List[int]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT user_id FROM users WHERE has_subscription = 1 AND vpn_url != '' AND vpn_url IS NOT NULL"
            ) as cursor:
                rows = await cursor.fetchall()
        return [int(row[0]) for row in rows]

    async def ban_user(self, user_id: int, reason: str = "") -> bool:
        return await self.update_user(user_id, banned=True, ban_reason=reason)

    async def unban_user(self, user_id: int) -> bool:
        return await self.update_user(user_id, banned=False, ban_reason="")

    async def set_subscription(
        self, user_id: int, plan_text: str, ip_limit: int, vpn_url: str, traffic_gb: int
    ) -> bool:
        return await self.update_user(
            user_id=user_id,
            has_subscription=1,
            plan_text=plan_text,
            ip_limit=ip_limit,
            vpn_url=vpn_url,
            traffic_gb=traffic_gb,
            notified_3d=0,
            notified_1d=0,
            notified_1h=0,
        )

    async def remove_subscription(self, user_id: int) -> bool:
        return await self.update_user(
            user_id=user_id,
            has_subscription=0,
            plan_text="",
            ip_limit=0,
            vpn_url="",
            traffic_gb=0,
            frozen_until=None,
            notified_3d=0,
            notified_1d=0,
            notified_1h=0,
        )

    async def set_ref_by(self, user_id: int, ref_by: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                """
                UPDATE users
                SET ref_by = ?
                WHERE user_id = ? AND user_id != ? AND (ref_by IS NULL OR ref_by = 0)
                """,
                (ref_by, user_id, user_id),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

    async def mark_ref_rewarded(self, user_id: int) -> bool:
        return await self.update_user(user_id, ref_rewarded=1)

    async def count_referrals(self, ref_by: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE ref_by = ?", (ref_by,)
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def count_referrals_paid(self, ref_by: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE ref_by = ? AND ref_rewarded = 1",
                (ref_by,),
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def get_bonus_days_pending(self, user_id: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT bonus_days_pending FROM users WHERE user_id = ?", (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    async def clear_bonus_days_pending(self, user_id: int) -> bool:
        return await self.update_user(user_id, bonus_days_pending=0)

    async def add_bonus_days_pending(self, user_id: int, days: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self.conn.execute(
                """
                UPDATE users
                SET bonus_days_pending = COALESCE(bonus_days_pending, 0) + ?
                WHERE user_id = ?
                """,
                (days, user_id),
            )
            await self.conn.commit()
        return True

    async def mark_trial_used(self, user_id: int) -> bool:
        return await self.update_user(user_id, trial_used=1)

    async def mark_trial_declined(self, user_id: int) -> bool:
        return await self.update_user(user_id, trial_declined=1)

    async def set_has_subscription(self, user_id: int) -> bool:
        return await self.update_user(user_id, has_subscription=1)

    async def clear_has_subscription(self, user_id: int) -> bool:
        return await self.update_user(user_id, has_subscription=0)

    async def add_ref_history(self, user_id: int, ref_user_id: int, amount: float = 0, bonus_days: int = 0) -> None:
        """Записывает начисление в историю."""
        if not self.conn:
            return
        async with self.lock:
            await self.conn.execute(
                "INSERT INTO ref_history (user_id, ref_user_id, amount, bonus_days) VALUES (?, ?, ?, ?)",
                (user_id, ref_user_id, amount, bonus_days),
            )
            await self.conn.commit()

    async def get_ref_history(self, user_id: int, limit: int = 10) -> list:
        """История начислений пользователя."""
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM ref_history WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_referrals_list(self, user_id: int) -> list:
        """Список рефералов с флагом оплаты."""
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT user_id, ref_rewarded, join_date FROM users WHERE ref_by = ? ORDER BY join_date DESC",
                (user_id,),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


    async def get_all_users(self) -> list:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute("SELECT * FROM users")
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_all_subscribers(self) -> list:
        """Все пользователи с активной подпиской."""
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM users WHERE has_subscription = 1 AND vpn_url != '' AND vpn_url IS NOT NULL AND banned = 0"
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def set_frozen(self, user_id: int, frozen_until: str) -> bool:
        return await self.update_user(user_id, frozen_until=frozen_until)

    async def clear_frozen(self, user_id: int) -> bool:
        return await self.update_user(user_id, frozen_until=None)

    async def reset_expiry_notifications(self, user_id: int) -> bool:
        return await self.update_user(user_id, notified_3d=0, notified_1d=0, notified_1h=0)

    async def get_top_referrers(self, limit: int = 10) -> list:
        """Топ рефереров по количеству оплативших рефералов."""
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT ref_by, COUNT(*) as paid_count
                FROM users
                WHERE ref_by IS NOT NULL AND ref_rewarded = 1
                GROUP BY ref_by
                ORDER BY paid_count DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def increment_ref_rewarded_count(self, user_id: int) -> None:
        """Увеличивает счётчик успешных рефералов."""
        if not self.conn:
            return
        async with self.lock:
            await self.conn.execute(
                "UPDATE users SET ref_rewarded_count = COALESCE(ref_rewarded_count, 0) + 1 WHERE user_id = ?",
                (user_id,),
            )
            await self.conn.commit()

    async def ensure_ref_code(self, user_id: int) -> Optional[str]:
        user = await self.get_user(user_id)
        if not user:
            await self.add_user(user_id)
            user = await self.get_user(user_id)

        if not user:
            return None

        if user.get("ref_code"):
            return user.get("ref_code")

        for _ in range(20):
            code = generate_ref_code()
            existing = await self.get_user_by_ref_code(code)
            if existing:
                continue
            updated = await self.update_user(user_id, ref_code=code)
            if updated:
                return code

        return None

    # --- Работа с балансом ---
    async def get_balance(self, user_id: int) -> float:
        user = await self.get_user(user_id)
        return user.get("balance", 0.0) if user else 0.0

    async def add_balance(self, user_id: int, amount: float) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                "UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE user_id = ?",
                (amount, user_id),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

    async def subtract_balance(self, user_id: int, amount: float) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                "UPDATE users SET balance = COALESCE(balance, 0) - ? WHERE user_id = ? AND balance >= ?",
                (amount, user_id, amount),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

    # --- Работа с запросами на вывод ---
    async def create_withdraw_request(self, user_id: int, amount: float) -> int:
        if not self.conn or amount <= 0:
            return 0
        async with self.lock:
            existing = await self.conn.execute(
                "SELECT id FROM withdraw_requests WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1",
                (user_id,),
            )
            existing_row = await existing.fetchone()
            if existing_row:
                return int(existing_row[0])

            cursor = await self.conn.execute(
                "INSERT INTO withdraw_requests (user_id, amount) VALUES (?, ?)",
                (user_id, amount),
            )
            await self.conn.commit()
            return cursor.lastrowid

    async def get_pending_withdraw_requests(self) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM withdraw_requests WHERE status = 'pending' ORDER BY created_at ASC"
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_withdraw_request(self, request_id: int) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM withdraw_requests WHERE id = ?",
                (request_id,),
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_user_pending_withdraw_request(self, user_id: int) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM withdraw_requests WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1",
                (user_id,),
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_withdraw_requests_by_user(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM withdraw_requests WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def process_withdraw_request(self, request_id: int, accept: bool) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT user_id, amount FROM withdraw_requests WHERE id = ? AND status = 'pending'",
                (request_id,),
            )
            row = await cursor.fetchone()
            if not row:
                return False

            if accept:
                user_id = row["user_id"]
                amount = row["amount"]
                debit_cursor = await self.conn.execute(
                    "UPDATE users SET balance = COALESCE(balance, 0) - ? WHERE user_id = ? AND balance >= ?",
                    (amount, user_id, amount),
                )
                if debit_cursor.rowcount <= 0:
                    logger.warning(
                        "process_withdraw_request: insufficient balance for user=%s request=%s",
                        user_id,
                        request_id,
                    )
                    await self.conn.commit()
                    return False

                status_cursor = await self.conn.execute(
                    "UPDATE withdraw_requests SET status = 'completed', processed_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'pending'",
                    (request_id,),
                )
            else:
                status_cursor = await self.conn.execute(
                    "UPDATE withdraw_requests SET status = 'rejected', processed_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'pending'",
                    (request_id,),
                )
            await self.conn.commit()
            return status_cursor.rowcount > 0


    async def add_pending_payment(self, payment_id, user_id, plan_id, amount, msg_id=None) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    "INSERT OR IGNORE INTO pending_payments (payment_id, user_id, plan_id, amount, msg_id) VALUES (?, ?, ?, ?, ?)",
                    (payment_id, user_id, plan_id, amount, msg_id),
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"add_pending_payment: {e}")
                return False

    async def get_pending_payment(self, payment_id) -> Optional[Dict[str, Any]]:
        if not self.conn: return None
        async with self.lock:
            async with self.conn.execute("SELECT * FROM pending_payments WHERE payment_id = ?", (payment_id,)) as c:
                row = await c.fetchone()
        return dict(row) if row else None

    async def get_pending_payment_by_itpay_id(self, itpay_id) -> Optional[Dict[str, Any]]:
        if not self.conn: return None
        async with self.lock:
            async with self.conn.execute("SELECT * FROM pending_payments WHERE itpay_id = ?", (itpay_id,)) as c:
                row = await c.fetchone()
        return dict(row) if row else None

    async def set_pending_payment_itpay_id(self, payment_id, itpay_id) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    "UPDATE pending_payments SET itpay_id = ? WHERE payment_id = ?",
                    (itpay_id, payment_id),
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"set_pending_payment_itpay_id: {e}")
                return False

    async def claim_pending_payment(self, payment_id: str) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    """
                    UPDATE pending_payments
                    SET status = 'processing',
                        processed_at = NULL,
                        processing_started_at = CURRENT_TIMESTAMP,
                        activation_attempts = COALESCE(activation_attempts, 0) + 1
                    WHERE payment_id = ? AND status = 'pending'
                    """,
                    (payment_id,),
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"claim_pending_payment: {e}")
                return False

    async def release_processing_payment(self, payment_id: str, error_text: Optional[str] = None) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    """
                    UPDATE pending_payments
                    SET status = 'pending',
                        processed_at = NULL,
                        processing_started_at = NULL,
                        last_error = COALESCE(?, last_error)
                    WHERE payment_id = ? AND status = 'processing'
                    """,
                    (error_text, payment_id),
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"release_processing_payment: {e}")
                return False

    async def mark_payment_error(self, payment_id: str, error_text: str) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    "UPDATE pending_payments SET last_error = ? WHERE payment_id = ?",
                    (error_text[:500], payment_id),
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"mark_payment_error: {e}")
                return False

    async def reclaim_stale_processing_payments(self, timeout_minutes: int = 15) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    """
                    UPDATE pending_payments
                    SET status = 'pending',
                        processing_started_at = NULL,
                        last_error = CASE
                            WHEN COALESCE(last_error, '') = '' THEN 'auto-released stale processing lock'
                            ELSE last_error
                        END
                    WHERE status = 'processing'
                      AND processing_started_at IS NOT NULL
                      AND processing_started_at < datetime('now', '-' || ? || ' minutes')
                    """,
                    (timeout_minutes,),
                )
                await self.conn.commit()
                return cursor.rowcount
            except Exception as e:
                logger.error(f"reclaim_stale_processing_payments: {e}")
                return 0

    async def get_all_pending_payments(self, statuses: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        effective_statuses = statuses or ["pending"]
        placeholders = ",".join(["?"] * len(effective_statuses))
        query = f"SELECT * FROM pending_payments WHERE status IN ({placeholders}) ORDER BY created_at ASC"
        async with self.lock:
            async with self.conn.execute(query, tuple(effective_statuses)) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def get_pending_payments_by_user(self, user_id: int) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM pending_payments WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            ) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def get_user_pending_payment(self, user_id: int, *, plan_id: Optional[str] = None, statuses: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        effective_statuses = statuses or ["pending", "processing"]
        placeholders = ",".join(["?"] * len(effective_statuses))
        params: List[Any] = [user_id, *effective_statuses]
        query = f"SELECT * FROM pending_payments WHERE user_id = ? AND status IN ({placeholders})"
        if plan_id is not None:
            query += " AND plan_id = ?"
            params.append(plan_id)
        query += " ORDER BY created_at DESC LIMIT 1"
        async with self.lock:
            async with self.conn.execute(query, tuple(params)) as c:
                row = await c.fetchone()
        return dict(row) if row else None

    async def update_payment_status(self, payment_id, status, allowed_current_statuses=None) -> bool:
        if not self.conn:
            return False
        current_statuses = allowed_current_statuses or ["pending"]
        placeholders = ",".join(["?"] * len(current_statuses))
        query = (
            f"UPDATE pending_payments SET status = ?, processed_at = CURRENT_TIMESTAMP, "
            f"processing_started_at = NULL, last_error = '' "
            f"WHERE payment_id = ? AND status IN ({placeholders})"
        )
        params = [status, payment_id, *current_statuses]
        async with self.lock:
            try:
                cursor = await self.conn.execute(query, tuple(params))
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"update_payment_status: {e}")
                return False

    async def get_processing_payments_count(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute("SELECT COUNT(*) FROM pending_payments WHERE status = 'processing'") as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def get_old_pending_payments(self, minutes: int = 10) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM pending_payments WHERE status = 'pending' AND created_at < datetime('now', '-' || ? || ' minutes') ORDER BY created_at ASC",
                (minutes,),
            ) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def get_recent_payment_errors(self, hours: int = 24) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM pending_payments WHERE COALESCE(last_error, '') != '' AND created_at >= datetime('now', '-' || ? || ' hours') ORDER BY created_at DESC",
                (hours,),
            ) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def count_user_payments_created_since(self, user_id: int, seconds: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM pending_payments WHERE user_id = ? AND created_at >= datetime('now', '-' || ? || ' seconds')",
                (user_id, seconds),
            ) as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def count_user_pending_payments(self, user_id: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM pending_payments WHERE user_id = ? AND status IN ('pending', 'processing')",
                (user_id,),
            ) as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def cleanup_old_pending_payments(self, days=30) -> int:
        if not self.conn: return 0
        async with self.lock:
            cursor = await self.conn.execute("DELETE FROM pending_payments WHERE status IN ('accepted','rejected') AND processed_at < datetime('now', '-' || ? || ' days')", (days,))
            await self.conn.commit()
        return cursor.rowcount
