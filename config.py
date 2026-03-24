import logging
import os
from typing import List
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))


def str_to_bool(val: str) -> bool:
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_USER_IDS: List[int] = [
        int(x.strip()) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()
    ]
    PAYMENT_CARD_NUMBER: str = os.getenv("PAYMENT_CARD_NUMBER", "")
    PANEL_BASE: str = os.getenv("PANEL_BASE", "").rstrip("/")
    SUB_PANEL_BASE: str = os.getenv("SUB_PANEL_BASE", "")
    PANEL_LOGIN: str = os.getenv("PANEL_LOGIN", "")
    PANEL_PASSWORD: str = os.getenv("PANEL_PASSWORD", "")
    VERIFY_SSL: bool = str_to_bool(os.getenv("VERIFY_SSL", "true"))
    DATA_DIR: str = os.getenv("DATA_DIR", "/data")
    DATA_FILE: str = os.getenv("DATA_FILE", os.path.join(os.getenv("DATA_DIR", "/data"), "users.db"))
    SITE_URL: str = os.getenv("SITE_URL", "")
    TG_CHANNEL: str = os.getenv("TG_CHANNEL", "https://t.me/+XsoxseRgJa8yN2Ni")
    SUPPORT_URL: str = os.getenv("SUPPORT_URL", "")
    REF_BONUS_DAYS: int = int(os.getenv("REF_BONUS_DAYS", "7"))
    REF_PERCENT_LEVEL1: float = float(os.getenv("REF_PERCENT_LEVEL1", "25"))
    REF_PERCENT_LEVEL2: float = float(os.getenv("REF_PERCENT_LEVEL2", "10"))
    REF_PERCENT_LEVEL3: float = float(os.getenv("REF_PERCENT_LEVEL3", "5"))
    MIN_WITHDRAW: float = float(os.getenv("MIN_WITHDRAW", "300"))
    PANEL_EMAIL_DOMAIN: str = os.getenv("PANEL_EMAIL_DOMAIN", "vpnbot")
    ITPAY_PUBLIC_ID: str = os.getenv("ITPAY_PUBLIC_ID", "")
    ITPAY_API_SECRET: str = os.getenv("ITPAY_API_SECRET", "")
    ITPAY_WEBHOOK_SECRET: str = os.getenv("ITPAY_WEBHOOK_SECRET", "")
    WEBHOOK_HOST: str = os.getenv("WEBHOOK_HOST", "")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_JSON: bool = str_to_bool(os.getenv("LOG_JSON", "false"))
    LOG_TO_FILE: bool = str_to_bool(os.getenv("LOG_TO_FILE", "true"))
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")
    PAYMENT_RECONCILE_INTERVAL_SEC: int = int(os.getenv("PAYMENT_RECONCILE_INTERVAL_SEC", "120"))
    EXPIRED_CHECK_INTERVAL_SEC: int = int(os.getenv("EXPIRED_CHECK_INTERVAL_SEC", "300"))
    STALE_PROCESSING_TIMEOUT_MIN: int = int(os.getenv("STALE_PROCESSING_TIMEOUT_MIN", "15"))
    STALE_PROCESSING_RECOVERY_INTERVAL_SEC: int = int(os.getenv("STALE_PROCESSING_RECOVERY_INTERVAL_SEC", "300"))
    HEALTHCHECK_INTERVAL_SEC: int = int(os.getenv("HEALTHCHECK_INTERVAL_SEC", "120"))
    HEALTH_ALERT_COOLDOWN_SEC: int = int(os.getenv("HEALTH_ALERT_COOLDOWN_SEC", "900"))
    HEALTH_PENDING_AGE_MIN: int = int(os.getenv("HEALTH_PENDING_AGE_MIN", "10"))
    HEALTH_MAX_PROCESSING: int = int(os.getenv("HEALTH_MAX_PROCESSING", "3"))
    PAYMENT_CREATE_COOLDOWN_SEC: int = int(os.getenv("PAYMENT_CREATE_COOLDOWN_SEC", "10"))
    MAX_PENDING_PAYMENTS_PER_USER: int = int(os.getenv("MAX_PENDING_PAYMENTS_PER_USER", "3"))
    MAX_WITHDRAW_REQUESTS_PER_DAY: int = int(os.getenv("MAX_WITHDRAW_REQUESTS_PER_DAY", "3"))
    MAX_DAILY_REF_BONUS_RUB: float = float(os.getenv("MAX_DAILY_REF_BONUS_RUB", "5000"))
    MIGRATIONS_AUTO_APPLY: bool = str_to_bool(os.getenv("MIGRATIONS_AUTO_APPLY", "true"))

    @classmethod
    def validate_startup(cls) -> list[str]:
        errors: list[str] = []
        if not cls.BOT_TOKEN:
            errors.append("BOT_TOKEN is required")
        if not cls.ADMIN_USER_IDS:
            errors.append("ADMIN_USER_IDS must contain at least one Telegram user id")
        if not cls.PANEL_BASE:
            errors.append("PANEL_BASE is required")
        if not cls.PANEL_LOGIN:
            errors.append("PANEL_LOGIN is required")
        if not cls.PANEL_PASSWORD:
            errors.append("PANEL_PASSWORD is required")
        if not cls.ITPAY_PUBLIC_ID:
            errors.append("ITPAY_PUBLIC_ID is required")
        if not cls.ITPAY_API_SECRET:
            errors.append("ITPAY_API_SECRET is required")
        if cls.PAYMENT_RECONCILE_INTERVAL_SEC < 30:
            errors.append("PAYMENT_RECONCILE_INTERVAL_SEC must be >= 30")
        if cls.EXPIRED_CHECK_INTERVAL_SEC < 60:
            errors.append("EXPIRED_CHECK_INTERVAL_SEC must be >= 60")
        if cls.STALE_PROCESSING_TIMEOUT_MIN < 1:
            errors.append("STALE_PROCESSING_TIMEOUT_MIN must be >= 1")
        if cls.STALE_PROCESSING_RECOVERY_INTERVAL_SEC < 60:
            errors.append("STALE_PROCESSING_RECOVERY_INTERVAL_SEC must be >= 60")
        if cls.HEALTHCHECK_INTERVAL_SEC < 30:
            errors.append("HEALTHCHECK_INTERVAL_SEC must be >= 30")
        if cls.HEALTH_ALERT_COOLDOWN_SEC < 60:
            errors.append("HEALTH_ALERT_COOLDOWN_SEC must be >= 60")
        if cls.HEALTH_PENDING_AGE_MIN < 1:
            errors.append("HEALTH_PENDING_AGE_MIN must be >= 1")
        if cls.HEALTH_MAX_PROCESSING < 1:
            errors.append("HEALTH_MAX_PROCESSING must be >= 1")
        if cls.PAYMENT_CREATE_COOLDOWN_SEC < 0:
            errors.append("PAYMENT_CREATE_COOLDOWN_SEC must be >= 0")
        if cls.MAX_PENDING_PAYMENTS_PER_USER < 1:
            errors.append("MAX_PENDING_PAYMENTS_PER_USER must be >= 1")
        if cls.MAX_WITHDRAW_REQUESTS_PER_DAY < 1:
            errors.append("MAX_WITHDRAW_REQUESTS_PER_DAY must be >= 1")
        if cls.MAX_DAILY_REF_BONUS_RUB < 0:
            errors.append("MAX_DAILY_REF_BONUS_RUB must be >= 0")
        return errors

    @classmethod
    def startup_summary(cls) -> dict:
        return {
            "environment": cls.ENVIRONMENT,
            "verify_ssl": cls.VERIFY_SSL,
            "log_level": cls.LOG_LEVEL,
            "log_json": cls.LOG_JSON,
            "log_to_file": cls.LOG_TO_FILE,
            "data_file": cls.DATA_FILE,
            "admin_count": len(cls.ADMIN_USER_IDS),
            "payment_reconcile_interval_sec": cls.PAYMENT_RECONCILE_INTERVAL_SEC,
            "expired_check_interval_sec": cls.EXPIRED_CHECK_INTERVAL_SEC,
            "stale_processing_timeout_min": cls.STALE_PROCESSING_TIMEOUT_MIN,
            "stale_processing_recovery_interval_sec": cls.STALE_PROCESSING_RECOVERY_INTERVAL_SEC,
            "healthcheck_interval_sec": cls.HEALTHCHECK_INTERVAL_SEC,
            "health_alert_cooldown_sec": cls.HEALTH_ALERT_COOLDOWN_SEC,
            "health_pending_age_min": cls.HEALTH_PENDING_AGE_MIN,
            "health_max_processing": cls.HEALTH_MAX_PROCESSING,
            "payment_create_cooldown_sec": cls.PAYMENT_CREATE_COOLDOWN_SEC,
            "max_pending_payments_per_user": cls.MAX_PENDING_PAYMENTS_PER_USER,
            "max_withdraw_requests_per_day": cls.MAX_WITHDRAW_REQUESTS_PER_DAY,
            "max_daily_ref_bonus_rub": cls.MAX_DAILY_REF_BONUS_RUB,
            "migrations_auto_apply": cls.MIGRATIONS_AUTO_APPLY,
            "webhook_enabled": bool(cls.ITPAY_WEBHOOK_SECRET or cls.WEBHOOK_HOST),
        }


try:
    os.makedirs(Config.DATA_DIR, exist_ok=True)
except OSError as exc:
    logger.warning("Не удалось создать DATA_DIR %s: %s", Config.DATA_DIR, exc)
