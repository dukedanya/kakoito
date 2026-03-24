import unittest
from unittest.mock import patch


class ConfigValidationTests(unittest.TestCase):
    def test_str_to_bool_truthy_values(self):
        from config import str_to_bool
        self.assertTrue(str_to_bool("1"))
        self.assertTrue(str_to_bool("true"))
        self.assertTrue(str_to_bool("YES"))

    def test_validate_startup_reports_missing_required_values(self):
        from config import Config
        with patch.object(Config, "BOT_TOKEN", ""), \
             patch.object(Config, "ADMIN_USER_IDS", []), \
             patch.object(Config, "PANEL_BASE", ""), \
             patch.object(Config, "PANEL_LOGIN", ""), \
             patch.object(Config, "PANEL_PASSWORD", ""), \
             patch.object(Config, "ITPAY_PUBLIC_ID", ""), \
             patch.object(Config, "ITPAY_API_SECRET", ""):
            errors = Config.validate_startup()
        self.assertGreaterEqual(len(errors), 6)
        self.assertIn("BOT_TOKEN is required", errors)
        self.assertIn("ADMIN_USER_IDS must contain at least one Telegram user id", errors)

    def test_startup_summary_hides_secrets_but_has_runtime_flags(self):
        from config import Config
        summary = Config.startup_summary()
        self.assertIn("environment", summary)
        self.assertIn("verify_ssl", summary)
        self.assertIn("admin_count", summary)
        self.assertNotIn("BOT_TOKEN", summary)
        self.assertNotIn("ITPAY_API_SECRET", summary)


if __name__ == "__main__":
    unittest.main()
