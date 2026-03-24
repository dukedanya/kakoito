import logging
import re
from pathlib import Path

from db import Database

logger = logging.getLogger(__name__)

MIGRATION_RE = re.compile(r"^(\d+)_.*\.sql$")


async def apply_migrations(db: Database, base_dir: str) -> int:
    migrations_dir = Path(base_dir) / "migrations"
    if not migrations_dir.exists():
        return 0

    applied = set(await db.get_applied_migration_versions())
    applied_now = 0
    for path in sorted(migrations_dir.glob("*.sql")):
        match = MIGRATION_RE.match(path.name)
        if not match:
            continue
        version = int(match.group(1))
        if version in applied:
            continue
        sql = path.read_text(encoding="utf-8")
        await db.executescript(sql)
        await db.record_migration(version, path.name)
        logger.info("Applied migration %s", path.name)
        applied_now += 1
    return applied_now
