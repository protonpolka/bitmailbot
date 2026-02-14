import asyncpg
import os


class Database:
    def __init__(self):
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=os.getenv("DATABASE_URL"),
            min_size=2,
            max_size=10
        )
        await self.init()

    async def init(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT DEFAULT '',
                    full_name TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mails (
                    id SERIAL PRIMARY KEY,
                    mail TEXT UNIQUE NOT NULL,
                    is_used BOOLEAN DEFAULT FALSE,
                    used_by BIGINT REFERENCES users(user_id),
                    used_at TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

            await conn.execute("""
                INSERT INTO settings (key, value) VALUES ('daily_limit', '3')
                ON CONFLICT (key) DO NOTHING
            """)

            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mails_is_used ON mails(is_used)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mails_used_by ON mails(used_by)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mails_used_at ON mails(used_at)")

    # ---- Users ----

    async def add_user(self, user_id: int, username: str, full_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (user_id, username, full_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE SET username=$2, full_name=$3
            """, user_id, username, full_name)

    async def get_user_info(self, user_id: int):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT user_id, username, full_name FROM users WHERE user_id=$1", user_id
            )

    async def count_users(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM users")

    async def get_active_users(self):
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT u.user_id, u.username, u.full_name, COUNT(m.id) as cnt
                FROM users u
                JOIN mails m ON m.used_by = u.user_id
                WHERE m.is_used = TRUE
                GROUP BY u.user_id, u.username, u.full_name
                ORDER BY cnt DESC
            """)

    # ---- Mails ----

    async def add_mails_bulk(self, mails: list[str]) -> tuple[int, int]:
        added = 0
        duplicates = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for mail in mails:
                    try:
                        await conn.execute("INSERT INTO mails (mail) VALUES ($1)", mail)
                        added += 1
                    except asyncpg.UniqueViolationError:
                        duplicates += 1
        return added, duplicates

    async def take_mail(self, user_id: int) -> str | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                UPDATE mails SET is_used = TRUE, used_by = $1, used_at = NOW()
                WHERE id = (
                    SELECT id FROM mails WHERE is_used = FALSE ORDER BY id LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING mail
            """, user_id)
            return row['mail'] if row else None

    async def count_available_mails(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM mails WHERE is_used = FALSE")

    async def count_used_mails(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM mails WHERE is_used = TRUE")

    async def count_today_given(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM mails WHERE is_used = TRUE AND used_at::date = CURRENT_DATE"
            )

    async def delete_unused_mails(self) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM mails WHERE is_used = FALSE")
            return int(result.split()[-1])

    async def delete_used_mails(self) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM mails WHERE is_used = TRUE")
            return int(result.split()[-1])

    async def delete_all_mails(self) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM mails")
            return int(result.split()[-1])

    async def get_user_today_count(self, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM mails WHERE used_by = $1 AND used_at::date = CURRENT_DATE",
                user_id
            )

    async def get_user_mails(self, user_id: int):
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                "SELECT mail, used_at FROM mails WHERE used_by = $1 ORDER BY used_at DESC",
                user_id
            )

    async def get_user_mails_by_date(self, user_id: int, date: str):
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                "SELECT mail, used_at FROM mails WHERE used_by = $1 AND used_at::date = $2 ORDER BY used_at DESC",
                user_id, date
            )

    async def get_user_mails_by_month(self, user_id: int, month: str):
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                "SELECT mail, used_at FROM mails WHERE used_by = $1 AND to_char(used_at, 'YYYY-MM') = $2 ORDER BY used_at DESC",
                user_id, month
            )

    async def get_user_active_months(self, user_id: int):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT to_char(used_at, 'YYYY-MM') as m FROM mails WHERE used_by = $1 ORDER BY m DESC",
                user_id
            )
            return [row['m'] for row in rows]

    # ---- Settings ----

    async def get_daily_limit(self) -> int:
        async with self.pool.acquire() as conn:
            val = await conn.fetchval("SELECT value FROM settings WHERE key = 'daily_limit'")
            return int(val) if val else 3

    async def set_daily_limit(self, limit: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ('daily_limit', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                str(limit)
            )

    async def close(self):
        if self.pool:
            await self.pool.close()
