"""Admin user and exchange utilities for fullon OHLCV service."""

import os

from fullon_orm import DatabaseContext
from fullon_orm.models import Exchange


async def get_admin_exchanges() -> tuple[int, list[Exchange]]:
    """Load admin user and their exchanges from database.

    Uses ADMIN_MAIL environment variable to find admin user.

    Returns:
        tuple[int, list[Exchange]]: (admin_uid, list of admin exchanges)

    Raises:
        ValueError: If admin user not found in database
    """
    admin_email = os.getenv("ADMIN_MAIL", "admin@fullon")

    async with DatabaseContext() as db:
        # Get admin user ID
        admin_uid = await db.users.get_user_id(admin_email)
        if not admin_uid:
            raise ValueError(f"Admin user {admin_email} not found")

        # Get admin's exchanges
        exchanges = await db.exchanges.get_user_exchanges(admin_uid)

    return admin_uid, exchanges