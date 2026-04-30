import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

from app.config.supermag.config import SMConnect
from app.config.system_access.config import SystemAccess

from app.query.supermag.queries import STORES_QUERY
from app.query.system_access.queries import (
    SELECT_EXISTING_SMSTORES,
    INSERT_STORE,
)


def to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return int(s)


def to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    s = str(value).strip().replace(",", ".")
    if not s:
        return None
    return Decimal(s)


def to_date(value: Any) -> Optional[date]:
    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    s = str(value).strip()
    if not s:
        return None

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    return None


def clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def load_existing_smstores(sa: SystemAccess) -> set[int]:
    rows = sa.execute_sql(SELECT_EXISTING_SMSTORES)
    result = set()

    for row in rows:
        smstore = row.get("smstore")
        if smstore is not None:
            result.add(int(smstore))

    return result


def normalize_store_row(row: dict) -> dict:
    return {
        "region": clean_str(row.get("region")),
        "smstore": to_int(row.get("smstore")),
        "name": clean_str(row.get("name")),
        "address": clean_str(row.get("address")),
        "close_date": to_date(row.get("close_date")),
        "ukm4store": to_int(row.get("ukm4store")),
        "ukm4ip": clean_str(row.get("ukm4ip")),
        "ukm5store": to_int(row.get("ukm5store")),
        "latitude": to_decimal(row.get("latitude")),
        "longitude": to_decimal(row.get("longitude")),
    }


def sync_new_stores_only():
    sm = SMConnect()
    sa = SystemAccess()

    try:
        sm_rows = sm.execute_sql(STORES_QUERY)
        existing_smstores = load_existing_smstores(sa)

        inserted = 0
        skipped = 0

        for raw_row in sm_rows:
            row = normalize_store_row(raw_row)
            smstore = row["smstore"]

            if smstore is None:
                skipped += 1
                continue

            if smstore in existing_smstores:
                skipped += 1
                continue

            sa.execute_sql(INSERT_STORE, row, commit=True)
            existing_smstores.add(smstore)
            inserted += 1

            print(f"[ADDED] smstore={smstore} | {row['name']}")

        print(f"Готово. Добавлено новых: {inserted}, пропущено: {skipped}")

    finally:
        sm.close()
        sa.close()


if __name__ == "__main__":
    sync_new_stores_only()