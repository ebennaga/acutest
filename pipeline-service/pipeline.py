"""
dlt-based pipeline: fetches paginated data from the Flask mock-server
and upserts it into PostgreSQL via SQLAlchemy.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Generator, Iterator

import dlt
import httpx
from dlt.sources import DltResource
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from models import Customer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# dlt source: paginated fetch from Flask API
# ---------------------------------------------------------------------------

@dlt.source(name="flask_customers")
def flask_customer_source(base_url: str, page_size: int = 50):
    """dlt source that paginates through the Flask /api/customers endpoint."""

    @dlt.resource(name="customers", write_disposition="replace")
    def customers_resource() -> Iterator[dict]:
        page = 1
        fetched = 0

        with httpx.Client(timeout=30) as client:
            while True:
                url = f"{base_url}/api/customers"
                params = {"page": page, "limit": page_size}

                logger.info("Fetching page %d from %s", page, url)
                response = client.get(url, params=params)
                response.raise_for_status()

                payload = response.json()
                records: list[dict] = payload.get("data", [])

                if not records:
                    break

                for record in records:
                    yield record
                    fetched += 1

                total = payload.get("total", 0)
                if fetched >= total:
                    break

                page += 1

    return customers_resource()


# ---------------------------------------------------------------------------
# Helpers: type coercion before upsert
# ---------------------------------------------------------------------------

def _coerce_record(raw: dict) -> dict:
    """Convert raw JSON types to Python types expected by SQLAlchemy."""
    record = dict(raw)

    # Flatten nested address dict → JSON string stored in TEXT column
    addr = record.get("address")
    if isinstance(addr, dict):
        record["address"] = json.dumps(addr)

    # Parse date string
    dob = record.get("date_of_birth")
    if isinstance(dob, str):
        record["date_of_birth"] = date.fromisoformat(dob)

    # Parse timestamp string (strip trailing Z for fromisoformat compat)
    cat = record.get("created_at")
    if isinstance(cat, str):
        record["created_at"] = datetime.fromisoformat(cat.replace("Z", "+00:00"))

    # Ensure Decimal for balance
    bal = record.get("account_balance")
    if bal is not None:
        record["account_balance"] = Decimal(str(bal))

    return record


# ---------------------------------------------------------------------------
# Upsert logic using PostgreSQL INSERT … ON CONFLICT
# ---------------------------------------------------------------------------

def upsert_customers(session: Session, records: list[dict]) -> int:
    """
    Upsert a list of raw customer dicts into the customers table.
    Returns the number of rows processed.
    """
    if not records:
        return 0

    coerced = [_coerce_record(r) for r in records]

    # Columns to update when a conflict on customer_id is found
    update_cols = [
        "first_name", "last_name", "email", "phone",
        "address", "date_of_birth", "account_balance", "created_at",
    ]

    stmt = pg_insert(Customer).values(coerced)
    stmt = stmt.on_conflict_do_update(
        index_elements=["customer_id"],
        set_={col: stmt.excluded[col] for col in update_cols},
    )

    session.execute(stmt)
    session.commit()
    return len(coerced)


# ---------------------------------------------------------------------------
# High-level pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(flask_base_url: str, session: Session) -> int:
    """
    Fetch all customers from Flask (auto-paginated via dlt) and upsert
    them into PostgreSQL.  Returns total records processed.
    """
    source = flask_customer_source(base_url=flask_base_url, page_size=50)

    all_records: list[dict] = []
    for resource in source.resources.values():
        for record in resource:
            all_records.append(record)

    logger.info("Fetched %d records from Flask; upserting…", len(all_records))
    processed = upsert_customers(session, all_records)
    logger.info("Upsert complete — %d records processed.", processed)
    return processed
