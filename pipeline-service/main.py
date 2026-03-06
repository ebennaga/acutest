"""
FastAPI pipeline service — port 8000
Ingests customer data from Flask mock-server into PostgreSQL.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from models import Customer, init_db
from pipeline import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:password@postgres:5432/customer_db",
)
FLASK_BASE_URL: str = os.environ.get("FLASK_BASE_URL", "http://mock-server:5000")


def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def wait_for_db(retries: int = 20, delay: int = 5) -> None:
    """Retry DB connection — handles both DNS and connection refused errors."""
    for attempt in range(1, retries + 1):
        try:
            engine = get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established.")
            return
        except Exception as e:
            logger.warning(
                "DB not ready (attempt %d/%d): %s — retrying in %ds…",
                attempt, retries, str(e)[:100], delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to database after multiple retries.")


engine = None
SessionLocal = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine, SessionLocal
    logger.info("Starting up — waiting for database…")
    wait_for_db(retries=20, delay=5)
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    init_db(engine)
    logger.info("Database ready.")
    yield
    logger.info("Shutting down.")


app = FastAPI(
    title="Customer Pipeline Service",
    description="Ingests Flask customer data into PostgreSQL via dlt",
    version="1.0.0",
    lifespan=lifespan,
)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/api/ingest")
def ingest_customers(db: Session = Depends(get_db)) -> dict[str, Any]:
    logger.info("Ingest triggered — fetching from %s", FLASK_BASE_URL)
    try:
        records_processed = run_pipeline(flask_base_url=FLASK_BASE_URL, session=db)
    except Exception as exc:
        logger.exception("Ingest failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"Ingest failed: {exc}") from exc
    return {"status": "success", "records_processed": records_processed}


@app.get("/api/customers")
def list_customers(
    page:  int = Query(default=1,  ge=1,         description="Page number (1-based)"),
    limit: int = Query(default=10, ge=1, le=100, description="Records per page"),
    db:    Session = Depends(get_db),
) -> dict[str, Any]:
    total: int = db.scalar(select(func.count()).select_from(Customer)) or 0
    offset = (page - 1) * limit
    rows = db.scalars(
        select(Customer).order_by(Customer.customer_id).offset(offset).limit(limit)
    ).all()
    return {
        "data":        [r.to_dict() for r in rows],
        "total":       total,
        "page":        page,
        "limit":       limit,
        "total_pages": (total + limit - 1) // limit if total else 0,
    }


@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "Customer not found", "customer_id": customer_id},
        )
    return {"data": customer.to_dict()}


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "healthy", "service": "pipeline-service"}
