"""
FastAPI pipeline service — port 8000
Ingests customer data from Flask mock-server into PostgreSQL.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from models import Customer, init_db
from pipeline import run_pipeline

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:password@localhost:5432/customer_db",
)
FLASK_BASE_URL: str = os.environ.get("FLASK_BASE_URL", "http://mock-server:5000")

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Lifespan — create tables on startup
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up — ensuring database tables exist…")
    init_db(engine)
    logger.info("Database ready.")
    yield
    logger.info("Shutting down.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Customer Pipeline Service",
    description="Ingests Flask customer data into PostgreSQL via dlt",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# POST /api/ingest
# ---------------------------------------------------------------------------
@app.post("/api/ingest")
def ingest_customers(db: Session = Depends(get_db)) -> dict[str, Any]:
    """
    Fetch all customers from the Flask mock-server (handles pagination
    automatically) and upsert them into PostgreSQL.
    """
    logger.info("Ingest triggered — fetching from %s", FLASK_BASE_URL)
    try:
        records_processed = run_pipeline(
            flask_base_url=FLASK_BASE_URL,
            session=db,
        )
    except Exception as exc:
        logger.exception("Ingest failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"Ingest failed: {exc}") from exc

    return {"status": "success", "records_processed": records_processed}


# ---------------------------------------------------------------------------
# GET /api/customers
# ---------------------------------------------------------------------------
@app.get("/api/customers")
def list_customers(
    page:  int = Query(default=1,  ge=1,            description="Page number (1-based)"),
    limit: int = Query(default=10, ge=1,  le=100,   description="Records per page"),
    db:    Session = Depends(get_db),
) -> dict[str, Any]:
    """Return a paginated list of customers from PostgreSQL."""
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


# ---------------------------------------------------------------------------
# GET /api/customers/{customer_id}
# ---------------------------------------------------------------------------
@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    """Return a single customer by customer_id, or 404."""
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "Customer not found", "customer_id": customer_id},
        )
    return {"data": customer.to_dict()}


# ---------------------------------------------------------------------------
# GET /api/health
# ---------------------------------------------------------------------------
@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "healthy", "service": "pipeline-service"}
