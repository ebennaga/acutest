from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Column, Date, DateTime, Numeric, String, Text, create_engine
)
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    pass


class Customer(Base):
    """ORM model matching the required customers table schema."""

    __tablename__ = "customers"

    customer_id     = Column(String(50),  primary_key=True)
    first_name      = Column(String(100), nullable=False)
    last_name       = Column(String(100), nullable=False)
    email           = Column(String(255), nullable=False)
    phone           = Column(String(20))
    address         = Column(Text)
    date_of_birth   = Column(Date)
    account_balance = Column(Numeric(15, 2))
    created_at      = Column(DateTime)

    def to_dict(self) -> dict:
        return {
            "customer_id":     self.customer_id,
            "first_name":      self.first_name,
            "last_name":       self.last_name,
            "email":           self.email,
            "phone":           self.phone,
            "address":         self.address,
            "date_of_birth":   self.date_of_birth.isoformat()   if self.date_of_birth   else None,
            "account_balance": float(self.account_balance)       if self.account_balance is not None else None,
            "created_at":      self.created_at.isoformat()       if self.created_at      else None,
        }


def init_db(engine) -> None:
    """Create all tables if they don't exist."""
    Base.metadata.create_all(engine)
