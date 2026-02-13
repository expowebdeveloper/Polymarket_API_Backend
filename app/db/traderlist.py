# app/db/models/polymarket_trader.py

from sqlalchemy import Column, String, Float, Integer, Boolean, DateTime
from sqlalchemy.sql import func
from app.db.base import Base


class PolymarketTrader(Base):
    __tablename__ = "polymarket_traders"

    wallet_address = Column(String(42), primary_key=True, index=True)

    username = Column(String, index=True)
    pseudonym = Column(String)
    profile_image = Column(String)
    verified_badge = Column(Boolean, default=False)

    # Daily
    daily_rank = Column(Integer)
    daily_volume = Column(Float)
    daily_pnl = Column(Float)

    # Weekly
    weekly_rank = Column(Integer)
    weekly_volume = Column(Float)
    weekly_pnl = Column(Float)

    # Monthly
    monthly_rank = Column(Integer)
    monthly_volume = Column(Float)
    monthly_pnl = Column(Float)

    # All-time
    all_time_rank = Column(Integer)
    all_time_volume = Column(Float)
    all_time_pnl = Column(Float)

    first_seen_at = Column(DateTime(timezone=True), server_default=func.now())
    last_updated_at = Column(DateTime(timezone=True), onupdate=func.now())