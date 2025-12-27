from sqlalchemy import Column, Integer, String, Numeric, Boolean, DateTime, Text, UniqueConstraint, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    name = Column(String)


class Trader(Base):
    __tablename__ = "traders"

    id = Column(Integer, primary_key=True, index=True)
    wallet_address = Column(String(42), nullable=False, unique=True, index=True)  # Wallet address (unique)
    name = Column(String(255), nullable=True)  # User name
    pseudonym = Column(String(255), nullable=True)  # User pseudonym
    bio = Column(Text, nullable=True)  # User bio
    profile_image = Column(Text, nullable=True)  # Profile image URL
    profile_image_optimized = Column(Text, nullable=True)  # Optimized profile image URL
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    trades = relationship("Trade", back_populates="trader")
    aggregated_metrics = relationship("AggregatedMetrics", back_populates="trader")


class Position(Base):
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, index=True)
    proxy_wallet = Column(String(42), index=True, nullable=False)
    asset = Column(String, nullable=False, index=True)
    condition_id = Column(String(66), nullable=False, index=True)
    size = Column(Numeric(20, 8), nullable=False)
    avg_price = Column(Numeric(10, 6), nullable=False)
    initial_value = Column(Numeric(20, 8), nullable=False)
    current_value = Column(Numeric(20, 8), nullable=False, default=0)
    cash_pnl = Column(Numeric(20, 8), nullable=False)
    percent_pnl = Column(Numeric(10, 4), nullable=False)
    total_bought = Column(Numeric(20, 8), nullable=False)
    realized_pnl = Column(Numeric(20, 8), nullable=False, default=0)
    percent_realized_pnl = Column(Numeric(10, 4), nullable=False)
    cur_price = Column(Numeric(10, 6), nullable=False, default=0)
    redeemable = Column(Boolean, default=False)
    mergeable = Column(Boolean, default=False)
    title = Column(Text)
    slug = Column(String(255), index=True)
    icon = Column(Text)
    event_id = Column(String(50), index=True)
    event_slug = Column(String(255))
    outcome = Column(String(255))
    outcome_index = Column(Integer)
    opposite_outcome = Column(String(255))
    opposite_asset = Column(String)
    end_date = Column(String(50))
    negative_risk = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('proxy_wallet', 'asset', 'condition_id', name='uq_position_wallet_asset_condition'),
    )


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    token_id = Column(String, nullable=False, index=True)
    token_label = Column(String(10), nullable=False)  # "Yes" or "No"
    side = Column(String(10), nullable=False, index=True)  # "BUY" or "SELL"
    market_slug = Column(String(255), nullable=False, index=True)
    condition_id = Column(String(66), nullable=False, index=True)
    shares = Column(Numeric(30, 0), nullable=False)  # Large numbers for shares
    price = Column(Numeric(10, 8), nullable=False)
    tx_hash = Column(String(66), nullable=False, index=True)
    title = Column(Text)
    timestamp = Column(Integer, nullable=False, index=True)  # Unix timestamp
    order_hash = Column(String(66), nullable=False, unique=True, index=True)
    user = Column(String(42), nullable=False, index=True)  # Wallet address
    taker = Column(String(42), nullable=False, index=True)  # Taker wallet address
    shares_normalized = Column(Numeric(20, 8), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class UserPnL(Base):
    __tablename__ = "user_pnl"

    id = Column(Integer, primary_key=True, index=True)
    user_address = Column(String(42), nullable=False, index=True)  # Wallet address
    timestamp = Column(Integer, nullable=False, index=True)  # Unix timestamp (t)
    pnl = Column(Numeric(20, 8), nullable=False)  # Profit and Loss value (p)
    interval = Column(String(10), nullable=False, default="1m")  # Interval (1m, 5m, etc.)
    fidelity = Column(String(10), nullable=False, default="1d")  # Fidelity (1d, 1w, etc.)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('user_address', 'timestamp', 'interval', 'fidelity', name='uq_user_pnl_unique'),
    )


class ProfileStats(Base):
    __tablename__ = "profile_stats"

    id = Column(Integer, primary_key=True, index=True)
    proxy_address = Column(String(42), nullable=False, index=True)  # Wallet address
    username = Column(String(255), nullable=True, index=True)  # Username (optional)
    trades = Column(Integer, nullable=False, default=0)
    largest_win = Column(Numeric(20, 8), nullable=False, default=0)
    views = Column(Integer, nullable=False, default=0)
    join_date = Column(String(50), nullable=True)  # e.g., "Oct 2025"
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('proxy_address', 'username', name='uq_profile_stats_unique'),
    )


class Activity(Base):
    __tablename__ = "activities"

    id = Column(Integer, primary_key=True, index=True)
    proxy_wallet = Column(String(42), nullable=False, index=True)  # Wallet address
    timestamp = Column(Integer, nullable=False, index=True)  # Unix timestamp
    condition_id = Column(String(66), nullable=True, index=True)  # Condition ID (can be empty)
    type = Column(String(20), nullable=False, index=True)  # TRADE, REDEEM, REWARD, etc.
    size = Column(Numeric(20, 8), nullable=False, default=0)
    usdc_size = Column(Numeric(20, 8), nullable=False, default=0)
    transaction_hash = Column(String(66), nullable=False, index=True)  # Transaction hash
    price = Column(Numeric(10, 8), nullable=False, default=0)
    asset = Column(String, nullable=True)  # Asset ID (can be empty)
    side = Column(String(10), nullable=True)  # BUY, SELL (can be empty)
    outcome_index = Column(Integer, nullable=True)  # Outcome index (999 for non-trades)
    title = Column(Text, nullable=True)  # Market title
    slug = Column(String(255), nullable=True, index=True)  # Market slug
    icon = Column(Text, nullable=True)  # Icon URL
    event_slug = Column(String(255), nullable=True)  # Event slug
    outcome = Column(String(255), nullable=True)  # Outcome name
    name = Column(String(255), nullable=True)  # User name
    pseudonym = Column(String(255), nullable=True)  # User pseudonym
    bio = Column(Text, nullable=True)  # User bio
    profile_image = Column(Text, nullable=True)  # Profile image URL
    profile_image_optimized = Column(Text, nullable=True)  # Optimized profile image URL
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('proxy_wallet', 'transaction_hash', 'timestamp', 'condition_id', name='uq_activity_unique'),
    )


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    trader_id = Column(Integer, ForeignKey("traders.id"), nullable=True, index=True)  # Foreign key to Trader (nullable for backward compatibility)
    proxy_wallet = Column(String(42), nullable=False, index=True)  # Wallet address (kept for backward compatibility)
    side = Column(String(10), nullable=False, index=True)  # BUY or SELL
    asset = Column(String, nullable=False, index=True)  # Asset ID
    condition_id = Column(String(66), nullable=False, index=True)  # Condition ID
    size = Column(Numeric(20, 8), nullable=False)  # Trade size (stake)
    price = Column(Numeric(10, 8), nullable=False)  # Trade price
    entry_price = Column(Numeric(10, 8), nullable=True)  # Entry price for the position
    exit_price = Column(Numeric(10, 8), nullable=True)  # Exit price for the position
    pnl = Column(Numeric(20, 8), nullable=True)  # Profit and Loss for this trade
    timestamp = Column(Integer, nullable=False, index=True)  # Unix timestamp
    title = Column(Text, nullable=True)  # Market title
    slug = Column(String(255), nullable=True, index=True)  # Market slug
    icon = Column(Text, nullable=True)  # Icon URL
    event_slug = Column(String(255), nullable=True)  # Event slug
    outcome = Column(String(255), nullable=True)  # Outcome name
    outcome_index = Column(Integer, nullable=True)  # Outcome index
    name = Column(String(255), nullable=True)  # User name
    pseudonym = Column(String(255), nullable=True)  # User pseudonym
    bio = Column(Text, nullable=True)  # User bio
    profile_image = Column(Text, nullable=True)  # Profile image URL
    profile_image_optimized = Column(Text, nullable=True)  # Optimized profile image URL
    transaction_hash = Column(String(66), nullable=False, index=True)  # Transaction hash
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    trader = relationship("Trader", back_populates="trades")
    
    __table_args__ = (
        UniqueConstraint('proxy_wallet', 'transaction_hash', 'timestamp', 'asset', name='uq_trade_unique'),
    )


class AggregatedMetrics(Base):
    __tablename__ = "aggregated_metrics"

    id = Column(Integer, primary_key=True, index=True)
    trader_id = Column(Integer, ForeignKey("traders.id"), nullable=False, index=True)  # Foreign key to Trader
    total_trades = Column(Integer, nullable=False, default=0)  # Total number of trades
    total_stake = Column(Numeric(20, 8), nullable=False, default=0)  # Total stake (sum of all trade sizes)
    total_pnl = Column(Numeric(20, 8), nullable=False, default=0)  # Total PnL
    realized_pnl = Column(Numeric(20, 8), nullable=False, default=0)  # Realized PnL
    unrealized_pnl = Column(Numeric(20, 8), nullable=False, default=0)  # Unrealized PnL
    win_count = Column(Integer, nullable=False, default=0)  # Number of winning trades
    loss_count = Column(Integer, nullable=False, default=0)  # Number of losing trades
    win_rate = Column(Numeric(5, 2), nullable=False, default=0)  # Win rate percentage
    avg_trade_size = Column(Numeric(20, 8), nullable=False, default=0)  # Average trade size
    largest_win = Column(Numeric(20, 8), nullable=False, default=0)  # Largest winning trade
    largest_loss = Column(Numeric(20, 8), nullable=False, default=0)  # Largest losing trade
    total_volume = Column(Numeric(20, 8), nullable=False, default=0)  # Total trading volume
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    trader = relationship("Trader", back_populates="aggregated_metrics")
    
    __table_args__ = (
        UniqueConstraint('trader_id', name='uq_aggregated_metrics_trader'),
    )


class ClosedPosition(Base):
    __tablename__ = "closed_positions"

    id = Column(Integer, primary_key=True, index=True)
    proxy_wallet = Column(String(42), nullable=False, index=True)
    asset = Column(String, nullable=False, index=True)
    condition_id = Column(String(66), nullable=False, index=True)
    avg_price = Column(Numeric(10, 8), nullable=False)
    total_bought = Column(Numeric(20, 8), nullable=False)
    realized_pnl = Column(Numeric(20, 8), nullable=False)
    cur_price = Column(Numeric(10, 8), nullable=False)
    title = Column(Text)
    slug = Column(String(255), index=True)
    icon = Column(Text)
    event_slug = Column(String(255))
    outcome = Column(String(255))
    outcome_index = Column(Integer)
    opposite_outcome = Column(String(255))
    opposite_asset = Column(String)
    end_date = Column(String(50))
    timestamp = Column(Integer, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Market(Base):
    __tablename__ = "markets"

    id = Column(String, primary_key=True, index=True)  # Polymarket Market ID or Slug
    slug = Column(String(255), unique=True, index=True)
    question = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    status = Column(String(50), index=True)  # active, closed, resolved
    end_date = Column(DateTime, nullable=True)
    creation_date = Column(DateTime, nullable=True)
    volume = Column(Numeric(20, 8), default=0)
    liquidity = Column(Numeric(20, 8), default=0)
    open_interest = Column(Numeric(20, 8), default=0)
    image = Column(Text, nullable=True)
    icon = Column(Text, nullable=True)
    category = Column(String(255), nullable=True)
    tags = Column(Text, nullable=True)  # Comma-separated or JSON string
    outcome_prices = Column(Text, nullable=True)  # JSON string of outcomes and prices
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
