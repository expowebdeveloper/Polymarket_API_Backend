from sqlalchemy import Column, Integer, String, Numeric, Boolean, DateTime, Text, UniqueConstraint, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class Market(Base):
    __tablename__ = "markets"

    id = Column(String, primary_key=True, index=True)
    slug = Column(String, index=True)
    question = Column(Text)
    description = Column(Text)
    status = Column(String, index=True)
    end_date = Column(DateTime)
    creation_date = Column(DateTime)
    volume = Column(Numeric(20, 8), default=0)
    liquidity = Column(Numeric(20, 8), default=0)
    open_interest = Column(Numeric(20, 8), default=0)
    image = Column(Text)
    icon = Column(Text)
    category = Column(String)
    tags = Column(Text)
    outcome_prices = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=False)
    password_hash = Column(String, nullable=False)  # Hashed password
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


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
    portfolio_value = Column(Numeric(20, 8), nullable=False, default=0)  # Current portfolio value (including cash)
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

    __table_args__ = (
        UniqueConstraint('proxy_wallet', 'asset', 'condition_id', 'timestamp', name='uq_closed_position_unique'),
    )


class LeaderboardEntry(Base):
    __tablename__ = "leaderboard_entries"
    
    id = Column(Integer, primary_key=True, index=True)
    wallet_address = Column(String(42), nullable=False, unique=True, index=True)  # Wallet address (unique)
    
    # Basic trader info
    name = Column(String(255), nullable=True)  # User name
    pseudonym = Column(String(255), nullable=True)  # User pseudonym
    profile_image = Column(Text, nullable=True)  # Profile image URL
    
    # Core metrics
    total_pnl = Column(Numeric(20, 8), nullable=False, default=0)  # Total Profit and Loss
    roi = Column(Numeric(10, 4), nullable=False, default=0)  # Return on Investment (%)
    win_rate = Column(Numeric(10, 4), nullable=False, default=0)  # Win Rate (%)
    total_trades = Column(Integer, nullable=False, default=0)  # Total number of trades
    total_trades_with_pnl = Column(Integer, nullable=False, default=0)  # Total trades with calculated PnL
    winning_trades = Column(Integer, nullable=False, default=0)  # Number of winning trades
    
    # Shrunk values
    w_shrunk = Column(Numeric(20, 10), nullable=False, default=0)  # W_shrunk (Win Rate shrunk)
    roi_shrunk = Column(Numeric(20, 10), nullable=False, default=0)  # ROI_shrunk
    pnl_shrunk = Column(Numeric(20, 10), nullable=False, default=0)  # PNL_shrunk
    
    # Score values
    score_win_rate = Column(Numeric(10, 6), nullable=False, default=0)  # W_Score
    score_roi = Column(Numeric(10, 6), nullable=False, default=0)  # ROI_Score
    score_pnl = Column(Numeric(10, 6), nullable=False, default=0)  # PNL_Score
    score_risk = Column(Numeric(10, 6), nullable=False, default=0)  # Risk_Score
    final_score = Column(Numeric(10, 4), nullable=False, default=0)  # Final_Score
    
    # Additional metrics for calculations
    total_stakes = Column(Numeric(20, 8), nullable=False, default=0)  # Total stakes
    winning_stakes = Column(Numeric(20, 8), nullable=False, default=0)  # Winning stakes
    sum_sq_stakes = Column(Numeric(20, 8), nullable=False, default=0)  # Sum of squared stakes
    max_stake = Column(Numeric(20, 8), nullable=False, default=0)  # Max stake
    worst_loss = Column(Numeric(20, 8), nullable=False, default=0)  # Worst loss
    
    # Population info (for percentile calculations)
    population_size = Column(Integer, nullable=False, default=0)  # Total population size when calculated
    
    # Timestamps
    calculated_at = Column(DateTime, default=datetime.utcnow, nullable=False)  # When this entry was calculated
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        # Index on final_score for fast sorting
        # Index on wallet_address is already created above
    )


class LeaderboardMetadata(Base):
    __tablename__ = "leaderboard_metadata"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Percentiles
    w_shrunk_1_percent = Column(Numeric(20, 10), nullable=False, default=0)
    w_shrunk_99_percent = Column(Numeric(20, 10), nullable=False, default=0)
    roi_shrunk_1_percent = Column(Numeric(20, 10), nullable=False, default=0)
    roi_shrunk_99_percent = Column(Numeric(20, 10), nullable=False, default=0)
    pnl_shrunk_1_percent = Column(Numeric(20, 10), nullable=False, default=0)
    pnl_shrunk_99_percent = Column(Numeric(20, 10), nullable=False, default=0)
    
    # Medians
    roi_median = Column(Numeric(20, 10), nullable=False, default=0)
    pnl_median = Column(Numeric(20, 10), nullable=False, default=0)
    
    # Population info
    population_size = Column(Integer, nullable=False, default=0)
    total_traders = Column(Integer, nullable=False, default=0)
    
    # Timestamps
    calculated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        # Only one metadata record
    )


class TraderLeaderboard(Base):
    """Table to store all traders from Polymarket leaderboard API."""
    __tablename__ = "trader_leaderboard"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    wallet_address = Column(String(42), nullable=False, unique=True, index=True)  # Wallet address (unique, indexed)
    
    # Basic trader info
    rank = Column(Integer, nullable=True)  # Rank from API
    name = Column(String(255), nullable=True)  # User name (userName)
    pseudonym = Column(String(255), nullable=True)  # User pseudonym (xUsername)
    profile_image = Column(Text, nullable=True)  # Profile image URL (profileImage)
    
    # Core metrics from API
    pnl = Column(Numeric(20, 8), nullable=True)  # Profit and Loss
    volume = Column(Numeric(20, 8), nullable=True)  # Trading volume (vol)
    roi = Column(Numeric(10, 4), nullable=True)  # Return on Investment (%)
    win_rate = Column(Numeric(10, 4), nullable=True)  # Win Rate (%)
    trades_count = Column(Integer, nullable=True)  # Total number of trades
    
    # Additional fields from API (stored in raw_data, but extracted for convenience)
    verified_badge = Column(Boolean, nullable=True, default=False)  # Verified badge status
    
    # Store full API response as JSON
    raw_data = Column(Text, nullable=True)  # Full API response as JSON string
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        # Index on wallet_address is already created above
    )


class TraderProfile(Base):
    """Table to store trader profile stats from Polymarket API."""
    __tablename__ = "trader_profile"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    trader_id = Column(Integer, ForeignKey("trader_leaderboard.id"), nullable=False, index=True)
    
    trades = Column(Integer, nullable=True)
    largest_win = Column(Numeric(20, 8), nullable=True)
    views = Column(Integer, nullable=True)
    join_date = Column(String(50), nullable=True)
    
    raw_data = Column(Text, nullable=True)  # Full API response as JSON
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('trader_id', name='uq_trader_profile_trader'),
    )


class TraderValue(Base):
    """Table to store trader value from Polymarket API."""
    __tablename__ = "trader_value"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    trader_id = Column(Integer, ForeignKey("trader_leaderboard.id"), nullable=False, index=True)
    
    value = Column(Numeric(20, 8), nullable=True)
    
    raw_data = Column(Text, nullable=True)  # Full API response as JSON
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('trader_id', name='uq_trader_value_trader'),
    )


class TraderPosition(Base):
    """Table to store trader positions from Polymarket API."""
    __tablename__ = "trader_positions"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    trader_id = Column(Integer, ForeignKey("trader_leaderboard.id"), nullable=False, index=True)
    
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
    
    raw_data = Column(Text, nullable=True)  # Full API response as JSON
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('trader_id', 'asset', 'condition_id', name='uq_trader_position_trader_asset_condition'),
    )


class TraderActivity(Base):
    """Table to store trader activity from Polymarket API."""
    __tablename__ = "trader_activity"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    trader_id = Column(Integer, ForeignKey("trader_leaderboard.id"), nullable=False, index=True)
    
    timestamp = Column(Integer, nullable=False, index=True)
    condition_id = Column(String(66), nullable=True, index=True)
    type = Column(String(20), nullable=False, index=True)
    size = Column(Numeric(20, 8), nullable=False, default=0)
    usdc_size = Column(Numeric(20, 8), nullable=False, default=0)
    transaction_hash = Column(String(66), nullable=False, index=True)
    price = Column(Numeric(10, 8), nullable=False, default=0)
    asset = Column(String, nullable=True)
    side = Column(String(10), nullable=True)
    outcome_index = Column(Integer, nullable=True)
    title = Column(Text, nullable=True)
    slug = Column(String(255), nullable=True, index=True)
    icon = Column(Text, nullable=True)
    event_slug = Column(String(255), nullable=True)
    outcome = Column(String(255), nullable=True)
    name = Column(String(255), nullable=True)
    pseudonym = Column(String(255), nullable=True)
    bio = Column(Text, nullable=True)
    profile_image = Column(Text, nullable=True)
    profile_image_optimized = Column(Text, nullable=True)
    
    raw_data = Column(Text, nullable=True)  # Full API response as JSON
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('trader_id', 'transaction_hash', 'timestamp', 'condition_id', name='uq_trader_activity_unique'),
    )


class TraderClosedPosition(Base):
    """Table to store trader closed positions from Polymarket API."""
    __tablename__ = "trader_closed_positions"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    trader_id = Column(Integer, ForeignKey("trader_leaderboard.id"), nullable=False, index=True)
    
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
    
    raw_data = Column(Text, nullable=True)  # Full API response as JSON
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('trader_id', 'asset', 'condition_id', 'timestamp', name='uq_trader_closed_position_unique'),
    )


class TraderTrade(Base):
    """Table to store trader trades from Polymarket API."""
    __tablename__ = "trader_trades"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    trader_id = Column(Integer, ForeignKey("trader_leaderboard.id"), nullable=False, index=True)
    
    side = Column(String(10), nullable=False, index=True)
    asset = Column(String, nullable=False, index=True)
    condition_id = Column(String(66), nullable=False, index=True)
    size = Column(Numeric(20, 8), nullable=False)
    price = Column(Numeric(10, 8), nullable=False)
    timestamp = Column(Integer, nullable=False, index=True)
    title = Column(Text, nullable=True)
    slug = Column(String(255), nullable=True, index=True)
    icon = Column(Text, nullable=True)
    event_slug = Column(String(255), nullable=True)
    outcome = Column(String(255), nullable=True)
    outcome_index = Column(Integer, nullable=True)
    name = Column(String(255), nullable=True)
    pseudonym = Column(String(255), nullable=True)
    bio = Column(Text, nullable=True)
    profile_image = Column(Text, nullable=True)
    profile_image_optimized = Column(Text, nullable=True)
    transaction_hash = Column(String(66), nullable=False, index=True)
    
    raw_data = Column(Text, nullable=True)  # Full API response as JSON
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('trader_id', 'transaction_hash', 'timestamp', 'asset', name='uq_trader_trade_unique'),
    )


class TraderCalculatedScore(Base):
    """Table to store calculated scores for traders from trader_leaderboard."""
    __tablename__ = "trader_calculated_scores"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    trader_id = Column(Integer, ForeignKey("trader_leaderboard.id"), nullable=False, index=True)
    wallet_address = Column(String(42), nullable=False, index=True)  # For quick lookup
    
    # Basic metrics
    rank = Column(Integer, nullable=True, index=True)
    total_pnl = Column(Numeric(20, 8), nullable=True)
    roi = Column(Numeric(10, 4), nullable=True)
    win_rate = Column(Numeric(10, 4), nullable=True)
    trades = Column(Integer, nullable=True)
    
    # Shrunk values
    w_shrunk = Column(Numeric(20, 10), nullable=True)
    roi_shrunk = Column(Numeric(20, 10), nullable=True)
    pnl_shrunk = Column(Numeric(20, 10), nullable=True)
    
    # Scores
    w_score = Column(Numeric(10, 6), nullable=True)
    roi_score = Column(Numeric(10, 6), nullable=True)
    pnl_score = Column(Numeric(10, 6), nullable=True)
    risk_score = Column(Numeric(10, 6), nullable=True)
    final_score = Column(Numeric(10, 4), nullable=True, index=True)
    
    # Additional metrics for reference
    total_stakes = Column(Numeric(20, 8), nullable=True)
    winning_stakes = Column(Numeric(20, 8), nullable=True)
    sum_sq_stakes = Column(Numeric(20, 8), nullable=True)
    max_stake = Column(Numeric(20, 8), nullable=True)
    worst_loss = Column(Numeric(20, 8), nullable=True)
    total_trades_with_pnl = Column(Integer, nullable=True)
    winning_trades = Column(Integer, nullable=True)
    
    # Percentile anchors (for reference)
    w_shrunk_1_percent = Column(Numeric(20, 10), nullable=True)
    w_shrunk_99_percent = Column(Numeric(20, 10), nullable=True)
    roi_shrunk_1_percent = Column(Numeric(20, 10), nullable=True)
    roi_shrunk_99_percent = Column(Numeric(20, 10), nullable=True)
    pnl_shrunk_1_percent = Column(Numeric(20, 10), nullable=True)
    pnl_shrunk_99_percent = Column(Numeric(20, 10), nullable=True)
    
    # Timestamps
    calculated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('trader_id', name='uq_trader_calculated_score_trader'),
    )


class DailyVolumeLeaderboard(Base):
    """Table to store daily volume leaderboard data from Polymarket API."""
    __tablename__ = "daily_volume_leaderboard"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    wallet_address = Column(String(42), nullable=False, index=True)  # Wallet address (not unique, can have multiple entries)
    
    # Basic trader info
    rank = Column(Integer, nullable=True)  # Rank from API
    name = Column(String(255), nullable=True)  # User name (userName)
    pseudonym = Column(String(255), nullable=True)  # User pseudonym (xUsername)
    profile_image = Column(Text, nullable=True)  # Profile image URL (profileImage)
    
    # Core metrics from API
    pnl = Column(Numeric(20, 8), nullable=True)  # Profit and Loss
    volume = Column(Numeric(20, 8), nullable=True)  # Trading volume (vol)
    
    # Advanced metrics (calculated)
    roi = Column(Numeric(10, 4), nullable=True)
    win_rate = Column(Numeric(10, 4), nullable=True)
    total_trades = Column(Integer, nullable=True)
    total_trades_with_pnl = Column(Integer, nullable=True)
    winning_trades = Column(Integer, nullable=True)
    total_stakes = Column(Numeric(20, 8), nullable=True)
    
    # Scores
    score_win_rate = Column(Numeric(10, 6), nullable=True)
    score_roi = Column(Numeric(10, 6), nullable=True)
    score_pnl = Column(Numeric(10, 6), nullable=True)
    score_risk = Column(Numeric(10, 6), nullable=True)
    final_score = Column(Numeric(10, 4), nullable=True, index=True)
    
    # Shrunk values
    w_shrunk = Column(Numeric(20, 10), nullable=True)
    roi_shrunk = Column(Numeric(20, 10), nullable=True)
    pnl_shrunk = Column(Numeric(20, 10), nullable=True)
    
    # Additional fields from API
    verified_badge = Column(Boolean, nullable=True, default=False)  # Verified badge status
    
    # Store full API response as JSON
    raw_data = Column(Text, nullable=True)  # Full API response as JSON string
    
    # Timestamps
    fetched_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)  # When this data was fetched
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

class WeeklyVolumeLeaderboard(Base):
    """Table to store weekly volume leaderboard data from Polymarket API."""
    __tablename__ = "weekly_volume_leaderboard"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    wallet_address = Column(String(42), nullable=False, index=True)  # Wallet address
    
    # Basic trader info
    rank = Column(Integer, nullable=True)  # Rank from API
    name = Column(String(255), nullable=True)  # User name (userName)
    pseudonym = Column(String(255), nullable=True)  # User pseudonym (xUsername)
    profile_image = Column(Text, nullable=True)  # Profile image URL (profileImage)
    
    # Core metrics from API
    pnl = Column(Numeric(20, 8), nullable=True)  # Profit and Loss
    volume = Column(Numeric(20, 8), nullable=True)  # Trading volume (vol)
    
    # Advanced metrics (calculated)
    roi = Column(Numeric(10, 4), nullable=True)
    win_rate = Column(Numeric(10, 4), nullable=True)
    total_trades = Column(Integer, nullable=True)
    total_trades_with_pnl = Column(Integer, nullable=True)
    winning_trades = Column(Integer, nullable=True)
    total_stakes = Column(Numeric(20, 8), nullable=True)
    
    # Scores
    score_win_rate = Column(Numeric(10, 6), nullable=True)
    score_roi = Column(Numeric(10, 6), nullable=True)
    score_pnl = Column(Numeric(10, 6), nullable=True)
    score_risk = Column(Numeric(10, 6), nullable=True)
    final_score = Column(Numeric(10, 4), nullable=True, index=True)
    
    # Shrunk values
    w_shrunk = Column(Numeric(20, 10), nullable=True)
    roi_shrunk = Column(Numeric(20, 10), nullable=True)
    pnl_shrunk = Column(Numeric(20, 10), nullable=True)
    
    # Additional fields from API
    verified_badge = Column(Boolean, nullable=True, default=False)  # Verified badge status
    
    # Store full API response as JSON
    raw_data = Column(Text, nullable=True)  # Full API response as JSON string
    
    # Timestamps
    fetched_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)  # When this data was fetched
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class MonthlyVolumeLeaderboard(Base):
    """Table to store monthly volume leaderboard data from Polymarket API."""
    __tablename__ = "monthly_volume_leaderboard"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    wallet_address = Column(String(42), nullable=False, index=True)  # Wallet address
    
    # Basic trader info
    rank = Column(Integer, nullable=True)  # Rank from API
    name = Column(String(255), nullable=True)  # User name (userName)
    pseudonym = Column(String(255), nullable=True)  # User pseudonym (xUsername)
    profile_image = Column(Text, nullable=True)  # Profile image URL (profileImage)
    
    # Core metrics from API
    pnl = Column(Numeric(20, 8), nullable=True)  # Profit and Loss
    volume = Column(Numeric(20, 8), nullable=True)  # Trading volume (vol)
    
    # Advanced metrics (calculated)
    roi = Column(Numeric(10, 4), nullable=True)
    win_rate = Column(Numeric(10, 4), nullable=True)
    total_trades = Column(Integer, nullable=True)
    total_trades_with_pnl = Column(Integer, nullable=True)
    winning_trades = Column(Integer, nullable=True)
    total_stakes = Column(Numeric(20, 8), nullable=True)
    
    # Scores
    score_win_rate = Column(Numeric(10, 6), nullable=True)
    score_roi = Column(Numeric(10, 6), nullable=True)
    score_pnl = Column(Numeric(10, 6), nullable=True)
    score_risk = Column(Numeric(10, 6), nullable=True)
    final_score = Column(Numeric(10, 4), nullable=True, index=True)
    
    # Shrunk values
    w_shrunk = Column(Numeric(20, 10), nullable=True)
    roi_shrunk = Column(Numeric(20, 10), nullable=True)
    pnl_shrunk = Column(Numeric(20, 10), nullable=True)
    
    # Additional fields from API
    verified_badge = Column(Boolean, nullable=True, default=False)  # Verified badge status
    
    # Store full API response as JSON
    raw_data = Column(Text, nullable=True)  # Full API response as JSON string
    
    # Timestamps
    fetched_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)  # When this data was fetched
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
