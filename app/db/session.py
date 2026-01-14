from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.db.base import Base

# Configure engine arguments based on database type
engine_kwargs = {
    "echo": False,
}

# PostgreSQL-specific optimizations
if "sqlite" not in settings.DATABASE_URL:
    engine_kwargs.update({
        "pool_size": 50,          # Increased for extreme concurrency
        "max_overflow": 20,       # Increased for peak loads
        "pool_timeout": 60        # Increased timeout for high contention
    })

engine = create_async_engine(
    settings.DATABASE_URL, 
    **engine_kwargs
)

AsyncSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession
)

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

async def init_db():
    """Initialize database by creating all tables."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
