from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.db.base import Base

engine = create_async_engine(
    settings.DATABASE_URL, 
    echo=False,
    pool_size=10,          # Reduced from 20 to 10 to avoid connection limits
    max_overflow=5,        # Reduced from 10 to 5
    pool_timeout=30        # Timeout for getting a connection
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
