"""
Application configuration settings.
Production: Frontend https://polyrating.com | Backend https://backend.polyrating.com
"""

import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings:
    """Application settings."""
    
    # API Configuration
    # PostgreSQL connection string format: postgresql+asyncpg://user:password@host:port/database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://digamber:user_123@localhost:5432/polymarket"
    )

    class Config:
        env_file = ".env"
        
    API_TITLE: str = "Polymarket Analytics Platform"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "API for fetching trader data and computing performance scores from Polymarket"
    
    # Server Configuration
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    RELOAD: bool = os.getenv("RELOAD", "false").lower() == "true"
    
    # Production domains
    FRONTEND_URL: str = os.getenv("FRONTEND_URL", "https://polyrating.com")
    BACKEND_URL: str = os.getenv("BACKEND_URL", "https://backend.polyrating.com")

    # Polymarket API Credentials
    POLYMARKET_API_KEY: str = os.getenv(
        "POLYMARKET_API_KEY",
        "019a95c4-7769-7e71-8671-6d9d3b8e2d37"
    )
    POLYMARKET_SECRET: str = os.getenv(
        "POLYMARKET_SECRET",
        "625QbjLXrtnhSbTaUsyHu92vTWwVjtjHu_u_gtovU_o="
    )
    POLYMARKET_PASSPHRASE: str = os.getenv(
        "POLYMARKET_PASSPHRASE",
        "75ceae086387dcb1232c9e7979ca4d84001dddc99446be89c283a35d5950723f"
    )
    
    # Dome API credentials (for fetching trader data from Dome instead of Polymarket)
    DOME_API_KEY: str = os.getenv(
        "DOME_API_KEY",
        "4d8e5410-e3bf-4abf-838b-0d3b0312bdd9"
    )
    
    # API URLs - Try alternative endpoints
    POLYMARKET_BASE_URL: str = os.getenv("POLYMARKET_BASE_URL", "https://clob.polymarket.com")
    POLYMARKET_API_URL: str = os.getenv("POLYMARKET_API_URL", "https://api.polymarket.com")
    # Public Polymarket data API (used for trades instead of direct CLOB calls)
    POLYMARKET_DATA_API_URL: str = os.getenv("POLYMARKET_DATA_API_URL", "https://data-api.polymarket.com")
    # Dome API base URL (used for market research / wallet analytics)
    DOME_API_URL: str = os.getenv("DOME_API_URL", "https://api.domeapi.io/v1")
    
    # Goldsky Configuration
    # Placeholder - User must provide valid URL
    GOLDSKY_SUBGRAPH_URL: str = os.getenv(
        "GOLDSKY_SUBGRAPH_URL", 
        "https://api.goldsky.com/api/public/project_xyz/subgraphs/polymarket-indexer/v1.0.0/gn"
    )
    
    # Validation
    TARGET_WALLET: str = os.getenv(
        "TARGET_WALLET",
        "0x56687bf447db6ffa42ffe2204a05edaa20f55839"
    )
    
    # Testing/Development limits
    MARKETS_FETCH_LIMIT: int = int(os.getenv("MARKETS_FETCH_LIMIT", "50"))  # Default limit: 50 markets per page (optimized for performance)
    
    # JWT Configuration
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-change-this-in-production")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))  # 24 hours


settings = Settings()

