"""
Database initialization script.
Run this script to create the database and tables if they don't exist.
"""

import asyncio
import sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from app.db.session import init_db
from app.core.config import settings


async def create_database_if_not_exists():
    """Create the database if it doesn't exist."""
    # Extract database name from DATABASE_URL
    db_url = settings.DATABASE_URL
    # Parse the database name
    # Format: postgresql+asyncpg://user:password@host:port/database
    if "/" in db_url:
        db_name = db_url.split("/")[-1]
        # Remove query parameters if any
        if "?" in db_name:
            db_name = db_name.split("?")[0]
        
        # Connect to postgres database to create the target database
        # Replace the database name with 'postgres'
        admin_url = db_url.rsplit("/", 1)[0] + "/postgres"
        
        try:
            admin_engine = create_async_engine(admin_url, echo=False)
            async with admin_engine.begin() as conn:
                # Check if database exists
                result = await conn.execute(
                    text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
                )
                exists = result.fetchone()
                
                if not exists:
                    print(f"Creating database '{db_name}'...")
                    await conn.execute(text(f'CREATE DATABASE "{db_name}"'))
                    print(f"Database '{db_name}' created successfully!")
                else:
                    print(f"Database '{db_name}' already exists.")
            
            await admin_engine.dispose()
        except Exception as e:
            print(f"Error creating database: {e}")
            print("Make sure PostgreSQL is running and credentials are correct.")
            sys.exit(1)


async def main():
    """Main function to initialize database."""
    print("Initializing database...")
    print(f"Database URL: {settings.DATABASE_URL.split('@')[1] if '@' in settings.DATABASE_URL else 'hidden'}")
    
    # Create database if it doesn't exist
    await create_database_if_not_exists()
    
    # Create tables
    print("Creating tables...")
    await init_db()
    print("Database initialization complete!")


if __name__ == "__main__":
    asyncio.run(main())

