"""
Migration script to add password_hash, created_at, and updated_at columns to users table.
"""

import asyncio
from sqlalchemy import text
from app.db.session import AsyncSessionLocal, engine
from app.core.config import settings


async def migrate_users_table():
    """Add missing columns to users table."""
    async with AsyncSessionLocal() as session:
        try:
            # Check if password_hash column exists
            check_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'password_hash'
            """)
            result = await session.execute(check_query)
            exists = result.scalar_one_or_none()
            
            if not exists:
                print("Adding password_hash, created_at, and updated_at columns to users table...")
                
                # Add password_hash column
                await session.execute(text("""
                    ALTER TABLE users 
                    ADD COLUMN IF NOT EXISTS password_hash VARCHAR NOT NULL DEFAULT ''
                """))
                
                # Add created_at column
                await session.execute(text("""
                    ALTER TABLE users 
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                """))
                
                # Add updated_at column
                await session.execute(text("""
                    ALTER TABLE users 
                    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                """))
                
                # Update existing rows to have non-empty password_hash (for existing users)
                # In production, you'd want to handle this differently
                await session.execute(text("""
                    UPDATE users 
                    SET password_hash = '' 
                    WHERE password_hash IS NULL OR password_hash = ''
                """))
                
                # Make password_hash NOT NULL (remove default after setting values)
                await session.execute(text("""
                    ALTER TABLE users 
                    ALTER COLUMN password_hash DROP DEFAULT
                """))
                
                await session.commit()
                print("✅ Successfully added columns to users table")
            else:
                print("✅ Columns already exist in users table")
                
        except Exception as e:
            await session.rollback()
            print(f"❌ Error migrating users table: {e}")
            raise


async def main():
    """Run migration."""
    print("Starting users table migration...")
    await migrate_users_table()
    print("Migration completed!")


if __name__ == "__main__":
    asyncio.run(main())
