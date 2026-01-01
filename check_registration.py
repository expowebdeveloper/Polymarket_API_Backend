"""
Quick diagnostic script to check if registration will work.

Run this to verify:
1. Users table exists
2. Required columns exist
3. Registration endpoint can work
"""

import asyncio
from sqlalchemy import text
from app.db.session import AsyncSessionLocal


async def check_users_table():
    """Check if users table has all required columns."""
    print("="*60)
    print("Checking Users Table for Registration")
    print("="*60)
    
    async with AsyncSessionLocal() as session:
        try:
            # Check if table exists
            result = await session.execute(
                text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'users'
                    )
                """)
            )
            table_exists = result.scalar()
            
            if not table_exists:
                print("❌ Users table does NOT exist!")
                print("\n🔧 Fix: Run migrations:")
                print("   python run_migrations.py")
                return False
            
            print("✅ Users table exists")
            
            # Check required columns
            result = await session.execute(
                text("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = 'users'
                    ORDER BY column_name
                """)
            )
            columns = result.fetchall()
            
            required_columns = {
                'id': False,
                'email': False,
                'name': False,
                'password_hash': False,
                'created_at': False,
                'updated_at': False
            }
            
            print("\n📋 Table columns:")
            for col_name, data_type, is_nullable in columns:
                print(f"   - {col_name} ({data_type}, nullable: {is_nullable})")
                if col_name in required_columns:
                    required_columns[col_name] = True
            
            # Check for missing columns
            missing = [col for col, exists in required_columns.items() if not exists]
            
            if missing:
                print(f"\n❌ Missing required columns: {', '.join(missing)}")
                print("\n🔧 Fix: Run migrations:")
                print("   python run_migrations.py")
                return False
            
            print("\n✅ All required columns exist!")
            
            # Test if we can query the table
            try:
                result = await session.execute(text("SELECT COUNT(*) FROM users"))
                count = result.scalar()
                print(f"✅ Can query table (current users: {count})")
            except Exception as e:
                print(f"❌ Cannot query table: {e}")
                return False
            
            print("\n" + "="*60)
            print("✅ Registration should work!")
            print("="*60)
            return True
            
        except Exception as e:
            print(f"\n❌ Error checking table: {e}")
            import traceback
            traceback.print_exc()
            return False


async def main():
    """Run diagnostic check."""
    success = await check_users_table()
    if not success:
        print("\n⚠️  Please run migrations before trying to register:")
        print("   python run_migrations.py")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
