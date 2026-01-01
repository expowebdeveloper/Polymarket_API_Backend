"""
Diagnose server registration issues.

This script will:
1. Check users table structure
2. Test if we can insert a test user
3. Show the exact error that would occur
"""

import asyncio
from sqlalchemy import text
from app.db.session import AsyncSessionLocal
from app.db.models import User
from app.services.auth_service import get_password_hash
from sqlalchemy.future import select


async def diagnose():
    """Diagnose the issue."""
    print("="*60)
    print("Server Registration Diagnosis")
    print("="*60)
    
    async with AsyncSessionLocal() as session:
        try:
            # Step 1: Check table structure
            print("\n1️⃣ Checking users table structure...")
            result = await session.execute(
                text("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = 'users'
                    ORDER BY ordinal_position
                """)
            )
            columns = result.fetchall()
            
            if not columns:
                print("❌ Users table has no columns! This is very wrong.")
                return
            
            print(f"✅ Found {len(columns)} columns:")
            column_dict = {}
            for col_name, data_type, is_nullable, col_default in columns:
                print(f"   - {col_name}: {data_type} (nullable: {is_nullable})")
                column_dict[col_name] = {
                    'type': data_type,
                    'nullable': is_nullable == 'YES',
                    'default': col_default
                }
            
            # Step 2: Check required columns
            print("\n2️⃣ Checking required columns...")
            required = ['id', 'email', 'name', 'password_hash', 'created_at', 'updated_at']
            missing = [col for col in required if col not in column_dict]
            
            if missing:
                print(f"❌ Missing columns: {', '.join(missing)}")
                print("\n🔧 Run: python fix_users_table.py")
                return
            else:
                print("✅ All required columns exist")
            
            # Step 3: Check constraints
            print("\n3️⃣ Checking constraints...")
            result = await session.execute(
                text("""
                    SELECT constraint_name, constraint_type
                    FROM information_schema.table_constraints
                    WHERE table_name = 'users'
                """)
            )
            constraints = result.fetchall()
            print(f"   Found {len(constraints)} constraints:")
            for name, ctype in constraints:
                print(f"   - {name}: {ctype}")
            
            # Step 4: Try to create a test user (then delete it)
            print("\n4️⃣ Testing user creation...")
            try:
                test_email = "test_diagnosis_12345@example.com"
                
                # Check if test user exists
                result = await session.execute(
                    select(User).where(User.email == test_email)
                )
                existing = result.scalar_one_or_none()
                if existing:
                    # Delete it first
                    await session.delete(existing)
                    await session.commit()
                
                # Try to create
                test_user = User(
                    email=test_email,
                    name="Test User",
                    password_hash=get_password_hash("test123")
                )
                
                session.add(test_user)
                await session.flush()  # Don't commit yet
                
                print("✅ User creation test passed!")
                
                # Clean up
                await session.delete(test_user)
                await session.commit()
                print("✅ Test user cleaned up")
                
            except Exception as e:
                await session.rollback()
                print(f"❌ User creation test FAILED:")
                print(f"   Error: {e}")
                print(f"   Error type: {type(e).__name__}")
                
                # Try to get more details
                error_str = str(e).lower()
                if "column" in error_str and "does not exist" in error_str:
                    print("\n🔧 Issue: Missing column")
                    print("   Solution: Run python fix_users_table.py")
                elif "null" in error_str or "not null" in error_str:
                    print("\n🔧 Issue: NULL constraint violation")
                    print("   Solution: Run python fix_users_table.py")
                elif "duplicate" in error_str or "unique" in error_str:
                    print("\n🔧 Issue: Unique constraint (this is OK for test)")
                else:
                    print("\n🔧 Unknown issue - check error details above")
                
                import traceback
                print("\nFull traceback:")
                traceback.print_exc()
                return
            
            # Step 5: Check if we can query
            print("\n5️⃣ Testing queries...")
            try:
                result = await session.execute(text("SELECT COUNT(*) FROM users"))
                count = result.scalar()
                print(f"✅ Can query table (current users: {count})")
            except Exception as e:
                print(f"❌ Cannot query table: {e}")
                return
            
            print("\n" + "="*60)
            print("✅ All checks passed! Registration should work.")
            print("="*60)
            print("\nIf registration still fails, check:")
            print("1. Server logs for the exact error")
            print("2. CORS settings")
            print("3. Request format (Content-Type: application/json)")
            print("4. Database connection on server")
            
        except Exception as e:
            print(f"\n❌ Diagnosis error: {e}")
            import traceback
            traceback.print_exc()


async def main():
    """Run diagnosis."""
    await diagnose()


if __name__ == "__main__":
    asyncio.run(main())
