"""
Fix users table structure on the server.

This script will:
1. Check current users table structure
2. Add any missing columns
3. Fix any column type issues
"""

import asyncio
from sqlalchemy import text
from app.db.session import AsyncSessionLocal


async def fix_users_table():
    """Fix users table structure."""
    print("="*60)
    print("Fixing Users Table Structure")
    print("="*60)
    
    async with AsyncSessionLocal() as session:
        try:
            # Check current columns
            print("\n📋 Current table structure:")
            result = await session.execute(
                text("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = 'users'
                    ORDER BY ordinal_position
                """)
            )
            columns = result.fetchall()
            
            existing_columns = {}
            for col_name, data_type, is_nullable, col_default in columns:
                print(f"   - {col_name}: {data_type} (nullable: {is_nullable})")
                existing_columns[col_name] = {
                    'type': data_type,
                    'nullable': is_nullable,
                    'default': col_default
                }
            
            # Required columns with their definitions
            required_columns = {
                'id': {
                    'type': 'integer',
                    'nullable': False,
                    'action': 'check'  # Should already exist as primary key
                },
                'email': {
                    'type': 'character varying',
                    'nullable': False,
                    'action': 'check'
                },
                'name': {
                    'type': 'character varying',
                    'nullable': False,
                    'action': 'check'
                },
                'password_hash': {
                    'type': 'character varying',
                    'nullable': False,
                    'action': 'add'
                },
                'created_at': {
                    'type': 'timestamp without time zone',
                    'nullable': False,
                    'action': 'add'
                },
                'updated_at': {
                    'type': 'timestamp without time zone',
                    'nullable': False,
                    'action': 'add'
                }
            }
            
            print("\n🔧 Checking and fixing columns...")
            
            fixes_applied = []
            
            # Check and add password_hash
            if 'password_hash' not in existing_columns:
                print("   Adding password_hash column...")
                await session.execute(text("""
                    ALTER TABLE users 
                    ADD COLUMN password_hash VARCHAR NOT NULL DEFAULT ''
                """))
                fixes_applied.append('password_hash')
                
                # Update existing rows
                await session.execute(text("""
                    UPDATE users 
                    SET password_hash = '' 
                    WHERE password_hash IS NULL
                """))
                
                # Remove default
                await session.execute(text("""
                    ALTER TABLE users 
                    ALTER COLUMN password_hash DROP DEFAULT
                """))
            else:
                print("   ✅ password_hash exists")
            
            # Check and add created_at
            if 'created_at' not in existing_columns:
                print("   Adding created_at column...")
                await session.execute(text("""
                    ALTER TABLE users 
                    ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                """))
                fixes_applied.append('created_at')
            else:
                print("   ✅ created_at exists")
            
            # Check and add updated_at
            if 'updated_at' not in existing_columns:
                print("   Adding updated_at column...")
                await session.execute(text("""
                    ALTER TABLE users 
                    ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                """))
                fixes_applied.append('updated_at')
            else:
                print("   ✅ updated_at exists")
            
            # Fix nullable constraints if needed
            if 'password_hash' in existing_columns:
                if existing_columns['password_hash']['nullable'] == 'YES':
                    print("   Fixing password_hash nullable constraint...")
                    # First set default for existing NULL values
                    await session.execute(text("""
                        UPDATE users 
                        SET password_hash = '' 
                        WHERE password_hash IS NULL
                    """))
                    # Then make it NOT NULL
                    await session.execute(text("""
                        ALTER TABLE users 
                        ALTER COLUMN password_hash SET NOT NULL
                    """))
                    fixes_applied.append('password_hash NOT NULL constraint')
            
            if 'created_at' in existing_columns:
                if existing_columns['created_at']['nullable'] == 'YES':
                    print("   Fixing created_at nullable constraint...")
                    await session.execute(text("""
                        UPDATE users 
                        SET created_at = CURRENT_TIMESTAMP 
                        WHERE created_at IS NULL
                    """))
                    await session.execute(text("""
                        ALTER TABLE users 
                        ALTER COLUMN created_at SET NOT NULL
                    """))
                    fixes_applied.append('created_at NOT NULL constraint')
            
            if 'updated_at' in existing_columns:
                if existing_columns['updated_at']['nullable'] == 'YES':
                    print("   Fixing updated_at nullable constraint...")
                    await session.execute(text("""
                        UPDATE users 
                        SET updated_at = CURRENT_TIMESTAMP 
                        WHERE updated_at IS NULL
                    """))
                    await session.execute(text("""
                        ALTER TABLE users 
                        ALTER COLUMN updated_at SET NOT NULL
                    """))
                    fixes_applied.append('updated_at NOT NULL constraint')
            
            await session.commit()
            
            print("\n" + "="*60)
            if fixes_applied:
                print(f"✅ Applied {len(fixes_applied)} fixes:")
                for fix in fixes_applied:
                    print(f"   - {fix}")
            else:
                print("✅ Users table structure is correct!")
            print("="*60)
            
            # Verify final structure
            print("\n📋 Final table structure:")
            result = await session.execute(
                text("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = 'users'
                    ORDER BY ordinal_position
                """)
            )
            for col_name, data_type, is_nullable in result.fetchall():
                status = "✅" if col_name in ['id', 'email', 'name', 'password_hash', 'created_at', 'updated_at'] else "  "
                print(f"   {status} {col_name}: {data_type} (nullable: {is_nullable})")
            
            return True
            
        except Exception as e:
            await session.rollback()
            print(f"\n❌ Error fixing users table: {e}")
            import traceback
            traceback.print_exc()
            return False


async def main():
    """Run the fix."""
    success = await fix_users_table()
    if success:
        print("\n✅ Users table is now ready for registration!")
    else:
        print("\n❌ Failed to fix users table. Check errors above.")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
