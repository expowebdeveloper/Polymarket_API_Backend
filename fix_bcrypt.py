"""
Fix bcrypt/passlib compatibility issue on server.

Run this on the server to fix the bcrypt version issue.
"""

import subprocess
import sys

def fix_bcrypt():
    """Fix bcrypt installation."""
    print("="*60)
    print("Fixing bcrypt/passlib Compatibility")
    print("="*60)
    
    print("\n1️⃣ Checking current versions...")
    try:
        import bcrypt
        print(f"   bcrypt version: {bcrypt.__version__}")
    except ImportError:
        print("   ❌ bcrypt not installed")
    except AttributeError:
        print("   ⚠️  bcrypt installed but version unknown")
    
    try:
        import passlib
        print(f"   passlib version: {passlib.__version__}")
    except ImportError:
        print("   ❌ passlib not installed")
    except AttributeError:
        print("   ⚠️  passlib installed but version unknown")
    
    print("\n2️⃣ Installing/upgrading bcrypt...")
    try:
        # Install bcrypt 4.0.1 (compatible with newer passlib)
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "--upgrade", "bcrypt>=4.0.1"
        ])
        print("   ✅ bcrypt upgraded")
    except subprocess.CalledProcessError as e:
        print(f"   ⚠️  Error upgrading bcrypt: {e}")
        print("   Trying alternative: installing bcrypt 3.2.2 (compatible with passlib 1.7.4)")
        try:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", "bcrypt==3.2.2"
            ])
            print("   ✅ bcrypt 3.2.2 installed")
        except subprocess.CalledProcessError as e2:
            print(f"   ❌ Failed to install bcrypt: {e2}")
            return False
    
    print("\n3️⃣ Testing bcrypt...")
    try:
        import bcrypt
        password = b"test123"
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password, salt)
        result = bcrypt.checkpw(password, hashed)
        if result:
            print("   ✅ bcrypt works correctly")
        else:
            print("   ❌ bcrypt test failed")
            return False
    except Exception as e:
        print(f"   ❌ bcrypt test failed: {e}")
        return False
    
    print("\n4️⃣ Testing password hashing...")
    try:
        from app.services.auth_service import get_password_hash, verify_password
        test_password = "test123"
        hashed = get_password_hash(test_password)
        if verify_password(test_password, hashed):
            print("   ✅ Password hashing works correctly")
        else:
            print("   ❌ Password verification failed")
            return False
    except Exception as e:
        print(f"   ❌ Password hashing test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "="*60)
    print("✅ bcrypt/passlib compatibility fixed!")
    print("="*60)
    return True


if __name__ == "__main__":
    success = fix_bcrypt()
    if not success:
        print("\n❌ Failed to fix bcrypt. Try manually:")
        print("   pip install --upgrade bcrypt")
        print("   OR")
        print("   pip install bcrypt==3.2.2")
        sys.exit(1)
