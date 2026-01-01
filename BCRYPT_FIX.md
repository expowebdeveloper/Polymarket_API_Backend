# Fix bcrypt/passlib Compatibility Issue

## Problem
Error: `AttributeError: module 'bcrypt' has no attribute '__about__'`
This happens when `passlib` 1.7.4 is incompatible with newer `bcrypt` versions (4.0+).

## Quick Fix

### On Server:

```bash
# Option 1: Upgrade bcrypt (recommended)
pip install --upgrade bcrypt>=4.0.1

# Option 2: OR downgrade bcrypt to compatible version
pip install bcrypt==3.2.2

# Then test
python -c "from app.services.auth_service import get_password_hash; print(get_password_hash('test'))"
```

### Or use the fix script:

```bash
python fix_bcrypt.py
```

## What Was Fixed

1. **Updated `auth_service.py`**:
   - Now uses `bcrypt` directly (more reliable)
   - Falls back to `passlib` if bcrypt not available
   - Handles password length limit (72 bytes)
   - Compatible with both old and new bcrypt versions

2. **Updated `requirements.txt`**:
   - Added explicit `bcrypt>=4.0.0` requirement

## After Fixing

1. Restart your server:
   ```bash
   sudo systemctl restart polymarket
   # OR
   pm2 restart polymarket
   ```

2. Test registration:
   ```bash
   python diagnose_server.py
   ```

3. Should now show: `✅ All checks passed!`

## Manual Fix Steps

If the script doesn't work:

```bash
# 1. Uninstall both
pip uninstall bcrypt passlib -y

# 2. Install compatible versions
pip install bcrypt==3.2.2
pip install passlib[bcrypt]==1.7.4

# 3. Test
python -c "import bcrypt; print('OK')"
python -c "from app.services.auth_service import get_password_hash; print('OK')"
```

## Why This Happens

- `passlib` 1.7.4 expects bcrypt < 4.0
- Newer bcrypt (4.0+) changed internal API
- Our fix uses bcrypt directly, avoiding passlib's compatibility issues
