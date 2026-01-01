# Fix Registration Endpoint Issue

## Problem
The `/auth/register` endpoint is not working, likely due to missing database columns.

## Quick Fix

### Step 1: Check the Issue
Run the diagnostic script:
```bash
python check_registration.py
```

This will tell you exactly what's missing.

### Step 2: Run Migrations
```bash
python run_migrations.py
```

This will:
- ✅ Create all missing tables
- ✅ Add missing columns to existing tables
- ✅ Verify everything is set up correctly

### Step 3: Verify
Run the check again:
```bash
python check_registration.py
```

You should see: `✅ Registration should work!`

### Step 4: Test Registration
Try registering a user via:
- API: `POST http://127.0.0.1:8000/auth/register`
- Frontend: Go to the registration page

## What Was Fixed

1. **Enhanced error handling** in `/auth/register` endpoint
   - Better error messages
   - Detects database schema issues
   - Suggests running migrations

2. **Improved migration script**
   - Checks if tables exist before adding columns
   - Handles edge cases better

3. **Created diagnostic tool**
   - `check_registration.py` - Quickly diagnose registration issues

## Common Errors

### Error: "column password_hash does not exist"
**Solution**: Run `python run_migrations.py`

### Error: "table users does not exist"
**Solution**: Run `python run_migrations.py` (it creates all tables)

### Error: "Email already registered"
**Solution**: This is normal - try a different email

## Still Having Issues?

1. Check server logs for detailed error messages
2. Verify database connection in `.env` file
3. Ensure PostgreSQL is running
4. Check if tables exist: `SELECT * FROM information_schema.tables WHERE table_name = 'users';`
