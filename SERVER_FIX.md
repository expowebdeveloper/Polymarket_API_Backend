# Fix Server Registration Issue

## Problem
Registration works locally but fails on server. The `users` table exists but registration doesn't work.

## Solution Steps

### Step 1: Diagnose the Issue
SSH into your server and run:
```bash
cd /path/to/polymarket
source venv/bin/activate  # if using venv
python diagnose_server.py
```

This will show you exactly what's wrong.

### Step 2: Fix the Users Table
Run the fix script:
```bash
python fix_users_table.py
```

This will:
- ✅ Check current table structure
- ✅ Add missing columns (password_hash, created_at, updated_at)
- ✅ Fix nullable constraints
- ✅ Verify everything is correct

### Step 3: Verify
Run diagnosis again:
```bash
python diagnose_server.py
```

Should show: `✅ All checks passed! Registration should work.`

### Step 4: Restart Server
```bash
# If using systemd
sudo systemctl restart polymarket

# If using PM2
pm2 restart polymarket

# If using Docker
docker-compose restart app

# If using uvicorn directly
# Kill and restart the process
```

### Step 5: Test Registration
Try registering via:
- API: `POST http://your-server:8000/auth/register`
- Frontend: Go to registration page

## Common Server Issues

### Issue: "column password_hash does not exist"
**Solution**: Run `python fix_users_table.py`

### Issue: "null value in column violates not-null constraint"
**Solution**: Run `python fix_users_table.py` (it fixes nullable constraints)

### Issue: "relation users does not exist"
**Solution**: Run `python run_migrations.py`

### Issue: Works locally but not on server
**Possible causes**:
1. Different database on server
2. Missing columns on server
3. Different environment variables
4. Server code not updated

**Solution**:
1. Run `python diagnose_server.py` on server
2. Compare server table structure with local
3. Run `python fix_users_table.py` on server
4. Ensure server has latest code

## Quick Commands

```bash
# Full diagnostic
python diagnose_server.py

# Fix users table
python fix_users_table.py

# Run all migrations
python run_migrations.py

# Check registration readiness
python check_registration.py
```

## If Still Not Working

1. **Check server logs**:
   ```bash
   # Systemd
   sudo journalctl -u polymarket -f
   
   # PM2
   pm2 logs polymarket
   
   # Docker
   docker-compose logs -f app
   ```

2. **Check database connection**:
   ```bash
   python -c "
   from app.core.config import settings
   print('DB URL:', settings.DATABASE_URL.split('@')[-1] if '@' in settings.DATABASE_URL else 'N/A')
   "
   ```

3. **Test database directly**:
   ```bash
   psql -U your_user -d your_db -c "SELECT column_name FROM information_schema.columns WHERE table_name = 'users';"
   ```

4. **Compare local vs server**:
   - Run `python diagnose_server.py` on both
   - Compare outputs
   - Fix differences
