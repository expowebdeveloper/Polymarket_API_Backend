# Database Migration Guide

## Overview

This project uses SQLAlchemy models with automatic table creation. When you deploy new code with new models or table changes, you need to run migrations on the server.

## Quick Start

### On the Server (Production)

1. **SSH into your server**
   ```bash
   ssh user@your-server
   ```

2. **Navigate to project directory**
   ```bash
   cd /path/to/polymarket
   ```

3. **Activate virtual environment** (if using one)
   ```bash
   source venv/bin/activate
   ```

4. **Run migrations**
   ```bash
   python run_migrations.py
   ```

5. **Restart your application server**
   ```bash
   # If using systemd
   sudo systemctl restart polymarket
   
   # If using PM2
   pm2 restart polymarket
   
   # If using Docker
   docker-compose restart app
   ```

## What the Migration Script Does

The `run_migrations.py` script:

1. ✅ Creates all tables from SQLAlchemy models
2. ✅ Runs specific migrations (e.g., adds columns to users table)
3. ✅ Ensures all trader detail tables exist
4. ✅ Verifies all critical tables are created

## Manual Migration Steps

If you prefer to run migrations manually:

### 1. Create All Tables
```bash
python -c "
import asyncio
from app.db.session import init_db
asyncio.run(init_db())
"
```

### 2. Run Users Table Migration
```bash
python migrate_users_table.py
```

### 3. Create Trader Detail Tables
```bash
python fetch_trader_details.py
# This will create tables if they don't exist
```

## Troubleshooting

### Issue: "Table already exists" errors
**Solution**: This is normal. The script checks if tables exist before creating them.

### Issue: "Column already exists" errors
**Solution**: The migration scripts check for column existence before adding them. If you see this, the migration already ran.

### Issue: Registration not working
**Possible causes**:
1. `users` table missing required columns (`password_hash`, `created_at`, `updated_at`)
2. Tables not created at all

**Solution**: Run `python run_migrations.py` to ensure all tables and columns exist.

### Issue: "Cannot connect to database"
**Solution**: 
1. Check your `.env` file has correct `DATABASE_URL`
2. Ensure PostgreSQL is running: `sudo systemctl status postgresql`
3. Check database credentials

## Automatic Migration on Startup

The application automatically runs `init_db()` on startup (see `app/main.py`), which creates all tables from models. However, this doesn't:
- Add new columns to existing tables
- Run specific migrations

That's why you need to run `run_migrations.py` after deploying new code.

## Migration Scripts Available

1. **`run_migrations.py`** - Comprehensive migration script (RECOMMENDED)
2. **`migrate_users_table.py`** - Adds password_hash, created_at, updated_at to users table
3. **`migrate_trades_table.py`** - Adds trader_id foreign key to trades table
4. **`init_database.py`** - Creates database if it doesn't exist

## Best Practices

1. **Always run migrations after deploying new code**
2. **Backup database before migrations** (for production)
3. **Test migrations on staging first**
4. **Check migration output for errors**

## Production Deployment Checklist

- [ ] Backup database
- [ ] Deploy new code
- [ ] Run `python run_migrations.py`
- [ ] Verify tables exist
- [ ] Restart application
- [ ] Test registration/login
- [ ] Monitor logs for errors
