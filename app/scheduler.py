from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.services.market_service import update_all_markets

scheduler = AsyncIOScheduler()

def start_scheduler():
    """
    Starts the scheduler with defined jobs.
    """
    # Schedule market update job every 2 hours
    scheduler.add_job(
        update_all_markets,
        trigger=IntervalTrigger(hours=2),
        id="update_all_markets",
        name="Update all Polymarket markets",
        replace_existing=True
    )
    
    scheduler.start()
    print("Scheduler started. Jobs:")
    for job in scheduler.get_jobs():
        print(f"- {job.name} (Next run: {job.next_run_time})")
