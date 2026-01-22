
from fastapi import APIRouter
from app.services.dashboard_service import dashboard_service

router = APIRouter()

@router.get("/stats")
async def get_dashboard_stats():
    """
    Returns real-time scraped stats from Polydata.
    """
    return await dashboard_service.get_stats()
