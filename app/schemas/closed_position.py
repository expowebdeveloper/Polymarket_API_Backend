from typing import Optional
from pydantic import BaseModel, computed_field
from datetime import datetime

class ClosedPositionBase(BaseModel):
    proxy_wallet: str
    asset: str
    condition_id: str
    avg_price: float
    total_bought: float
    realized_pnl: float
    cur_price: float
    title: Optional[str] = None
    slug: Optional[str] = None
    icon: Optional[str] = None
    event_slug: Optional[str] = None
    outcome: Optional[str] = None
    outcome_index: Optional[int] = None
    opposite_outcome: Optional[str] = None
    opposite_asset: Optional[str] = None
    end_date: Optional[str] = None
    timestamp: int

class ClosedPositionCreate(ClosedPositionBase):
    pass

class ClosedPosition(ClosedPositionBase):
    id: int
    created_at: datetime
    updated_at: datetime

    @computed_field
    @property
    def size(self) -> float:
        """Size field for frontend compatibility (maps to total_bought)"""
        return float(self.total_bought)

    class Config:
        from_attributes = True
