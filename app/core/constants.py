"""
Application constants.
"""

# HTTP Status Messages
STATUS_HEALTHY = "healthy"
STATUS_UNHEALTHY = "unhealthy"

# Default Values
DEFAULT_CURRENT_VALUE = 0.01
DEFAULT_PAGE_SIZE = 100

# Scoring Weights
ROI_WEIGHT = 0.4
WIN_RATE_WEIGHT = 0.3
CONSISTENCY_WEIGHT = 0.2
RECENCY_WEIGHT = 0.1

# Consistency Calculation
MAX_CONSISTENCY_TRADES = 10
CONSISTENCY_WEIGHTS = list(range(MAX_CONSISTENCY_TRADES, 0, -1))

# Recency Calculation
RECENCY_DAYS = 7

# Market Resolution Values
RESOLUTION_YES = "YES"
RESOLUTION_NO = "NO"

# Default Category
DEFAULT_CATEGORY = "Uncategorized"

