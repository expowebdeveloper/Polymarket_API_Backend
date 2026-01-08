import sys
import os

# Add the backend directory to sys.path
sys.path.append("/media/digamber-jha/New Volume/Polymarket/backend")

try:
    from app.db.models import Market
    print("Successfully imported Market from app.db.models")
except ImportError as e:
    print(f"ImportError: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
