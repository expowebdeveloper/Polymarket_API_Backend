"""WebSocket endpoints for real-time data streaming."""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from typing import List, Dict
import json
import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=["WebSocket"])


class ConnectionManager:
    """Manages WebSocket connections for real-time broadcasting."""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        """Accept and store new WebSocket connection."""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New WebSocket connection. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def broadcast(self, message: Dict):
        """Broadcast message to all connected clients."""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to client: {e}")
                disconnected.append(connection)
        
        # Clean up disconnected clients
        for conn in disconnected:
            self.disconnect(conn)
    
    async def send_personal(self, message: Dict, websocket: WebSocket):
        """Send message to specific client."""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Error sending to client: {e}")


# Global connection manager instance
manager = ConnectionManager()


@router.websocket("/ws/activity")
async def activity_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for real-time activity feed.
    
    Clients connect here to receive live Polymarket activities >$1000.
    """
    await manager.connect(websocket)
    
    from app.services.activity_broadcaster import broadcaster
    
    try:
        # Send welcome message
        await manager.send_personal({
            "type": "connected",
            "message": "Connected to activity feed",
            "timestamp": None
        }, websocket)
        
        # Send initial activities immediately so user doesn't see empty screen
        initial_activities = broadcaster.get_recent_activities()
        
        if initial_activities:
            logger.info(f"📤 Sending {len(initial_activities)} initial activities to new client in batch")
            await manager.send_personal({
                "type": "initial_activity_batch",
                "data": initial_activities
            }, websocket)
        
        # Keep connection alive and handle incoming messages
        while True:
            data = await websocket.receive_text()
            
            # Handle client messages (e.g., ping/pong)
            if data == "ping":
                await manager.send_personal({
                    "type": "pong",
                    "timestamp": None
                }, websocket)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("Client disconnected normally")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)
