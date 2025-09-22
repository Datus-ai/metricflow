"""
SSE Transport for MCP MetricFlow Server

Enhanced Server-Sent Events transport implementation with
streaming capabilities for MetricFlow MCP server.
"""

import asyncio
import json
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sse_starlette import EventSourceResponse


logger = logging.getLogger(__name__)


class SSETransport:
    """Server-Sent Events transport for MCP protocol"""

    def __init__(self, app: FastAPI):
        self.app = app
        self.active_connections: Dict[str, Dict] = {}
        self.setup_routes()

    def setup_routes(self):
        """Setup SSE routes"""

        @self.app.get("/sse")
        async def sse_endpoint(request: Request):
            """Main SSE endpoint for MCP communication"""
            return EventSourceResponse(
                self.event_stream(request),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Cache-Control"
                }
            )

        @self.app.get("/sse/health")
        async def sse_health():
            """SSE health check endpoint"""
            return {
                "status": "healthy",
                "active_connections": len(self.active_connections),
                "timestamp": time.time()
            }

    async def event_stream(self, request: Request) -> AsyncGenerator[str, None]:
        """Generate SSE event stream"""
        connection_id = f"conn_{int(time.time() * 1000)}"

        # Register connection
        self.active_connections[connection_id] = {
            "start_time": time.time(),
            "request": request
        }

        try:
            # Send initial connection as direct JSON-RPC message (as client expects)
            message = {
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "roots": {"listChanged": True},
                        "sampling": {},
                        "streaming": True
                    }
                }
            }
            yield json.dumps(message) + "\n"

            # Keep connection alive and handle incoming messages
            while True:
                if await request.is_disconnected():
                    break

                # Send heartbeat every 30 seconds as direct JSON-RPC message
                heartbeat_message = {
                    "jsonrpc": "2.0",
                    "method": "notifications/heartbeat",
                    "params": {
                        "timestamp": time.time(),
                        "connection_id": connection_id
                    }
                }
                yield json.dumps(heartbeat_message) + "\n"

                await asyncio.sleep(30)

        except asyncio.CancelledError:
            logger.info(f"SSE connection {connection_id} cancelled")
        except Exception as e:
            logger.error(f"SSE connection {connection_id} error: {e}")
            error_message = {
                "jsonrpc": "2.0",
                "method": "notifications/error",
                "params": {
                    "type": "connection_error",
                    "message": str(e),
                    "connection_id": connection_id
                }
            }
            yield json.dumps(error_message) + "\n"
        finally:
            # Cleanup connection
            self.active_connections.pop(connection_id, None)
            logger.info(f"SSE connection {connection_id} closed")

    def format_sse_event(self, event: str, data: Any, event_id: Optional[str] = None) -> str:
        """Format data as SSE event"""
        lines = []

        if event_id:
            lines.append(f"id: {event_id}")

        lines.append(f"event: {event}")

        # Serialize data as JSON
        if isinstance(data, (dict, list)):
            data_str = json.dumps(data)
        else:
            data_str = str(data)

        # Handle multi-line data
        for line in data_str.split('\n'):
            lines.append(f"data: {line}")

        lines.append("")  # Empty line to end event

        return '\n'.join(lines)

    async def broadcast_event(self, event: str, data: Any, target_connections: Optional[List[str]] = None):
        """Broadcast event to active connections"""
        event_data = self.format_sse_event(event, data)

        connections_to_notify = target_connections or list(self.active_connections.keys())

        for connection_id in connections_to_notify:
            if connection_id in self.active_connections:
                try:
                    # Note: In a real implementation, you'd need to maintain
                    # connection-specific queues or websockets for bidirectional communication
                    logger.info(f"Would broadcast to {connection_id}: {event}")
                except Exception as e:
                    logger.error(f"Failed to broadcast to {connection_id}: {e}")


class StreamingQueryHandler:
    """Handle streaming query responses"""

    def __init__(self, sse_transport: SSETransport):
        self.sse_transport = sse_transport

    async def stream_query_progress(self, connection_id: str, query_info: Dict[str, Any]):
        """Stream query execution progress"""
        await self.sse_transport.broadcast_event(
            "query_started",
            {
                "connection_id": connection_id,
                "query": query_info,
                "timestamp": time.time()
            },
            [connection_id]
        )

    async def stream_query_results(self, connection_id: str, results: Dict[str, Any]):
        """Stream query results"""
        await self.sse_transport.broadcast_event(
            "query_results",
            {
                "connection_id": connection_id,
                "results": results,
                "timestamp": time.time()
            },
            [connection_id]
        )

    async def stream_query_error(self, connection_id: str, error: str):
        """Stream query error"""
        await self.sse_transport.broadcast_event(
            "query_error",
            {
                "connection_id": connection_id,
                "error": error,
                "timestamp": time.time()
            },
            [connection_id]
        )