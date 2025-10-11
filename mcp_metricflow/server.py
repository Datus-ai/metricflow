"""
MCP MetricFlow Server

A Model Context Protocol server that wraps MetricFlow functionality
and exposes it via tools and resources with SSE support.
"""

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server import Server
from mcp import types
from pydantic import BaseModel, Field


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetricFlowConfig(BaseModel):
    """MetricFlow configuration model.

    Note: Only model_path is actually used by MetricFlow via MF_MODEL_PATH env var.
    Other configs are kept for reference but not passed to MetricFlow commands.
    """
    model_path: str = Field(default_factory=lambda: os.path.expanduser("~/.metricflow/semantic_models"))
    config_file: str = Field(default_factory=lambda: os.path.expanduser("~/.metricflow/config.yml"))


# Models are simplified since we're returning string responses to match Datus-agent implementation


# Initialize MCP server
server = Server("MetricFlow MCP Server")


def _run_mf_command(command: List[str]) -> Dict[str, Any]:
    """Run a MetricFlow command and return the result"""
    try:
        # Set environment variables
        env = os.environ.copy()
        config = MetricFlowConfig()

        env.update({"MF_MODEL_PATH": config.model_path})

        # Run the command
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=env,
            timeout=300  # 5 minute timeout
        )

        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "stdout": "",
            "stderr": "Command timed out after 5 minutes",
            "returncode": -1
        }
    except Exception as e:
        return {
            "success": False,
            "stdout": "",
            "stderr": str(e),
            "returncode": -1
        }


# Resources removed - not needed


# Define MCP tools
@server.list_tools()
async def handle_list_tools():
    """List available MCP tools"""
    return [
        types.Tool(
            name="list_metrics",
            description="List all available metrics in the MetricFlow project",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_dimensions",
            description="Get dimensions for MetricFlow project, optionally scoped to specific metrics",
            inputSchema={
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of metric names to get dimensions for",
                    }
                },
                "required": [],
            },
        ),
        types.Tool(
            name="get_entities",
            description="Get entities for MetricFlow project, optionally scoped to specific metrics",
            inputSchema={
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of metric names to get entities for",
                    }
                },
                "required": [],
            },
        ),
        types.Tool(
            name="query_metrics",
            description="Execute a MetricFlow query and return results",
            inputSchema={
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of metric names to query (required)",
                    },
                    "dimensions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of dimensions to group by",
                    },
                    "order_by": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of fields to order by",
                    },
                    "where": {"type": "string", "description": "Optional WHERE clause filter"},
                    "limit": {"type": "integer", "description": "Optional number of rows to limit"},
                    "start_time": {"type": "string", "description": "Optional start time for the query (ISO format)"},
                    "end_time": {"type": "string", "description": "Optional end time for the query (ISO format)"},
                    "explain": {"type": "boolean", "description": "Optional flag to explain the query"},
                },
                "required": ["metrics"],
            },
        ),
        types.Tool(
            name="validate_configs",
            description="Validate MetricFlow configuration and semantic models",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_dimension_values",
            description="Get possible values for a specific dimension",
            inputSchema={
                "type": "object",
                "properties": {
                    "dimension_name": {"type": "string", "description": "Name of the dimension to get values for"},
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of metrics to scope the dimension values",
                    },
                },
                "required": ["dimension_name"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> List[types.TextContent]:
    """Handle tool execution requests"""
    try:
        if name == "list_metrics":
            result = _run_mf_command(["mf", "list-metrics"])
            if not result["success"]:
                return [types.TextContent(type="text", text=f"Failed to list metrics: {result['stderr']}")]
            return [types.TextContent(type="text", text=result["stdout"])]

        elif name == "get_dimensions":
            metrics = arguments.get("metrics")
            if metrics:
                command = ["mf", "list-dimensions", "--metrics"] + metrics
            else:
                command = ["mf", "list-dimensions"]

            result = _run_mf_command(command)
            if not result["success"]:
                return [types.TextContent(type="text", text=f"Failed to get dimensions: {result['stderr']}")]
            return [types.TextContent(type="text", text=result["stdout"])]

        elif name == "get_entities":
            metrics = arguments.get("metrics")
            if metrics:
                command = ["mf", "list-entities", "--metrics"] + metrics
            else:
                command = ["mf", "list-entities"]

            result = _run_mf_command(command)
            if not result["success"]:
                return [types.TextContent(type="text", text=f"Failed to get entities: {result['stderr']}")]
            return [types.TextContent(type="text", text=result["stdout"])]

        elif name == "query_metrics":
            metrics = arguments["metrics"]
            command = ["mf", "query", "--metrics"] + metrics

            if arguments.get("dimensions"):
                command.extend(["--dimensions"] + arguments["dimensions"])
            if arguments.get("order_by"):
                command.extend(["--order-by"] + arguments["order_by"])
            if arguments.get("where"):
                command.extend(["--where", arguments["where"]])
            if arguments.get("limit"):
                command.extend(["--limit", str(arguments["limit"])])
            if arguments.get("start_time"):
                command.extend(["--start-time", arguments["start_time"]])
            if arguments.get("end_time"):
                command.extend(["--end-time", arguments["end_time"]])
            if arguments.get("explain"):
                command.append("--explain")

            result = _run_mf_command(command)
            if not result["success"]:
                return [types.TextContent(type="text", text=f"Query failed: {result['stderr']}")]
            return [types.TextContent(type="text", text=result["stdout"])]

        elif name == "validate_configs":
            result = _run_mf_command(["mf", "validate-configs"])
            if not result["success"]:
                return [types.TextContent(type="text", text=f"Validation failed: {result['stderr']}")]
            return [types.TextContent(type="text", text=result["stdout"])]

        elif name == "get_dimension_values":
            dimension_name = arguments["dimension_name"]
            command = ["mf", "list-dimension-values", "--dimension", dimension_name]

            if arguments.get("metrics"):
                command.extend(["--metrics"] + arguments["metrics"])

            result = _run_mf_command(command)
            if not result["success"]:
                return [
                    types.TextContent(
                        type="text", text=f"Failed to get dimension values for '{dimension_name}': {result['stderr']}"
                    )
                ]
            return [types.TextContent(type="text", text=result["stdout"])]

        else:
            raise ValueError(f"Unknown tool: {name}")

    except Exception as e:
        return [types.TextContent(type="text", text=f"Error: {str(e)}")]


# All tools are now handled by the MCP server handlers above


def create_app():
    """Create the FastAPI application with MCP JSON-RPC support"""
    from fastapi import FastAPI

    # Create basic FastAPI app for MCP support
    app = FastAPI(title="MetricFlow MCP Server")

    @app.get("/")
    async def root():
        return {
            "name": "MetricFlow MCP Server",
            "status": "running",
            "endpoints": {"mcp": "/mcp", "health": "/health"},
            "available_methods": ["initialize", "notifications/initialized", "tools/list", "tools/call"],
        }

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    @app.post("/mcp")
    async def handle_mcp_request(request_data: dict):
        """Handle MCP JSON-RPC requests"""
        try:
            method = request_data.get("method")
            params = request_data.get("params", {})
            request_id = request_data.get("id")

            # Initialize connection
            if method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}, "prompts": {}},
                        "serverInfo": {"name": "MetricFlow MCP Server", "version": "0.1.0"},
                    },
                }

            # Initialize notification (no response needed)
            elif method == "notifications/initialized":
                return None  # Notification, no response

            # List tools
            elif method == "tools/list":
                tools = await handle_list_tools()
                return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": tools}}

            # Call tool
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = await handle_call_tool(tool_name, arguments)
                return {"jsonrpc": "2.0", "id": request_id, "result": {"content": result}}

            # Method not found
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": f"Method not found: {method}"},
                }

        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "error": {"code": -32603, "message": f"Internal error: {str(e)}"},
            }

    # Store references for use in tools
    app.state.mcp_server = server

    return app


if __name__ == "__main__":
    import uvicorn

    # Create the FastAPI app
    app = create_app()

    # Run the server
    port = int(os.getenv("MCP_PORT", "8080"))
    host = os.getenv("MCP_HOST", "0.0.0.0")

    logger.info(f"Starting MetricFlow MCP Server on {host}:{port}")
    logger.info("Available endpoints:")
    logger.info(f"  - MCP: http://{host}:{port}/mcp")
    logger.info(f"  - Health: http://{host}:{port}/health")

    uvicorn.run(app, host=host, port=port)
