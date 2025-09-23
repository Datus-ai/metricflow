"""
Filesystem MCP Server

A Model Context Protocol server that provides filesystem access
using JSON-RPC protocol, similar to the MetricFlow MCP server.
"""

import logging
import os
import mimetypes
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server import Server
from mcp import types
from pydantic import BaseModel, Field

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilesystemConfig(BaseModel):
    """Filesystem configuration model"""
    root_path: str = Field(default_factory=lambda: os.getenv('FILESYSTEM_ROOT_PATH', os.path.expanduser("~")))
    allowed_extensions: List[str] = Field(default_factory=lambda: [
        ".txt", ".md", ".py", ".js", ".ts", ".json", ".yaml", ".yml",
        ".csv", ".sql", ".html", ".css", ".xml"
    ])
    max_file_size: int = 1024 * 1024  # 1MB default


# Initialize MCP server
server = Server("Filesystem MCP Server")


def _get_safe_path(config: FilesystemConfig, path: str) -> Optional[Path]:
    """Get a safe path within the root directory"""
    try:
        root = Path(config.root_path).resolve()
        target = (root / path).resolve()

        # Ensure the target is within the root directory
        if not str(target).startswith(str(root)):
            return None

        return target
    except Exception:
        return None


def _is_allowed_file(config: FilesystemConfig, file_path: Path) -> bool:
    """Check if file extension is allowed"""
    if not config.allowed_extensions:
        return True
    return file_path.suffix.lower() in config.allowed_extensions


def _get_file_info(file_path: Path) -> Dict[str, Any]:
    """Get file information"""
    try:
        stat = file_path.stat()
        mime_type, _ = mimetypes.guess_type(str(file_path))

        return {
            "name": file_path.name,
            "path": str(file_path),
            "size": stat.st_size,
            "modified": stat.st_mtime,
            "is_file": file_path.is_file(),
            "is_directory": file_path.is_dir(),
            "mime_type": mime_type,
            "extension": file_path.suffix
        }
    except Exception as e:
        return {"error": str(e)}


# Define MCP tools
@server.list_tools()
async def handle_list_tools():
    """List available filesystem tools"""
    return [
        types.Tool(
            name="list_directory",
            description="List contents of a directory",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Directory path to list (relative to root)",
                        "default": "."
                    }
                },
                "required": []
            }
        ),
        types.Tool(
            name="read_file",
            description="Read contents of a text file",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "File path to read (relative to root)"
                    }
                },
                "required": ["path"]
            }
        ),
        types.Tool(
            name="write_file",
            description="Write content to a file",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "File path to write (relative to root)"
                    },
                    "content": {
                        "type": "string",
                        "description": "Content to write to the file"
                    }
                },
                "required": ["path", "content"]
            }
        ),
        types.Tool(
            name="create_directory",
            description="Create a new directory",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Directory path to create (relative to root)"
                    }
                },
                "required": ["path"]
            }
        ),
        types.Tool(
            name="delete_file",
            description="Delete a file or directory",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to delete (relative to root)"
                    }
                },
                "required": ["path"]
            }
        ),
        types.Tool(
            name="file_info",
            description="Get information about a file or directory",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to get info for (relative to root)"
                    }
                },
                "required": ["path"]
            }
        )
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> List[types.TextContent]:
    """Handle tool execution requests"""
    try:
        config = FilesystemConfig()

        if name == "list_directory":
            path = arguments.get("path", ".")
            target_path = _get_safe_path(config, path)

            if not target_path or not target_path.exists():
                return [types.TextContent(type="text", text=f"Directory not found: {path}")]

            if not target_path.is_dir():
                return [types.TextContent(type="text", text=f"Path is not a directory: {path}")]

            try:
                items = []
                for item in sorted(target_path.iterdir()):
                    info = _get_file_info(item)
                    items.append(info)

                result = {
                    "directory": str(target_path),
                    "items": items,
                    "count": len(items)
                }
                return [types.TextContent(type="text", text=str(result))]
            except PermissionError:
                return [types.TextContent(type="text", text=f"Permission denied: {path}")]

        elif name == "read_file":
            path = arguments["path"]
            target_path = _get_safe_path(config, path)

            if not target_path or not target_path.exists():
                return [types.TextContent(type="text", text=f"File not found: {path}")]

            if not target_path.is_file():
                return [types.TextContent(type="text", text=f"Path is not a file: {path}")]

            if not _is_allowed_file(config, target_path):
                return [types.TextContent(type="text", text=f"File type not allowed: {path}")]

            if target_path.stat().st_size > config.max_file_size:
                return [types.TextContent(type="text", text=f"File too large: {path}")]

            try:
                content = target_path.read_text(encoding='utf-8')
                return [types.TextContent(type="text", text=content)]
            except UnicodeDecodeError:
                return [types.TextContent(type="text", text=f"Cannot read binary file: {path}")]
            except PermissionError:
                return [types.TextContent(type="text", text=f"Permission denied: {path}")]

        elif name == "write_file":
            path = arguments["path"]
            content = arguments["content"]
            target_path = _get_safe_path(config, path)

            if not target_path:
                return [types.TextContent(type="text", text=f"Invalid path: {path}")]

            if not _is_allowed_file(config, target_path):
                return [types.TextContent(type="text", text=f"File type not allowed: {path}")]

            try:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_text(content, encoding='utf-8')
                return [types.TextContent(type="text", text=f"File written successfully: {path}")]
            except PermissionError:
                return [types.TextContent(type="text", text=f"Permission denied: {path}")]

        elif name == "create_directory":
            path = arguments["path"]
            target_path = _get_safe_path(config, path)

            if not target_path:
                return [types.TextContent(type="text", text=f"Invalid path: {path}")]

            try:
                target_path.mkdir(parents=True, exist_ok=True)
                return [types.TextContent(type="text", text=f"Directory created: {path}")]
            except PermissionError:
                return [types.TextContent(type="text", text=f"Permission denied: {path}")]

        elif name == "delete_file":
            path = arguments["path"]
            target_path = _get_safe_path(config, path)

            if not target_path or not target_path.exists():
                return [types.TextContent(type="text", text=f"Path not found: {path}")]

            try:
                if target_path.is_file():
                    target_path.unlink()
                    return [types.TextContent(type="text", text=f"File deleted: {path}")]
                elif target_path.is_dir():
                    target_path.rmdir()
                    return [types.TextContent(type="text", text=f"Directory deleted: {path}")]
                else:
                    return [types.TextContent(type="text", text=f"Cannot delete: {path}")]
            except OSError as e:
                return [types.TextContent(type="text", text=f"Delete failed: {e}")]

        elif name == "file_info":
            path = arguments["path"]
            target_path = _get_safe_path(config, path)

            if not target_path or not target_path.exists():
                return [types.TextContent(type="text", text=f"Path not found: {path}")]

            info = _get_file_info(target_path)
            return [types.TextContent(type="text", text=str(info))]

        else:
            raise ValueError(f"Unknown tool: {name}")

    except Exception as e:
        return [types.TextContent(type="text", text=f"Error: {str(e)}")]


def create_filesystem_app(root_path: str = None):
    """Create the FastAPI application for filesystem MCP server"""
    from fastapi import FastAPI

    # Update config if root_path is provided
    if root_path:
        os.environ['FILESYSTEM_ROOT_PATH'] = root_path

    # Create basic FastAPI app for MCP support
    app = FastAPI(title="Filesystem MCP Server")

    @app.get("/")
    async def root():
        config = FilesystemConfig()
        return {
            "name": "Filesystem MCP Server",
            "status": "running",
            "root_path": config.root_path,
            "endpoints": {"mcp": "/mcp", "health": "/health"},
            "available_methods": ["initialize", "notifications/initialized", "tools/list", "tools/call"]
        }

    @app.get("/health")
    async def health():
        config = FilesystemConfig()
        return {
            "status": "healthy",
            "root_path": config.root_path,
            "root_exists": Path(config.root_path).exists()
        }

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
                        "serverInfo": {"name": "Filesystem MCP Server", "version": "0.1.0"}
                    }
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
                    "error": {"code": -32601, "message": f"Method not found: {method}"}
                }

        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "error": {"code": -32603, "message": f"Internal error: {str(e)}"}
            }

    # Store references for use in tools
    app.state.mcp_server = server

    return app


if __name__ == "__main__":
    import uvicorn
    import sys

    # Get root path from command line argument or environment
    root_path = sys.argv[1] if len(sys.argv) > 1 else os.getenv('FILESYSTEM_ROOT_PATH')

    # Create the FastAPI app
    app = create_filesystem_app(root_path)

    # Run the server
    port = int(os.getenv("FILESYSTEM_MCP_PORT", "8081"))
    host = os.getenv("FILESYSTEM_MCP_HOST", "0.0.0.0")

    logger.info(f"Starting Filesystem MCP Server on {host}:{port}")
    logger.info(f"Root directory: {root_path or '~'}")
    logger.info("Available endpoints:")
    logger.info(f"  - MCP: http://{host}:{port}/mcp")
    logger.info(f"  - Health: http://{host}:{port}/health")

    uvicorn.run(app, host=host, port=port)