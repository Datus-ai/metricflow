"""
Filesystem MCP Server

A Model Context Protocol server that provides filesystem access
using JSON-RPC protocol, similar to the MetricFlow MCP server.
"""

import logging
import os
from pathlib import Path
from typing import List, Optional

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




# Define MCP tools
@server.list_tools()
async def handle_list_tools():
    """List available filesystem tools"""
    return [
        types.Tool(
            name="read_file",
            description="Read the contents of a file",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "The path of the file to read"
                    }
                },
                "required": ["path"]
            }
        ),
        types.Tool(
            name="read_multiple_files",
            description="Read the contents of multiple files",
            inputSchema={
                "type": "object",
                "properties": {
                    "paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of file paths to read"
                    }
                },
                "required": ["paths"]
            }
        ),
        types.Tool(
            name="write_file",
            description="Create a new file or overwrite an existing file",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "The path of the file to write"
                    },
                    "content": {
                        "type": "string",
                        "description": "The content to write to the file"
                    }
                },
                "required": ["path", "content"]
            }
        ),
        types.Tool(
            name="edit_file",
            description="Make selective edits to a file",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "The path of the file to edit"
                    },
                    "edits": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "oldText": {"type": "string", "description": "Text to replace"},
                                "newText": {"type": "string", "description": "New text"}
                            },
                            "required": ["oldText", "newText"]
                        },
                        "description": "List of edits to apply"
                    }
                },
                "required": ["path", "edits"]
            }
        ),
        types.Tool(
            name="create_directory",
            description="Create a new directory or ensure it exists",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "The path of the directory to create"
                    }
                },
                "required": ["path"]
            }
        ),
        types.Tool(
            name="list_directory",
            description="List the contents of a directory",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "The path of the directory to list"
                    }
                },
                "required": ["path"]
            }
        ),
        types.Tool(
            name="directory_tree",
            description="Get a tree view of a directory",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "The path of the directory to analyze"
                    }
                },
                "required": ["path"]
            }
        ),
        types.Tool(
            name="move_file",
            description="Move or rename a file or directory",
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "The current path of the file or directory"
                    },
                    "destination": {
                        "type": "string",
                        "description": "The new path for the file or directory"
                    }
                },
                "required": ["source", "destination"]
            }
        )
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> List[types.TextContent]:
    """Handle tool execution requests"""
    try:
        config = FilesystemConfig()

        if name == "read_file":
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

        elif name == "read_multiple_files":
            paths = arguments["paths"]
            results = []

            for path in paths:
                target_path = _get_safe_path(config, path)
                if not target_path or not target_path.exists():
                    results.append(f"File not found: {path}")
                    continue

                if not target_path.is_file():
                    results.append(f"Path is not a file: {path}")
                    continue

                if not _is_allowed_file(config, target_path):
                    results.append(f"File type not allowed: {path}")
                    continue

                try:
                    content = target_path.read_text(encoding='utf-8')
                    results.append(f"=== {path} ===\n{content}")
                except UnicodeDecodeError:
                    results.append(f"Cannot read binary file: {path}")
                except PermissionError:
                    results.append(f"Permission denied: {path}")

            return [types.TextContent(type="text", text="\n\n".join(results))]

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

        elif name == "edit_file":
            path = arguments["path"]
            edits = arguments["edits"]
            target_path = _get_safe_path(config, path)

            if not target_path or not target_path.exists():
                return [types.TextContent(type="text", text=f"File not found: {path}")]

            if not target_path.is_file():
                return [types.TextContent(type="text", text=f"Path is not a file: {path}")]

            if not _is_allowed_file(config, target_path):
                return [types.TextContent(type="text", text=f"File type not allowed: {path}")]

            try:
                content = target_path.read_text(encoding='utf-8')

                for edit in edits:
                    old_text = edit["oldText"]
                    new_text = edit["newText"]
                    content = content.replace(old_text, new_text)

                target_path.write_text(content, encoding='utf-8')
                return [types.TextContent(type="text", text=f"File edited successfully: {path}")]
            except UnicodeDecodeError:
                return [types.TextContent(type="text", text=f"Cannot edit binary file: {path}")]
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

        elif name == "list_directory":
            path = arguments["path"]
            target_path = _get_safe_path(config, path)

            if not target_path or not target_path.exists():
                return [types.TextContent(type="text", text=f"Directory not found: {path}")]

            if not target_path.is_dir():
                return [types.TextContent(type="text", text=f"Path is not a directory: {path}")]

            try:
                items = []
                for item in sorted(target_path.iterdir()):
                    prefix = "[DIR]" if item.is_dir() else "[FILE]"
                    items.append(f"{prefix} {item.name}")

                result = f"Contents of {path}:\n" + "\n".join(items)
                return [types.TextContent(type="text", text=result)]
            except PermissionError:
                return [types.TextContent(type="text", text=f"Permission denied: {path}")]

        elif name == "directory_tree":
            path = arguments["path"]
            target_path = _get_safe_path(config, path)

            if not target_path or not target_path.exists():
                return [types.TextContent(type="text", text=f"Directory not found: {path}")]

            if not target_path.is_dir():
                return [types.TextContent(type="text", text=f"Path is not a directory: {path}")]

            try:
                def build_tree(dir_path: Path, prefix: str = "") -> List[str]:
                    lines = []
                    items = sorted(dir_path.iterdir())

                    for i, item in enumerate(items):
                        is_last = i == len(items) - 1
                        current_prefix = "└── " if is_last else "├── "

                        if item.is_dir():
                            lines.append(f"{prefix}{current_prefix}{item.name}/")
                            next_prefix = prefix + ("    " if is_last else "│   ")
                            lines.extend(build_tree(item, next_prefix))
                        else:
                            try:
                                size = item.stat().st_size
                                lines.append(f"{prefix}{current_prefix}{item.name} ({size} bytes)")
                            except:
                                lines.append(f"{prefix}{current_prefix}{item.name}")

                    return lines

                tree_lines = [f"{target_path.name}/"]
                tree_lines.extend(build_tree(target_path))

                return [types.TextContent(type="text", text="\n".join(tree_lines))]
            except PermissionError:
                return [types.TextContent(type="text", text=f"Permission denied: {path}")]

        elif name == "move_file":
            source = arguments["source"]
            destination = arguments["destination"]

            source_path = _get_safe_path(config, source)
            dest_path = _get_safe_path(config, destination)

            if not source_path or not source_path.exists():
                return [types.TextContent(type="text", text=f"Source not found: {source}")]

            if not dest_path:
                return [types.TextContent(type="text", text=f"Invalid destination: {destination}")]

            try:
                source_path.rename(dest_path)
                return [types.TextContent(type="text", text=f"Moved {source} to {destination}")]
            except PermissionError:
                return [types.TextContent(type="text", text=f"Permission denied")]
            except OSError as e:
                return [types.TextContent(type="text", text=f"Move failed: {e}")]

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