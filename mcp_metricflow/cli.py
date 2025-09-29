"""
MCP MetricFlow CLI

Command-line interface for the MetricFlow MCP server
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import uvicorn

from mcp_metricflow.server import create_app, MetricFlowConfig


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def validate_metricflow_setup():
    """Validate that MetricFlow is properly configured"""
    config = MetricFlowConfig()

    # Check if config file exists
    config_path = Path(config.config_file)
    if not config_path.exists():
        print(f"⚠️  MetricFlow config file not found at {config.config_file}")
        print("   Run 'datus-mf setup' to configure MetricFlow first")
        return False

    # Check if model path exists
    model_path = Path(config.model_path)
    if not model_path.exists():
        print(f"⚠️  MetricFlow model path not found at {config.model_path}")
        print("   Run 'datus-mf setup' to configure MetricFlow first")
        return False

    print("✅ MetricFlow configuration looks good")
    return True


def run_server(args):
    """Run the MCP server"""
    setup_logging(args.verbose)

    if not validate_metricflow_setup():
        sys.exit(1)

    # Create the FastAPI app
    app = create_app()

    # Configure server settings
    host = args.host or os.getenv("MCP_HOST", "0.0.0.0")
    port = args.port or int(os.getenv("MCP_PORT", "8080"))

    print(f"🚀 Starting MetricFlow MCP Server")
    print(f"   Host: {host}")
    print(f"   Port: {port}")
    print(f"   MCP endpoint: http://{host}:{port}/mcp")
    print(f"   Health endpoint: http://{host}:{port}/health")
    print("")
    print("Available MCP tools (matching Datus-agent):")
    print("  - list_metrics: List all available metrics")
    print("  - get_dimensions: Get dimensions for metrics")
    print("  - get_entities: Get entities for metrics")
    print("  - query_metrics: Execute MetricFlow queries")
    print("  - validate_configs: Validate MetricFlow configuration")
    print("  - get_dimension_values: Get possible values for a dimension")
    print("")
    print("Available MCP resources:")
    print("  - metricflow/config: Get MetricFlow configuration")
    print("  - metricflow/status: Get MetricFlow status and health")
    print("")

    # Run the server
    try:
        uvicorn.run(
            app,
            host=host,
            port=port,
            log_level="info" if args.verbose else "warning",
            access_log=args.verbose
        )
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"❌ Server error: {e}")
        sys.exit(1)


def test_connection(args):
    """Test connection to MetricFlow"""
    setup_logging(args.verbose)

    if not validate_metricflow_setup():
        sys.exit(1)

    print("🧪 Testing MetricFlow connection...")

    # Import here to avoid import errors if server dependencies aren't available
    from mcp_metricflow.server import list_metrics

    try:
        # Test basic MetricFlow command
        from mcp_metricflow.server import _run_mf_command
        health_result = _run_mf_command(["mf", "health-checks"])
        if health_result["success"]:
            print("✅ Health check passed")
        else:
            print(f"❌ Health check failed: {health_result.get('stderr', 'Unknown error')}")
            return False

        # Test listing metrics
        try:
            metrics_output = list_metrics()
            print(f"✅ Metrics listed successfully")
            if args.verbose and metrics_output:
                print("   Metrics output preview:")
                lines = metrics_output.split('\n')[:5]
                for line in lines:
                    if line.strip():
                        print(f"     {line.strip()}")
        except Exception as e:
            print(f"⚠️  Could not list metrics: {e}")

        print("✅ MetricFlow MCP server is ready!")
        return True

    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="MetricFlow MCP Server - Model Context Protocol wrapper for MetricFlow"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Server command
    server_parser = subparsers.add_parser("serve", help="Start the MCP server")
    server_parser.add_argument(
        "--host",
        default=None,
        help="Host to bind to (default: 0.0.0.0)"
    )
    server_parser.add_argument(
        "--port", "-p",
        type=int,
        default=None,
        help="Port to bind to (default: 8080)"
    )

    # Test command
    subparsers.add_parser("test", help="Test MetricFlow connection")

    # Parse arguments
    args = parser.parse_args()

    if args.command == "serve":
        run_server(args)
    elif args.command == "test":
        success = test_connection(args)
        sys.exit(0 if success else 1)
    else:
        # Default to serving
        args.host = None
        args.port = None
        run_server(args)


if __name__ == "__main__":
    main()