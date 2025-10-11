"""
Datus MetricFlow CLI - Simplified interface for MetricFlow integration with Datus.
"""

import os
import pathlib
import sys

import click

from .setup_utils import DatusMetricFlowSetup


@click.group()
@click.version_option()
def cli():
    """Datus MetricFlow - Simplified MetricFlow integration for Datus Agent."""
    pass


@cli.command()
@click.option("--demo", is_flag=True, help="Setup with demo data and DuckDB")
@click.option(
    "--namespace",
    help="Namespace from Datus config to use for setup",
)
def setup(demo: bool, namespace: str):
    """Setup Datus MetricFlow integration.

    Two setup modes:
    1. --demo: Quick demo setup with DuckDB
    2. --namespace: Load config from Datus agent configuration
    """

    click.echo("🚀 Setting up Datus MetricFlow integration...")

    setup_manager = DatusMetricFlowSetup()

    # Create necessary directories
    setup_manager.ensure_directories()

    # Mode 1: Setup from Datus namespace configuration
    if namespace:
        click.echo(f"📖 Loading configuration for namespace: {namespace}")

        # Load Datus config
        if not setup_manager.load_datus_config():
            click.echo("❌ Failed to load Datus config")
            sys.exit(1)

        # Get database config for namespace
        db_config = setup_manager.get_namespace_db_config(namespace)
        if not db_config:
            sys.exit(1)

        click.echo(f"✅ Found configuration for namespace '{namespace}'")
        click.echo(f"   Database type: {db_config.get('type')}")

        # Setup environment variables from namespace config
        setup_manager.setup_environment_variables(namespace=namespace, db_config=db_config)

        # Create semantic models directory
        semantic_models_dir = pathlib.Path.home() / ".metricflow" / "semantic_models"
        semantic_models_dir.mkdir(exist_ok=True)

        click.echo("\n✅ Setup completed!")
        click.echo(f"\nNext step: Start Datus CLI with: datus-cli --namespace {namespace}")
        return

    # Mode 2: Demo setup
    if demo:
        click.echo("🎯 Setting up demo environment with DuckDB...")

        # Create configurations
        setup_manager.create_metricflow_config(dialect="duckdb")

        # Run mf tutorial to create demo database
        setup_manager.run_mf_tutorial()

        # Setup environment variables
        setup_manager.setup_environment_variables()

        # Try to install npm packages
        setup_manager.check_npm_and_install_filesystem_server()

        # Create semantic models directory
        semantic_models_dir = pathlib.Path.home() / ".metricflow" / "semantic_models"
        semantic_models_dir.mkdir(exist_ok=True)

        click.echo("\n✅ Setup completed!")
        click.echo("\nNext step: Start Datus CLI with: datus-cli --namespace local_duckdb")
        return

    # If no mode specified, show error
    click.echo("❌ Please specify either --demo or --namespace")
    click.echo("   Examples:")
    click.echo("     datus-mf setup --demo")
    click.echo("     datus-mf setup --namespace starrocks")
    sys.exit(1)


if __name__ == '__main__':
    cli()
