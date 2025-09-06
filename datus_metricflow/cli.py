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
@click.option('--demo', is_flag=True, help='Setup with demo data and DuckDB')
@click.option('--dialect', default='duckdb', 
              type=click.Choice(['duckdb', 'snowflake', 'bigquery', 'redshift', 'postgresql']),
              help='Database dialect to configure')
def setup(demo: bool, dialect: str):
    """Setup Datus MetricFlow integration."""
    
    click.echo("🚀 Setting up Datus MetricFlow integration...")
    
    setup_manager = DatusMetricFlowSetup()
    
    # Create necessary directories
    setup_manager.ensure_directories()
    
    # Setup MetricFlow configuration
    if demo:
        dialect = 'duckdb'
        click.echo("🎯 Setting up demo environment with DuckDB...")
        
        # Create demo database
        setup_manager.create_demo_database()
    
    # Create configurations
    mf_config = setup_manager.create_metricflow_config(dialect)
    datus_config = setup_manager.create_datus_agent_config()
    
    # Setup environment variables
    env_vars = setup_manager.setup_environment_variables()
    
    # Try to install npm packages
    setup_manager.check_npm_and_install_filesystem_server()
    
    # Create semantic models directory
    semantic_models_dir = pathlib.Path.home() / ".metricflow" / "semantic_models"
    semantic_models_dir.mkdir(exist_ok=True)
    
    click.echo("\n✅ Setup completed!")
    click.echo("\nNext steps:")
    click.echo("1. Source environment variables: source ~/.metricflow/datus_env.sh")
    
    if demo:
        click.echo("2. Test MetricFlow: mf validate-configs")
        click.echo("3. Start Datus CLI: datus-cli --namespace local_duckdb")
    else:
        click.echo("2. Edit ~/.metricflow/config.yml with your database credentials")
        click.echo("3. Test connection: mf health-checks")
        click.echo("4. Validate configs: mf validate-configs")


@cli.command()
@click.option('--question', '-q', help='Ask a natural language question about your data')
def query(question: str):
    """Query data using natural language (requires Datus Agent)."""
    
    if not question:
        question = click.prompt("Enter your question about the data")
    
    click.echo(f"🤔 Processing question: {question}")
    click.echo("💡 This requires Datus Agent to be running.")
    click.echo("   Please use: datus-cli --namespace local_duckdb")
    click.echo(f"   Then ask: {question}")


@cli.command()
def validate():
    """Validate MetricFlow configurations."""
    
    # Import and run MetricFlow validate command
    try:
        from metricflow.cli.main import cli as mf_cli
        
        # Set up context and run validation
        import subprocess
        result = subprocess.run(['mf', 'validate-configs'], capture_output=True, text=True)
        
        click.echo(result.stdout)
        if result.stderr:
            click.echo(result.stderr, err=True)
            
        sys.exit(result.returncode)
        
    except ImportError:
        click.echo("❌ MetricFlow not properly installed")
        sys.exit(1)


@cli.command()
def health_checks():
    """Run MetricFlow health checks."""
    
    try:
        import subprocess
        result = subprocess.run(['mf', 'health-checks'], capture_output=True, text=True)
        
        click.echo(result.stdout)
        if result.stderr:
            click.echo(result.stderr, err=True)
            
        sys.exit(result.returncode)
        
    except Exception as e:
        click.echo(f"❌ Error running health checks: {e}")
        sys.exit(1)


@cli.command()
def status():
    """Show Datus MetricFlow integration status."""
    
    setup_manager = DatusMetricFlowSetup()
    
    click.echo("📊 Datus MetricFlow Integration Status")
    click.echo("=" * 40)
    
    # Check MetricFlow config
    mf_config = setup_manager.mf_config_dir / "config.yml"
    click.echo(f"MetricFlow config: {'✅' if mf_config.exists() else '❌'} {mf_config}")
    
    # Check Datus config
    datus_config = setup_manager.datus_config_dir / "conf" / "agent.yml"
    click.echo(f"Datus Agent config: {'✅' if datus_config.exists() else '❌'} {datus_config}")
    
    # Check semantic models directory
    semantic_dir = setup_manager.mf_config_dir / "semantic_models"
    click.echo(f"Semantic models dir: {'✅' if semantic_dir.exists() else '❌'} {semantic_dir}")
    
    # Check demo database
    demo_db = setup_manager.datus_config_dir / "demo" / "demo.duckdb"
    click.echo(f"Demo database: {'✅' if demo_db.exists() else '❌'} {demo_db}")
    
    # Check environment script
    env_script = setup_manager.mf_config_dir / "datus_env.sh"
    click.echo(f"Environment script: {'✅' if env_script.exists() else '❌'} {env_script}")


if __name__ == '__main__':
    cli()