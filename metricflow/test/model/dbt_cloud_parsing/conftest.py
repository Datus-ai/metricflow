import importlib

# Skip this entire test directory when dbt_metadata_client is not installed
# (dependency is disabled for Python 3.12 compatibility, see pyproject.toml)
if importlib.util.find_spec("dbt_metadata_client") is None:
    collect_ignore_glob = ["test_*.py"]
else:
    from metricflow.test.fixtures.dbt_metadata_fixtures import *  # noqa: F401, F403
