"""File containing constants used by Configurations."""

CONFIG_PATH_KEY = "MF_CONFIG_DIR"
CONFIG_DWH_DIALECT = "dwh_dialect"
CONFIG_DWH_HOST = "dwh_host"
CONFIG_DWH_PORT = "dwh_port"
CONFIG_DWH_USER = "dwh_user"
CONFIG_DWH_PASSWORD = "dwh_password"
CONFIG_DWH_DB = "dwh_database"
CONFIG_DWH_SCHEMA = "dwh_schema"
CONFIG_DWH_CREDS_PATH = "dwh_path_to_creds"
CONFIG_DWH_WAREHOUSE = "dwh_warehouse"
CONFIG_DWH_PROJECT_ID = "dwh_project_id"
CONFIG_EMAIL = "email"
CONFIG_MODEL_PATH = "model_path"
CONFIG_DWH_HTTP_PATH = "dwh_http_path"
CONFIG_DWH_ACCESS_TOKEN = "dwh_access_token"
CONFIG_DBT_REPO = "dbt_repo"
CONFIG_DBT_PROFILE = "dbt_profile"
CONFIG_DBT_TARGET = "dbt_target"
CONFIG_DBT_CLOUD_JOB_ID = "dbt_cloud_job_id"
CONFIG_DBT_CLOUD_SERVICE_TOKEN = "dbt_cloud_service_token"

# ENV constants
ENV_MF_DICT = {
    CONFIG_DWH_DIALECT: "MF_DWH_DIALECT",
    CONFIG_DWH_HOST: "MF_DWH_HOST",
    CONFIG_DWH_PORT: "MF_DWH_PORT",
    CONFIG_DWH_USER: "MF_DWH_USER",
    CONFIG_DWH_PASSWORD: "MF_DWH_PASSWORD",
    CONFIG_DWH_DB: "MF_DWH_DB",
    CONFIG_DWH_SCHEMA: "MF_DWH_SCHEMA",
    CONFIG_DWH_WAREHOUSE: "MF_DWH_WAREHOUSE",
    CONFIG_MODEL_PATH: "MF_MODEL_PATH",
}