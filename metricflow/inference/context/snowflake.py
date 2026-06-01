import json
from metricflow.dataflow.sql_column import SqlColumn
from metricflow.dataflow.sql_table import SqlTable
from metricflow.inference.context.data_warehouse import (
    TableProperties,
    ColumnProperties,
    InferenceColumnType,
    DataWarehouseInferenceContextProvider,
)


class SnowflakeInferenceContextProvider(DataWarehouseInferenceContextProvider):
    """The snowflake implementation for a DataWarehouseInferenceContextProvider"""

    COUNT_DISTINCT_SUFFIX = "countdistinct"
    COUNT_NULL_SUFFIX = "countnull"
    MIN_SUFFIX = "min"
    MAX_SUFFIX = "max"

    def _column_type_from_show_columns_data_type(self, type_str: str) -> InferenceColumnType:
        """Get the correspondent InferenceColumnType from Snowflake's returned type string.

        See for reference: https://docs.snowflake.com/en/sql-reference/sql/show-columns.html
        For string types: https://docs.snowflake.com/en/sql-reference/data-types-text.html#data-types-for-text-strings
        """
        type_str = type_str.upper()
        type_mapping = {
            "FIXED": InferenceColumnType.INTEGER,
            "REAL": InferenceColumnType.FLOAT,
            "BOOLEAN": InferenceColumnType.BOOLEAN,
            "DATE": InferenceColumnType.DATETIME,
            "TIMESTAMP_TZ": InferenceColumnType.DATETIME,
            "TIMESTAMP_LTZ": InferenceColumnType.DATETIME,
            "TIMESTAMP_NTZ": InferenceColumnType.DATETIME,
        }
        string_prefixes = {
            "VARCHAR",
            "CHAR",
            "CHARACTER",
            "NCHAR",
            "STRING",
            "TEXT",
            "NVARCHAR",
            "NVARCHAR2",
            "CHAR VARYING",
            "NCHAR VARYING",
        }

        if type_str in type_mapping:
            return type_mapping[type_str]

        # This might be a string type, which can either be something like "TEXT" or "VARCHAR(256)"
        for prefix in string_prefixes:
            if type_str.startswith(prefix):
                return InferenceColumnType.STRING

        return InferenceColumnType.UNKNOWN

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        escaped_identifier = identifier.replace('"', '""')
        return f'"{escaped_identifier}"'

    def _get_select_list_for_column_name(self, name: str, count_nulls: bool, alias_prefix: str = "") -> str:
        column_reference = self._quote_identifier(name)
        alias_prefix = alias_prefix or name
        count_distinct_alias = self._quote_identifier(
            f"{alias_prefix}_{SnowflakeInferenceContextProvider.COUNT_DISTINCT_SUFFIX}"
        )
        min_alias = self._quote_identifier(f"{alias_prefix}_{SnowflakeInferenceContextProvider.MIN_SUFFIX}")
        max_alias = self._quote_identifier(f"{alias_prefix}_{SnowflakeInferenceContextProvider.MAX_SUFFIX}")
        count_null_alias = self._quote_identifier(
            f"{alias_prefix}_{SnowflakeInferenceContextProvider.COUNT_NULL_SUFFIX}"
        )
        statements = [
            f"COUNT(DISTINCT {column_reference}) AS {count_distinct_alias}",
            f"MIN({column_reference}) AS {min_alias}",
            f"MAX({column_reference}) AS {max_alias}",
            (
                f"SUM(CASE WHEN {column_reference} IS NULL THEN 1 ELSE 0 END) AS {count_null_alias}"
                if count_nulls
                else f"0 AS {count_null_alias}"
            ),
        ]

        return ", ".join(statements)

    def _get_table_properties(self, table: SqlTable) -> TableProperties:
        all_columns_query = f"SHOW COLUMNS IN TABLE {table.sql}"
        all_columns = self._client.query(all_columns_query)

        sql_column_list = []
        col_types = {}
        col_nullable = {}
        select_lists = []

        for row in all_columns.itertuples():
            column_name = row.column_name
            column = SqlColumn.from_names(
                db_name=row.database_name.lower(),
                schema_name=row.schema_name.lower(),
                table_name=row.table_name.lower(),
                column_name=column_name.lower(),
            )
            sql_column_list.append(column)

            type_dict = json.loads(row.data_type)
            col_types[column] = self._column_type_from_show_columns_data_type(type_dict["type"])
            col_nullable[column] = type_dict["nullable"]
            select_lists.append(
                self._get_select_list_for_column_name(
                    name=column_name,
                    count_nulls=col_nullable[column],
                    alias_prefix=column.column_name,
                )
            )

        select_lists.append("COUNT(*) AS rowcount")
        select_list = ", ".join(select_lists)
        statistics_query = f"SELECT {select_list} FROM {table.sql} SAMPLE ({self.max_sample_size} ROWS)"
        statistics_df = self._client.query(statistics_query)

        column_props = [
            ColumnProperties(
                column=column,
                type=col_types[column],
                is_nullable=col_nullable[column],
                null_count=statistics_df[f"{column.column_name}_{SnowflakeInferenceContextProvider.COUNT_NULL_SUFFIX}"][
                    0
                ],
                row_count=statistics_df["rowcount"][0],
                distinct_row_count=statistics_df[
                    f"{column.column_name}_{SnowflakeInferenceContextProvider.COUNT_DISTINCT_SUFFIX}"
                ][0],
                min_value=statistics_df[f"{column.column_name}_{SnowflakeInferenceContextProvider.MIN_SUFFIX}"][0],
                max_value=statistics_df[f"{column.column_name}_{SnowflakeInferenceContextProvider.MAX_SUFFIX}"][0],
            )
            for column in sql_column_list
        ]

        return TableProperties(table=table, column_props=column_props)
