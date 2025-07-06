import dagster as dg
import duckdb
import filelock
from dagster_duckdb import DuckDBResource

# @dg.asset
# def assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...

def import_url_to_duckdb(url: str, duckdb: DuckDBResource, table_name: str):
    with duckdb.get_connection() as conn:
        row_count = conn.execute(
            f"""
            create or replace table {table_name} as (
                select * from read_csv_auto('{url}')
            )
            """
        ).fetchone()
        assert row_count is not None
        row_count = row_count[0]

@dg.asset(
        kinds={"duckdb"},
        key=["target", "main", "raw_customers"],
)
def raw_customers(duckdb: DuckDBResource) -> None:
    import_url_to_duckdb(
        url="https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv",
        duckdb_path="/tmp/jaffle_platform.duckdb",
        table_name="jaffle_platform.main.raw_customers",
    )

@dg.asset(
        kinds={"duckdb"},
        key=["target", "main", "raw_orders"],
    )
def raw_orders(duckdb: DuckDBResource) -> None:
    import_url_to_duckdb(
        url="https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_orders.csv",
        duckdb_path="/tmp/jaffle_platform.duckdb",
        table_name="jaffle_platform.main.raw_orders",
    )

@dg.asset(
        kinds={"duckdb"},
        key=["target", "main", "raw_payments"],
    )
def raw_payments(duckdb: DuckDBResource) -> None:
    import_url_to_duckdb(
        url="https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_payments.csv",
        duckdb_path="/tmp/jaffle_platform.duckdb",
        table_name="jaffle_platform.main.raw_payments",
    )