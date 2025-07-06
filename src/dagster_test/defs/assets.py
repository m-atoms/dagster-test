import dagster as dg
import duckdb
import filelock

# @dg.asset
# def assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...

def serialize_duckdb_query(duckdb_path: str, sql: str):
    """Execute SQL statement with file lock to guarantee cross-process concurrency."""
    lock_path = f"{duckdb_path}.lock"
    with filelock.FileLock(lock_path):
        conn = duckdb.connect(duckdb_path)
        try:
            return conn.execute(sql)
        finally:
            conn.close()

def import_url_to_duckdb(url: str, duckdb_path: str, table_name: str):
    create_query = f"""
        create or replace table {table_name} as (
            select * from read_csv_auto('{url}')
        )
    """

    serialize_duckdb_query(duckdb_path, create_query)

@dg.asset(kinds={"duckdb"}, key=["target", "main", "raw_customers"])
def raw_customers() -> None:
    import_url_to_duckdb(
        url="https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv",
        duckdb_path="/tmp/jaffle_platform.duckdb",
        table_name="jaffle_platform.main.raw_customers",
    )


@dg.asset(kinds={"duckdb"}, key=["target", "main", "raw_orders"])
def raw_orders() -> None:
    import_url_to_duckdb(
        url="https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_orders.csv",
        duckdb_path="/tmp/jaffle_platform.duckdb",
        table_name="jaffle_platform.main.raw_orders",
    )


@dg.asset(kinds={"duckdb"}, key=["target", "main", "raw_payments"])
def raw_payments() -> None:
    import_url_to_duckdb(
        url="https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_payments.csv",
        duckdb_path="/tmp/jaffle_platform.duckdb",
        table_name="jaffle_platform.main.raw_payments",
    )