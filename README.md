# dagster-test

This project is a Dagster-based ETL pipeline example. It demonstrates how to define assets, use DuckDB for data ingestion, and structure your Dagster project for best practices.

## Project Structure

```
src/
  dagster_test/
    defs/
      assets.py
    definitions.py
```

## Validating Your Dagster Definitions

As you develop your Dagster project, it is a good habit to run the following command to ensure everything is working as expected:

```bash
dg check defs
```

This command will validate that all components and definitions in your project are loading correctly. You should see output like:

```
All components validated successfully.
All definitions loaded successfully.
```

For more information, see the [Dagster ETL pipeline tutorial](https://docs.dagster.io/etl-pipeline-tutorial/extract-data). 