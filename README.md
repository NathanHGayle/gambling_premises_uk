# Dataform Branch - Data Transformation

This branch contains the data transformation logic for the project, [return to main branch here](https://github.com/NathanHGayle/gambling_premises_uk/tree/master?tab=readme-ov-file), implemented using **Dataform**. It is responsible for creating, testing, and maintaining the SQL queries that define intermediate and final tables in our data pipeline.

## Project Structure

The following directories are used in this branch:

- **`src/`**: Contains SQL queries for defining source tables and their transformations.
- **`tests/`**: Holds SQL-based test queries to ensure data integrity and validation.
- **`mart/`**: Defines the consumption layer for analytics, including materialized tables.
- **`analysis/`**: Contains analysis queries for reporting and data summarization.
- **`intermediate/` (stg tables)**: Defines staging tables that re-format tables for further processing.

## Data Pipeline Overview

The data pipeline includes several stages:

1. **Staging Tables**: Raw data is ingested and transformed into a structured format.
2. **Intermediate Tables**: Data is cleaned and aggregated to prepare it for analysis.
3. **Mart**: Final tables are created for downstream reporting and analysis.

## Lineage Flow

The data lineage flow diagram provides a visual representation of how data flows through the stages of transformation in the pipeline. 

![Dataform Lineage Diagram](https://github.com/NathanHGayle/gambling_premises_uk/blob/dev_dataform/diagrams/compiled_graph.png)

## Running the Dataform Project

To run the project locally or in the cloud, follow these steps:

1. Install Dataform by following the [Dataform installation guide](https://cloud.google.com/dataform/docs/use-dataform-cli) if you haven't already.
2. Clone this repository and navigate to the Dataform directory.
3. Authenticate with your Google Cloud account.
4. Run the following commands to compile, test, and execute all queries:

    ```bash
    dataform compile
    dataform test
    dataform run
    ```
