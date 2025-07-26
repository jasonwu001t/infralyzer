# CUR 2.0 Query Library

**Most queries in this library are adapted from the [AWS Well-Architected Cost & Usage Report Query Library](https://catalog.workshops.aws/cur-query-library/en-US/queries).**

This directory contains a collection of Polars-compatible SQL queries for AWS Cost & Usage Report (CUR) analytics, organized by category. Queries are designed for use with the de-polars toolkit and can be used for partitioned analytics table generation, cost optimization, and reporting.

---

## Structure

- Each subfolder corresponds to a CUR analytics category (e.g., `cost_analysis`, `application_integration`, `aws_cost_management`, `compute`, `container`, `cost_efficiency`, `cost_optimization`, `database`, etc.)
- Each `.sql` file contains a single, self-contained query with a header describing its purpose and source.

## Usage

- Use these queries with the `data_partitioner.py` or `client.py` modules in this repo.
- Queries are written for Polars SQL compatibility and may require minor adjustments for other engines.

## Attribution

- **Source:** Most queries are adapted from the [AWS Well-Architected Cost & Usage Report Query Library](https://catalog.workshops.aws/cur-query-library/en-US/queries).
- Some queries have been simplified or modified for Polars compatibility and partitioned analytics workflows.

## Contributing

- To add new queries, create a new `.sql` file in the appropriate category folder and include a header with a description and source.
- Please test queries for Polars compatibility before submitting.
