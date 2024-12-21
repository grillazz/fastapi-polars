# fastapi-polars

Problems to Solve:

Parquet is a highly useful and popular format for storing data in one place.
However, it has one key limitation:
Extending an existing Parquet file is not straightforward.

Additionally, Parquet files are often stored on S3, which is not highly 
responsive and was not designed for low-latency operations.

So what problem I will going to solver.
- have fast, simple and chep solution to gather and store big volumes fo data.