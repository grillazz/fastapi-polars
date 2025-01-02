# fastapi-polars

Problems to Solve:

Parquet is a highly useful and popular format for storing data in one place.
However, it has one key limitation:
Extending an existing Parquet file is not straightforward.

Additionally, Parquet files are often stored on S3, which is not highly 
responsive and was not designed for low-latency operations.

So what problem I will going to solver.
- have fast, simple and chep solution to gather and store big volumes fo data.
- ursa: big data framework to ingest, validate and harmonize data > only input protocol is rest
- by adding polars dataframe as in-memory sink - framework is delivering no latency data 
  storage based on parquet files which is transported to s3 as persistence layer in scheduled or adhoc steps
- main goal was simplicity and low cost of usage

on s3 we use merge to merge files for same dataframe for same day from different k8s nodes with gateway
or maybe better is to open then in one polars operation / sort / write to new parquet ?