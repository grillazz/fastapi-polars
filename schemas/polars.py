import polars as pl

pl_iced_schema = pl.Schema({"ingest": pl.Int64, "saffire": pl.String, "isbn": pl.String, "pid": pl.String, })

pl_frosted_schema = pl.Schema({"ingest": pl.Int64, "beryl": pl.String})
