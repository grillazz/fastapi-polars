import pyarrow as pa

import pyarrow.parquet as pq

import polars as pl

from whenever import Instant, ZonedDateTime

df = pl.DataFrame(
    {
        "a": [1, 2, 3],
        "b": ["x", "y", "z"],
        "c": [True, False, True],
    }
)


table = df.to_arrow()
pqwriter = pq.ParquetWriter('sample.parquet', table.schema)
# in loop
pqwriter.write_table(table)

pqwriter.close()

if __name__ == "__main__":
    # Run the test
    _day = Instant.now().py_datetime().date()
    print(_day)
    pass

# save df to s3 will drop sample.parquet file ?