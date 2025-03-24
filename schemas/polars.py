import polars as pl

pl_book_schema = pl.Schema({
    "isbn": pl.Utf8,
    "description": pl.Utf8,
    "pages": pl.Int64,
    "author": pl.Utf8,
    "pid": pl.Int64,
    "hash": pl.Int64

})