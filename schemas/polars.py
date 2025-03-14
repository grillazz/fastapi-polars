import polars as pl

pl_book_schema = pl.Schema({
    "pages": pl.Int64,
    "description": pl.String,
    "isbn": pl.String,
    "author": pl.String
})