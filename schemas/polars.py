import polars as pl

pl_iced_schema = pl.Schema({"ingest": pl.Int64, "saffire": pl.String})

pl_frosted_schema = pl.Schema({"ingest": pl.Int64, "beryl": pl.String})


# TODO: iterate over dir(polars) check if element is instance of polars.Schema and import it dynamically with importlib
# TODO: build this as service which will be returning list of polars schemas to create dataframe dynamically
# import polars as pl
# import importlib
#
# # Iterate over all elements in the `polars` module
# for element_name in dir(pl):
#     # Get the actual element (attribute or object) from the module
#     element = getattr(pl, element_name)
#
#     # Check if the element is an instance of `pl.Schema`
#     if isinstance(element, pl.Schema):
#         # Dynamically import the module or use the element
#         imported_element = importlib.import_module(f"polars.{element_name}")
#         print(f"Imported: {imported_element}")