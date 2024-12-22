# Makefile

# Run FastAPI with uvicorn
run:
	uvicorn run_polars:app --host 0.0.0.0 --port 8000 --reload


run-polars:
	granian --interface asgi run_polars:app --host 0.0.0.0 --port 8000 --log-level debug --workers 4 --threads 1