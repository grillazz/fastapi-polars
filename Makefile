# Makefile

# Run FastAPI with uvicorn
run:
	uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2


run-polars:
	granian --interface asgi run_polars:app --host 0.0.0.0 --port 8000 --log-level debug --workers 4 --threads 1