# Makefile

# Run FastAPI with uvicorn
run-uvicorn:
	uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload --loop uvloop --http httptools --log-level debug


run-granian:
	uv run granian --interface asgi main:app --host 0.0.0.0 --port 8000 --log-level debug --workers 4