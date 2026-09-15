.PHONY: lint test

lint:
	uv lock --check
	uv run ruff format --check .
	uv run ruff check .
	uv run ty check

test:
	uv run pytest -q
