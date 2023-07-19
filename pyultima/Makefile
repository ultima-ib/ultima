.DEFAULT_GOAL := help

SHELL=/bin/bash
VENV = venv
PY39 = C:/Python39/python
PY311 = C:/Python311/python

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

venv:  ## Set up virtual environment
	python3 -m venv $(VENV)
	$(VENV_BIN)/python -m pip install --upgrade pip
	$(VENV_BIN)/pip install -r requirements.txt
	$(VENV_BIN)/pip install -r requirements-lint.txt

.PHONY: develop
develop: venv  ## Compile and install Polars for development
	@unset CONDA_PREFIX && source $(VENV_BIN)/activate && maturin develop

.PHONY: build
build: venv  ## Build whl
	@unset CONDA_PREFIX && source $(VENV_BIN)/activate && maturin build -i=$(PY311) --release

.PHONY: develop-release
build-release: venv  ## Compile and install a faster Polars binary
	@unset CONDA_PREFIX && source $(VENV_BIN)/activate && maturin develop --release

.PHONY: fmt
fmt: venv  ## Run autoformatting and linting
	$(VENV_BIN)/isort .
	$(VENV_BIN)/black .
	$(VENV_BIN)/blackdoc .
	$(VENV_BIN)/pyupgrade --py37-plus
	cargo fmt --all
	$(VENV_BIN)/mypy .
	$(VENV_BIN)/ruff .
	$(VENV_BIN)/python tests/docs/run_doc_examples.py

.PHONY: clippy
clippy:  ## Run clippy
	cargo clippy -- -D warnings -A clippy::unnecessary-lazy-evaluations

.PHONY: pre-commit
pre-commit: fmt clippy  ## Run all code quality checks

.PHONY: test
test: venv develop  ## Run fast unittests
	$(VENV_BIN)/pytest tests/unit/

.PHONY: doctest
doctest: venv develop  ## Run doctests
	$(VENV_BIN)/python tests/docs/run_doc_examples.py

.PHONY: test-all
test-all: venv develop  ## Run all tests
	$(VENV_BIN)/pytest

.PHONY: coverage
coverage: venv develop  ## Run tests and report coverage
	$(VENV_BIN)/pytest --cov

.PHONY: publish
publish: venv
	maturin publish -r=testpypi -i=$(PY39) --skip-existing

.PHONY: clean
clean:  ## Clean up caches and build artifacts
	@rm -rf venv/
	@rm -rf target/
	@rm -rf docs/build/
	@rm -rf docs/source/reference/api/
	@rm -rf .hypothesis/
	@rm -rf .mypy_cache/
	@rm -rf .pytest_cache/
	@rm -rf .ruff_cache/
	@rm -f .coverage
	@rm -f coverage.xml
	@rm -f polars/polars.abi3.so
	@find . -type f -name '*.py[co]' -delete -or -type d -name __pycache__ -delete
	@cargo clean

.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m\n"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
