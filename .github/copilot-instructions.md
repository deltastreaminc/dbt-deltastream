# dbt deltastream
# Project Context
dbt Adapter for DeltaStream which is a streaming processing engine based on Flink.
# Code Style and Structure
- Write concise, technical Python, SQL or Jinja code with accurate examples
- Use functional and declarative programming patterns; avoid classes
- Prefer iteration and modularization over code duplication
- Use descriptive variable names with auxiliary verbs (e.g., isLoading, hasError)
- Structure repository files as follows:
.
├── src
│   └── dbt
│       ├── adapters
│       │   └── deltastream # Python part of the dbt adapter for DeltaStream
│       └── include
│           └── deltastream # Jinja templates for dbt macros and adapters
└── tests # Unit tests
    └── adapter # Unit tests for the adapter

# Build and project setup
The project is using `uv` for dependency management. You can find the lockfile in `uv.lock`.
To run tests, use `uv run pytest <path_to_test>`.
To add dependencies use `uv add <package>`.
Dependency resolution is done using `uv sync`.
Dependencies are specified in `pyproject.toml`.
Dependencies are installed in `./.venv`.