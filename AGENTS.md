# AGENTS.md - Coding Guidelines for fullon_ohlcv_service

## Build/Lint/Test Commands
- **Install**: `poetry install --with dev`
- **Run all tests**: `poetry run pytest`
- **Run single test**: `poetry run pytest tests/unit/test_config_settings.py::TestOhlcvServiceConfig::test_default_values`
- **Lint**: `poetry run ruff check . && poetry run mypy src/`
- **Format**: `poetry run black . && poetry run ruff --fix .`
- **All checks**: `make check` (format + lint + test)

## Code Style Guidelines

### Imports
- Standard library first, then third-party, then local modules
- Use explicit `from` imports: `from typing import Optional, Dict`
- Group imports with blank lines between groups

### Formatting & Linting
- **Line length**: 100 characters (Black + Ruff)
- **Quotes**: Double quotes for strings, single for docstrings
- **Trailing commas**: Always use in multi-line structures

### Types
- **Strict typing**: Required for all functions/parameters/returns
- **No implicit Optional**: Use `Optional[T]` or `T | None`
- **Generic types**: Use `list[T]` instead of `List[T]`

### Naming Conventions
- **Functions/variables**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_CASE`
- **Private methods**: `_leading_underscore`

### Error Handling
- Use try/except with specific exception types
- Log errors with context using component loggers
- Graceful degradation over crashing
- Always cleanup resources in finally blocks

### Async Patterns
- Prefer async/await over sync code
- Use context managers for resource management
- Initialize/shutdown patterns for external services

### Documentation
- Docstrings for all public functions/classes
- Clear, concise descriptions
- Type hints serve as documentation

### Testing
- pytest with asyncio auto-detection
- Unit tests in `tests/unit/`, integration in `tests/integration/`
- Factory-boy for test data creation
- 90% coverage minimum

