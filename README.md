# fullon_ohlcv_service

✅ **Foundation Complete**: Async daemon for OHLCV and trade data collection using the fullon ecosystem.

## Overview

A lightweight integration service that coordinates data collection from cryptocurrency exchanges using:
- **fullon_exchange**: WebSocket and REST API data collection
- **fullon_ohlcv**: Database storage via repositories
- **fullon_orm**: Database-driven exchange/symbol configuration
- **fullon_cache**: Health monitoring and real-time updates
- **fullon_log**: Structured component logging

## Installation

```bash
# Clone the repository
git clone https://github.com/ingmarAvocado/fullon_ohlcv_service.git
cd fullon_ohlcv_service

# Install dependencies
poetry install
```

## Usage

### Run as Service

```bash
# Start the OHLCV/Trade collection daemon
poetry run python -m fullon_ohlcv_service.daemon

# The service will:
# 1. Read exchange/symbol configuration from fullon_orm database
# 2. Start OHLCV collectors for configured symbols
# 3. Start trade streaming for enabled symbols
# 4. Monitor health via ProcessCache
# 5. Handle graceful shutdown on SIGTERM/SIGINT
```

### Run Examples

```bash
# Run the example pipeline demonstrating all features
poetry run python examples/run_example_pipeline.py
```

## Configuration

The service uses database-driven configuration via fullon_orm:

```python
# Configuration is read from the database
# No hardcoded exchange or symbol lists
# Set up exchanges and symbols via fullon_orm admin interface
```

### Environment Variables

```bash
# Optional: Set logging level
LOG_LEVEL=INFO  # or DEBUG, WARNING, ERROR
```

## Architecture

The service consists of:

- **OhlcvCollector**: REST-based OHLCV data collection
- **TradeCollector**: WebSocket-based real-time trade streaming
- **OhlcvManager**: Coordinates multiple OHLCV collectors
- **DatabaseConfig**: Fetches configuration from fullon_orm
- **Daemon**: Main service orchestration

## Development

See [CLAUDE.md](CLAUDE.md) for detailed development guidelines and architecture documentation.