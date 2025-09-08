# fullon_ohlcv_service

Async daemon service for collecting OHLCV candles and individual trade data from cryptocurrency exchanges.

## Architecture

- **ohlcv/**: 1-minute OHLCV candle collection
- **trade/**: Individual trade data collection  
- **daemon.py**: Main service launcher
- **config.py**: Environment configuration

## Dependencies

- fullon_log: Structured logging
- fullon_exchange: Unified exchange API
- fullon_ohlcv: Database storage
- fullon_cache: Redis coordination
- fullon_orm: Database models

## Setup

```bash
poetry install
cp .env.example .env  # Configure your environment
```

## Usage

```bash
# Run OHLCV service
poetry run python -m fullon_ohlcv_service.daemon --service ohlcv

# Run trade service  
poetry run python -m fullon_ohlcv_service.daemon --service trade

# Run both services
poetry run python -m fullon_ohlcv_service.daemon --service both
```

See `CLAUDE.md` for detailed LLM development guide.