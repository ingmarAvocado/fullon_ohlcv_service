# fullon_ohlcv_service

Simple async daemon for OHLCV/trade collection using fullon ecosystem integration.

## What It Is

~300-500 lines of integration code that coordinates:
- fullon_exchange: WebSocket data collection
- fullon_ohlcv: Database storage via repositories  
- fullon_orm: Database-driven exchange/symbol configuration
- fullon_cache: Health monitoring

## What It's NOT

- A data collection framework
- Exchange API wrapper (use fullon_exchange)
- Complex error recovery system (fullon_exchange handles this)
- Configuration management system (reads from fullon_orm database)

## Current Status

**Foundation issues missing (#1-10)**. Advanced issues (#11-20) should be closed until basics work.

## Usage

```bash
poetry install
poetry run python -m fullon_ohlcv_service.daemon
```

Reads exchanges/symbols from fullon_orm database automatically.

See `fix_this_fuck.md` for planning corrections and `CLAUDE.md` for simplified development guide.