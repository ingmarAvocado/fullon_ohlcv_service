# AGENTS Playbook

Guidelines for AI coding agents contributing to `fullon_ohlcv_service`. Keep changes small, integration‑focused, and aligned with the fullon ecosystem.

## Read First
- `CLAUDE.md`: Core mission, LRRS architecture, usage patterns
- `docs/CLAUDE_PROJECT_GUIDE.md`: Natural conversation + repo creation approach
- `README.md`: Scope and usage
- `fix_this_fuck.md`: Corrected scope and priorities
- `git_plan.md`, `git_issues_rules.md`: Workflow + completion criteria
- `docs/*`: LLM guides and method references for fullon libraries

## Core Principles (LRRS)
- Little: ~300–500 LOC of integration glue, not a framework
- Responsible: Single responsibility per component; follow proven patterns
- Reusable: Clean interfaces; avoid tight coupling
- Separate: Use fullon libraries; do not reimplement them

## Project Scope
- Do: Coordinate collectors, read config from DB, store OHLCV/trades, report health
- Don’t: Write direct SQL, build exchange clients, design recovery systems, hardcode config

## Use These Libraries (Mandatory)
- `fullon_exchange`: All exchange IO (REST/WebSocket, reconnection, rate limits)
- `fullon_ohlcv`: Repositories for saving/querying OHLCV and trades
- `fullon_orm`: Database‑driven exchange/symbol configuration
- `fullon_cache`: Health status and real‑time cache updates
- `fullon_log`: Structured component logging

If a needed library is missing from `pyproject.toml`, add it explicitly.

## Minimal Architecture
```
src/fullon_ohlcv_service/
├── daemon.py          # entry; coordinates based on DB config
├── config.py          # simple env config
├── ohlcv/
│   ├── collector.py   # exchange + ohlcv repo integration
│   └── manager.py     # multi‑symbol coordination
└── trade/
    ├── collector.py   # exchange trade stream + repo
    └── manager.py     # multi‑symbol coordination
```

## Canonical Patterns
- Async always; prefer context managers
- Initialize and shutdown `ExchangeQueue` around usage
- Use repositories for persistence; no raw SQL
- Component loggers for traceability
- Health via `ProcessCache`
- UTC timestamps and typed models

### OHLCV (historical)
```python
from fullon_exchange.queue import ExchangeQueue
from fullon_ohlcv.repositories.ohlcv import CandleRepository

await ExchangeQueue.initialize_factory()
try:
    handler = await ExchangeQueue.get_rest_handler("kraken")
    candles = await handler.get_ohlcv("BTC/USD", "1m", limit=100)
    async with CandleRepository("kraken", "BTC/USD", test=False) as repo:
        await repo.save_candles(candles)
finally:
    await ExchangeQueue.shutdown_factory()
```

### OHLCV (streaming)
```python
handler = await ExchangeQueue.get_websocket_handler("kraken")
await handler.connect()

async def on_ohlcv(ohlcv):
    async with CandleRepository("kraken", "BTC/USD", test=False) as repo:
        await repo.save_candles([ohlcv])

await handler.subscribe_ohlcv("BTC/USD", on_ohlcv, interval="1m")
```

### Config from DB
```python
from fullon_orm.database_context import DatabaseContext

async with DatabaseContext() as db:
    exchanges = await db.exchanges.get_user_exchanges(user_id=1)
    # build {exchange: [symbols]} from DB; no hardcoding
```

### Health + Logging
```python
from fullon_cache import ProcessCache
from fullon_log import get_component_logger

logger = get_component_logger("fullon.ohlcv.daemon")
async with ProcessCache() as cache:
    await cache.update_process("ohlcv_service", "daemon", "running")
```

## Workflow (Issues → Examples → Code)
- Plan: Create/update an issue with clear steps and acceptance tied to an example
- Branch: `git checkout -b feature/<issue-brief>`
- Examples‑first: Add/adjust an example in `examples/` proving the change
- Implement: Minimal code to satisfy the example and LRRS constraints
- Tests: Add/adjust tests as needed; keep them fast and focused
- Validate: See checklist below; iterate until green
- PR: Reference issue, summarize example coverage; merge once all checks pass

## Validation Checklist (Non‑Negotiable)
- Examples: `python run_example_pipeline.py` passes
- Tests: `poetry run pytest` green (async + DB/Redis isolation patterns)
- Style: `ruff`, `black`, `mypy` clean (see `pyproject.toml`)
- Scope: No reinvention; uses fullon libs; no direct SQL or hardcoded config
- Docs: Updated README/CLAUDE/AGENTS snippets when behavior changes

## Common Pitfalls (Avoid)
- Direct exchange SDK usage (must go through `ExchangeQueue`)
- Raw SQL or ad‑hoc schema work (use repositories)
- Hardcoded exchanges/symbols (read from `fullon_orm`)
- Skipping `ExchangeQueue.shutdown_factory()` in finally blocks
- Synchronous or blocking code in async flows

## Environment and Commands
- Install: `poetry install` (add `--with dev` for tooling)
- Run daemon: `poetry run python -m fullon_ohlcv_service.daemon`
- Lint/format: `ruff check . && black .`
- Types: `mypy`
- Tests: `poetry run pytest`
- Examples: `python run_example_pipeline.py`

## What to Implement First (Foundation)
Follow the corrected order from `fix_this_fuck.md` and `git_plan.md`:
1) Basic OHLCV collector
2) Basic Trade collector
3) DB‑driven configuration
4) Simple daemon coordination
5) Health monitoring via `ProcessCache`

Advanced features are out‑of‑scope until the foundation is solid and validated by examples.

## When in Doubt
- Start simpler than you think; keep diffs small
- Reuse patterns from CLAUDE.md and docs examples
- Prefer improving examples over expanding code
- Align with ticker service patterns across fullon repos

