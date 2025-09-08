# Fullon Log - LLM Integration Guide

## Quick Integration

Add to your CLAUDE.md:

```markdown
## Logging with Fullon Log

Use fullon-log for consistent, beautiful logging across all fullon components.

### Installation
```bash
poetry add fullon-log
# OR from git:
poetry add git+ssh://github.com/ingmarAvocado/fullon_log.git
```

### Basic Usage
```python
from fullon_log import logger, get_component_logger

# Simple logging
logger.info("Application started")
logger.error("Error occurred", user_id=123, error="timeout")

# Component-specific logging (recommended)
trading_logger = get_component_logger("fullon.trading.engine")
trading_logger.info("Trade executed", symbol="BTC/USD", amount=1.5)
```

### Environment Configuration
```bash
# .env file
LOG_LEVEL=INFO                    # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT=beautiful              # beautiful, minimal, development, detailed, trading, json
LOG_CONSOLE=true                  # Enable console output
LOG_COLORS=true                   # Enable colored output
LOG_FILE_PATH=/var/log/app.log    # Optional: log to file
LOG_ROTATION=100 MB               # Optional: file rotation
LOG_RETENTION=7 days              # Optional: retention period
```

### Component Isolation
Each component can have its own log file:
```python
# Configure component-specific file logging
from fullon_log import configure_logger

configure_logger(
    file_path=f"/tmp/fullon_log/{component_name}.log",
    console=True
)
```
```

## Repository Details

- **GitHub**: https://github.com/ingmarAvocado/fullon_log
- **Version**: 1.0.0 (production-ready)
- **Dependencies**: Only loguru (minimal overhead)
- **Performance**: 10,000+ logs/second, ~0.1ms latency
- **Test Coverage**: 100% with 79 passing tests

## Key Features for LLMs

1. **Zero Configuration**: Works out of the box with beautiful defaults
2. **Environment-Based Setup**: Configure via .env variables only
3. **Component Isolation**: Each fullon component gets its own logger
4. **Production Ready**: Battle-tested loguru wrapper with performance validation
5. **Thread-Safe**: Built for async/concurrent applications

## Usage Patterns

### Standard Pattern for Any Fullon Component
```python
from fullon_log import get_component_logger

class YourComponent:
    def __init__(self):
        self.logger = get_component_logger(f"fullon.{__name__}")
    
    def process(self):
        self.logger.info("Processing started", batch_id=123)
        try:
            # Your code here
            self.logger.info("Processing completed", records=456)
        except Exception as e:
            self.logger.error("Processing failed", error=str(e))
```

### File Separation by Component
```python
# In your component's initialization
from fullon_log import configure_logger

# Each component can have its own log file
configure_logger(
    file_path=f"/var/log/fullon/{self.__class__.__name__.lower()}.log",
    console=True,  # Also log to console
    level="INFO"
)
```

## Integration Checklist

When integrating fullon-log into any fullon component:

- [ ] Add `poetry add fullon-log` to dependencies
- [ ] Import: `from fullon_log import get_component_logger`
- [ ] Create logger: `self.logger = get_component_logger("fullon.component.name")`
- [ ] Use structured logging: `logger.info("message", key=value)`
- [ ] Configure via .env if needed
- [ ] Optional: Set component-specific log file

## Performance Notes

- **Minimal Overhead**: ~91μs per log entry
- **Async-Friendly**: Uses `enqueue=True` internally
- **Memory Efficient**: Automatic cleanup and rotation
- **High Throughput**: Validated at 10,000+ logs/second

## Common Configurations

### Development
```bash
LOG_LEVEL=DEBUG
LOG_FORMAT=development
LOG_COLORS=true
LOG_CONSOLE=true
```

### Production
```bash
LOG_LEVEL=INFO
LOG_FORMAT=json
LOG_CONSOLE=false
LOG_FILE_PATH=/var/log/fullon/app.log
LOG_ROTATION=100 MB
LOG_RETENTION=30 days
```

### Trading Components
```bash
LOG_LEVEL=INFO
LOG_FORMAT=trading
LOG_COLORS=true
LOG_CONSOLE=true
LOG_FILE_PATH=/var/log/fullon/trading.log
```

This library is a simple, fast wrapper around loguru designed specifically for the fullon ecosystem. Use it consistently across all fullon components for unified logging.
