# Fullon Credentials Service - LLM Guide

## What is fullon_credentials?

A secure credential resolver service for the fullon ecosystem that retrieves API credentials from environment variables (.env files) or Google Secrets Manager. Single purpose: resolve trading exchange API credentials by exchange ID.

## Core Function

**One method does everything:**
```python
from fullon_credentials import fullon_credentials

# Input: exchange ID (integer)
# Output: tuple (secret: str, key: str)
secret, key = fullon_credentials(ex_id=1)
```

## Installation

```bash
# Install from source
pip install git+https://github.com/ingmarAvocado/fullon_credentials.git

# Or with poetry
poetry add git+https://github.com/ingmarAvocado/fullon_credentials.git
```

## Dependencies

- `pydantic>=2.5.0`
- `python-dotenv>=1.0.0`
- `google-cloud-secret-manager>=2.16.0`
- `fullon-orm` (for exchange models)
- `fullon-log` (for logging)

## Setup

### Development (.env file)
Create `.env` file with credentials:
```bash
EX_ID_1_KEY=your_api_key_here
EX_ID_1_SECRET=your_api_secret_here
EX_ID_2_KEY=another_api_key
EX_ID_2_SECRET=another_api_secret
```

### Production (Google Secrets Manager)
Set environment variables:
```bash
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Secrets stored as:
- `fullon-ex-{ex_id}-api-key`
- `fullon-ex-{ex_id}-api-secret`

## Usage Examples

```python
from fullon_credentials import fullon_credentials

# Get credentials for exchange ID 1
try:
    secret, key = fullon_credentials(ex_id=1)
    print(f"Got credentials for exchange 1")
    # Use secret and key for API calls
except ValueError as e:
    print(f"Credentials not found: {e}")

# Use with fullon_orm exchange model
from fullon_orm.models import Exchange

exchange = Exchange.get(1)  # Get exchange with ID 1
secret, key = fullon_credentials(ex_id=exchange.ex_id)
```

## Error Handling

- Raises `ValueError` if credentials not found for given ex_id
- Never exposes credential values in error messages
- Checks .env first, falls back to Google Secrets Manager

## Security Notes

- Credentials are never logged
- Only exchange IDs are logged for audit trails
- Validates credential format before returning
- Secure fallback mechanism between development and production

## Integration with Fullon Ecosystem

This service is used by:
- `fullon_exchange`: Gets API credentials for exchange connections
- Any fullon service requiring external API authentication

Input always comes from `fullon_orm.models.exchange.ex_id` (integer).