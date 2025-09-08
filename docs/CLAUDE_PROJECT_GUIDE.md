# 🚀 Claude Project Creator - Natural Conversation Guide

## What This Is

A simple, conversation-driven approach for Claude to help users create well-architected multi-repository projects. No complex wizards, just natural dialogue that leads to professional GitHub repositories.

## The Natural Flow

### 1. Start Simple
```
User: "I want to build a trading bot"

Claude: "Cool! TacoTrader sounds awesome. Tell me more about what you want this trading bot to do. What exchanges, what strategies, any specific features you have in mind?"
```

### 2. Understand First  
Ask clarifying questions naturally:
- What problem does this solve?
- Who will use it?
- What are the main features you need?
- Do you have any existing libraries/code you want to use?

### 3. Think Architecture Out Loud
```
Claude: "Ok, based on what you've told me, here's how I'm thinking about the architecture:

For TacoTrader, I see these main components:
- **Exchange Connector** - talks to Binance/etc, handles API rate limits
- **Market Data** - fetches and stores OHLCV data, real-time feeds  
- **Strategy Engine** - your trading logic, backtesting, signal generation
- **Order Manager** - handles order placement, tracking, risk management
- **Portfolio Tracker** - positions, P&L, account balances

Each would be its own focused library that does ONE thing really well.

Does this breakdown make sense to you? Would you change anything?"
```

### 4. Refine Together
Let them guide the conversation:
- "Actually, I already have a portfolio tracker I built..."
- "Can we split the strategy engine into backtester and live trading?"
- "I need something for notifications too"

### 5. Check for Existing Libraries
```
Claude: "Great! So you already have a portfolio tracker and notification system. Can you share those repos or tell me about them so I know how to integrate?"

User: "Here's my portfolio tracker: github.com/user/portfolio-lib and I use Slack for notifications"

Claude: "Perfect! I'll design the other libraries to work with your existing portfolio tracker."
```

### 6. Create the Repos
```
Claude: "Alright, I'm going to create these repositories for you:

**Main Project:** TacoTrader 
**New Libraries:**
- tacotrader-exchange (connects to exchanges, handles rate limits)
- tacotrader-market-data (OHLCV fetching and storage)  
- tacotrader-strategy-engine (signal generation, backtesting)
- tacotrader-order-manager (order placement and tracking)

Each repo will have:
- Professional README and documentation
- Complete test structure  
- Working examples
- CI/CD setup
- CLAUDE.md guide for other LLMs to continue development

**Development Order:**
Since these are 100% isolated libraries, you can build them in parallel or tackle them in this order:
1. Start with market-data (foundational, no dependencies)
2. Build exchange connector (uses market-data)
3. Strategy engine (uses market-data) 
4. Order manager (uses exchange + your portfolio tracker)

Ready? Let me create these repos now!"
```

### 7. Execute and Report
Create actual GitHub repositories with proper structure, then give clear next steps.

## Key Principles

### Natural Conversation
- Talk like a helpful architect friend, not a form
- Ask follow-up questions based on their answers
- Think out loud about trade-offs
- Let them guide and correct your thinking

### LRRS Architecture  
- **Little**: Each library does ONE thing
- **Responsible**: Clear, single responsibility  
- **Reusable**: Can work in other projects
- **Separate**: No tight coupling, clean interfaces

### Practical Focus
- Create actual GitHub repositories
- Include working examples and tests
- Provide clear development instructions
- Give concrete next steps

### Parallel Development Ready
**Critical Point**: Since libraries follow LRRS principles, they are 100% isolated and can be developed in parallel. Multiple developers (or LLMs) can work on different libraries simultaneously without conflicts.

## Repository Structure (Generated)

Each library gets:

```
library-name/
├── README.md                 # Professional documentation
├── CLAUDE.md                 # Quick start guide for LLMs  
├── pyproject.toml           # Modern Python packaging
├── Makefile                 # Development commands
├── src/library_name/        # Source code
│   ├── __init__.py
│   ├── core.py              # Main implementation
│   ├── types.py             # Data models
│   └── examples/            # Working examples
│       └── basic_usage.py
├── tests/                   # Comprehensive tests
│   ├── unit/
│   ├── integration/
│   └── conftest.py
├── .github/workflows/       # CI/CD
│   └── test.yml
└── docs/                    # Documentation
```

## Templates Available

The `templates/` directory contains Jinja2 templates for generating:
- `README.md.j2` - Professional README
- `LLM_GUIDE.md.j2` - Quick start for LLMs
- `basic_usage.py.j2` - Working examples
- `conftest.py.j2` - Test configuration

## Implementation Script

Use the templates to generate repositories using a simple bash script or Python script that:
1. Creates the directory structure
2. Renders templates with project-specific values
3. Creates GitHub repositories using `gh` CLI
4. Pushes initial structure

## Example Usage

```bash
# After conversation, Claude runs:
./create_project.sh "TacoTrader" "trading bot" "tacotrader-exchange,tacotrader-market-data,tacotrader-strategy,tacotrader-orders"

# This creates:
# - 4 GitHub repositories
# - Complete project structure
# - Working examples and tests
# - CI/CD pipelines
# - Development documentation
```

## Success Metrics

A good project has:
- Clear conversation that understood the user's needs
- Libraries with single, obvious purposes  
- No circular dependencies
- Working examples in each library
- Other LLMs can pick up development easily using CLAUDE.md
- User knows exactly what to build next

## Next Steps After Creation

```
Claude: "Done! I've created 4 repositories for TacoTrader:

🔗 **Repositories Created:**
- github.com/user/tacotrader-exchange
- github.com/user/tacotrader-market-data  
- github.com/user/tacotrader-strategy
- github.com/user/tacotrader-orders

📋 **Development Order:**
Build these in order (or in parallel since they're isolated):
1. market-data (no dependencies)
2. exchange (uses market-data)
3. strategy (uses market-data)
4. orders (uses exchange + your portfolio tracker)

🚀 **Next Steps:**
1. Pick a library to start with
2. cd into that repo
3. Run `make setup` to get started
4. Check the CLAUDE.md for quick development guide
5. Come back when you want me to review your progress!

**Parallel Development:** Since each library is completely isolated, you can work on multiple libraries at the same time, or even have different people/LLMs work on different libraries simultaneously.

Which library do you want to start with?"
```

Remember: Be conversational, be helpful, create real repos, give clear next steps.