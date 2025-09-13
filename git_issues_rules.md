# GitHub Issues Rules - CORRECTED Planning for OHLCV Service

## **CRITICAL: Foundation Issues Missing**

**Current issues #11-20 should be CLOSED. Service needs foundation issues #1-10 first.**

## **MANDATORY Requirements for Every GitHub Issue**

### **1. Clarity & Documentation**
- **Detailed description** for another LLM to understand completely
- **Clear strategy** with step-by-step approach
- **Examples reference**: Which example script validates this issue (e.g., `examples/simple_daemon_control.py` for daemon management)

### **2. Examples-Driven Validation (PRIORITY 1)**
- **Primary completion criteria**: `./run_all_examples.py` passes completely
- **Examples Integration**: Corresponding example script must work (e.g., `examples/data_retrieval.py` for OHLCV collection)
- **Living documentation**: Examples ARE the documentation - no separate docs needed

### **3. Testing Requirements (PRIORITY ORDER)**
- **PRIORITY 1**: `./run_all_examples.py` passes (examples work = service works)
- **PRIORITY 2**: `poetry run pytest` passes (unit tests with database-per-worker pattern)
- **Both required**: Examples AND unit tests must pass before merge

### **4. Development Flow**
- **Examples-Driven Development**: Examples fail → Write tests → Implement → Examples pass
- **TDD Support**: Write unit tests using database-per-worker pattern to support example functionality
- **Branch workflow**: `git checkout -b feature/issue-name`

### **5. Git Workflow (NON-NEGOTIABLE)**
- Create new git branch for each issue
- Examples-driven implementation
- `./run_all_examples.py` must pass before commit
- `poetry run pytest` must pass (with database-per-worker isolation)
- Git add, commit, push, create PR, merge
- Close issue immediately after merge
- Switch to main and pull latest

### **6. Code Quality Standards**
- **No legacy code** - clean, modern async implementations only
- **Examples validation** - feature works in real-world OHLCV collection usage
- **Service-level operations** - pure fullon ecosystem patterns
- **CLAUDE.md compliance** - follow all architectural principles

## **Examples-First Completion Criteria**

### **Feature Complete When:**
1. ✅ **Examples pass**: Corresponding example script runs successfully
2. ✅ **Integration works**: `./run_all_examples.py` completes without errors  
3. ✅ **Unit tests pass**: `pytest` passes 100% with database-per-worker isolation
4. ✅ **Quality checks**: Linting (ruff, black), formatting, type checking (mypy)
5. ✅ **CLAUDE.md compliance**: Implementation follows all architectural patterns

### **Foundation Issues Needed (#1-10)**
- **Issue #1**: Basic OhlcvCollector (✅ DONE - see implementation)
- **Issue #2**: Basic TradeCollector  
- **Issue #3**: Simple Manager coordination
- **Issue #4**: Basic daemon with database-driven configuration  
- **Issue #5**: Health monitoring via ProcessCache
- **Issue #6**: Configuration management (✅ DONE - see implementation)
- **Issue #7**: Basic error handling
- **Issue #8**: Integration testing
- **Issue #9**: Examples creation
- **Issue #10**: Documentation cleanup

### **Advanced Issues (#11-20) - CLOSE UNTIL FOUNDATION COMPLETE**
Current issues are over-engineered and should be closed. Advanced features can be recreated later with proper scope.

## **Examples ARE the Contract**

**Why Examples-Driven for OHLCV Service?**
- **Integration tests**: Examples test the complete OHLCV collection stack
- **Real-world usage**: Shows how users will collect market data
- **Async validation**: Examples validate proper async/await patterns
- **Library integration**: Tests fullon ecosystem integration (exchange, ohlcv, cache, log)
- **Performance validation**: Examples run actual data collection scenarios
- **Acceptance criteria**: When examples pass, OHLCV service is ready for production

## **OHLCV Service Specific Requirements**

### **Simplified Architecture Focus**
Every issue must demonstrate:
- **Simple integration**: Use fullon ecosystem, don't reinvent
- **Database-driven**: Read config from fullon_orm, no hardcoded values
- **~50-100 lines per class**: Keep collectors simple
- **Follow ticker service patterns**: Proven approach
- **Foundation first**: Issues #1-10 before advanced features

### **Testing Infrastructure**
- **Database per worker**: Tests use TimescaleDB with rollback isolation
- **Redis per worker**: Cache tests use isolated Redis databases (1-15)
- **Factory pattern**: All test data via factories, no hardcoded data
- **Real integration**: Tests use actual fullon_ohlcv and fullon_cache

### **Performance Requirements**
- Examples must demonstrate:
  - **Multi-symbol collection**: Handle 10+ symbols simultaneously
  - **Real-time processing**: Sub-second data processing
  - **Error recovery**: Graceful handling of connection failures
  - **Resource efficiency**: Proper memory and connection management

## **Pre-Implementation Checklist**

Before starting any issue, developer must:
1. ✅ Read [CLAUDE.md](./CLAUDE.md) - Core architecture and patterns
2. ✅ Review [git_plan.md](./git_plan.md) - Issue requirements and roadmap
3. ✅ Check relevant example script - Understand expected behavior
4. ✅ Verify test infrastructure - Ensure database-per-worker setup works
5. ✅ Understand fullon library usage - Check integration patterns

## **Issue Validation Template**

```markdown
## Issue Validation Checklist
- [ ] Examples pass: `./run_all_examples.py`
- [ ] Unit tests pass: `poetry run pytest`
- [ ] Quality checks pass: `poetry run ruff check . && poetry run mypy src/`
- [ ] CLAUDE.md compliance verified
- [ ] Example integration demonstrates feature
- [ ] Database-per-worker isolation working
- [ ] Async patterns followed throughout
- [ ] Proper error handling implemented
- [ ] Structured logging included
```

**Remember**: `./run_all_examples.py` passing = OHLCV Service works = Issue complete = Ready for production market data collection! 🚀

## **Critical Success Metrics**

- **Reliability**: Examples run consistently without failures
- **Performance**: Handle real-time data collection at scale
- **Integration**: Seamless fullon ecosystem integration
- **Maintainability**: Clean, testable, documented code
- **Production Ready**: Examples demonstrate production usage patterns