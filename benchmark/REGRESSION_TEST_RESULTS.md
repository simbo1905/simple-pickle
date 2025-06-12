# Benchmark Visualization Automated Testing Methodology

## Current Testing Approach (Updated 2025-01-06)

### Self-Documenting Server Architecture
- Server provides `/info` endpoint with current API documentation
- Task tool checks `/info` first to understand available endpoints
- Automated testing builds on previous successful test scripts
- Timestamped test scripts saved for regression detection

### Intelligent Test Evolution Strategy

#### 1. API Discovery Phase
```bash
# Task tool first command
curl http://localhost:8080/info
```
Returns current endpoint list with descriptions for planning test approach.

#### 2. Historical Context Check  
```bash
# Look for previous successful tests
ls regression-test-2025-*-*.js
```
Task tool examines latest timestamped script to understand what worked before.

#### 3. Regression Detection
Task compares:
- Current `/info` endpoint list vs previous test script comments
- Determines if old test script is still valid
- Identifies new endpoints that need testing

#### 4. Adaptive Testing Strategy
- **API Unchanged**: Run previous successful test script
- **Minor Changes**: Comment out invalid sections, adapt for new endpoints  
- **Major Refactor**: Fall back to exploratory testing with `/info` guidance
- **New Features**: Test new endpoints with context provided in Task prompt

#### 5. Success Preservation
At completion, Task saves working script as:
```
regression-test-YYYY-MM-DD-HH-MM-SS.js
```
For next session to use as baseline.

## Benefits Over Previous Approach

### Before (Inefficient)
- Task spent 8+ minutes exploring code to understand software
- Had to guess endpoint behavior and UI functionality  
- No historical context from previous test runs
- Wasted tokens on repeated discovery

### Now (Efficient)
- Task checks `/info` for current API in seconds
- Builds on previous working test scripts
- Adapts incrementally rather than starting from scratch
- Only explores when major changes detected

## Example Task Workflow

### Phase 1: Context Gathering
```javascript
// Task tool automated steps:
// 1. curl http://localhost:8080/info -> get current API
// 2. ls regression-test-*.js -> find previous working test
// 3. Compare endpoints in old test vs current /info
```

### Phase 2: Test Execution  
```javascript
// If API matches previous test:
//   - Run existing test script
//   - Take verification screenshots
// If API changed:
//   - Comment out invalid endpoint tests
//   - Add tests for new endpoints described in prompt
//   - Verify functionality with screenshots
```

### Phase 3: Success Backup
```javascript
// Save working test with timestamp:
// regression-test-2025-01-06-14-30-45.js
// Contains comments mapping test sections to endpoints
```

## Quality Improvements

### Documentation Accuracy
- Server `/info` endpoint automatically reflects actual registered endpoints
- No drift between documentation and implementation
- Task tool gets authoritative API information

### Test Reliability  
- Historical context prevents repeated failures on known issues
- Incremental adaptation vs full exploration
- Timestamped success scripts provide rollback capability

### Token Efficiency
- Reduces 8+ minute exploration to seconds of `/info` checking
- Builds on working foundation rather than rediscovering basics
- Only burns tokens on actual new functionality testing

## Test Script Requirements

### Script Structure
```javascript
// Endpoint mapping comments for regression detection:
// Tests /api/results endpoint
// Tests /api/search endpoint  
// Tests /api/file endpoint
// Tests static file serving
// Tests UI search functionality
```

### Success Criteria
- All endpoints from `/info` tested
- UI functionality verified with screenshots
- No failures on core visualization features
- Working script saved with timestamp

This methodology transforms automated testing from exploration-heavy to context-aware, dramatically improving efficiency and reliability.