# NFP Benchmark Visualization System

## Overview
This directory contains a complete benchmark visualization system for Java serialization performance analysis. The system consists of:

1. **HTTP Server** (Server.java) - Serves static files and provides REST API endpoints
2. **Web Frontend** (assets/) - Interactive benchmark results viewer with file search
3. **Data Processing** - Converts NJSON benchmark results to visualization format

## Architecture

### Backend Components
- **Server.java**: HTTP server with static file serving and API endpoints
- **API Endpoints**:
  - `/api/results` - Returns latest benchmark results with filename
  - `/api/search?q=query` - Searches for result files matching query
  - `/api/file?name=filename` - Loads specific result file
  - `/info` - Server configuration and status

### Frontend Components  
- **index.html** - Main visualization interface
- **dataProcessor.js** - Core data processing logic (tested)
- **apiClient.js** - API interaction functions (tested)
- **Jest Tests** - Comprehensive test coverage for all logic

### Data Flow
1. **Benchmark Results** → NJSON files (e.g., `results-20250609_143651.njson`)
2. **Server** → Finds files, converts NJSON to visualization format
3. **Frontend** → Fetches data via REST API, renders interactive table
4. **Search** → Real-time file filtering with dropdown selection

## Testing & Development Workflow

### CRITICAL: Use Local Puppeteer MCP Server
**ALWAYS use the local Puppeteer MCP Server for UI testing. It works perfectly. Stop trying to reinstall it or use other methodologies.**

### Layered Testing
**NEVER attempt UI tests without verifying backend first. Follow this order:**

1. **Sanity Tests**: Run `./sanity-tests.sh` to verify all endpoints work with actual data
2. **Basic Screenshot**: Take one Puppeteer screenshot to verify UI loads and shows data  
3. **Full Regression**: Only after both above pass, run complete UI interaction tests

**Rule**: If sanity tests show "no benchmark data available", UI tests will show empty state - fix data first.

### Testing Task Requirements

#### Task Tool Prompt Transparency
**CRITICAL: Before running any Task tool, ALWAYS show the user the exact prompt being sent.**

The Task tool only receives what's in the prompt parameter - it doesn't automatically read project files. User must see and approve the context being passed.

#### Intelligent Test Script Evolution
**CRITICAL: When using the Task tool for testing, structure the prompt as follows:**

1. **Current Context**: "Check /info endpoint (curl http://localhost:8080/info) to understand current API"
2. **Previous Success**: "Look for timestamped test scripts (regression-test-YYYY-MM-DD-*.js) to see what worked before"  
3. **New Functionality**: "[Explain any new features/endpoints to test]"
4. **Fallback Strategy**: "If API changed significantly, comment out invalid parts of old test and adapt"
5. **Backup Success**: "At completion, save working test script with timestamp for future reference"

#### Test Script Baseline Strategy
The Task should:
- Compare current `/info` output with previous test script comments to detect API changes
- Start with last successful test script if endpoints match 
- Incrementally adapt test for changed/new endpoints rather than exploring from scratch
- Only fall back to full exploratory testing if major refactor invalidated previous approach
- Document which endpoints each test section targets (in script comments)
- Save successful final script as `regression-test-[timestamp].js` for next session

This prevents the 8+ minute fumbling that wastes time and tokens by giving the Task historical context.

An example of good practice for a prompt to a sub Task to run the Puppeteer MCP Server might look like this:

```json
{
  "prompt": "Check /info endpoint (curl http://localhost:8080/info) to understand current API. Look for timestamped test scripts (regression-test-YYYY-MM-DD-*.js) to see what worked before. Test the new file search functionality by taking screenshots of the UI state and verifying interactive features. If API changed significantly, comment out invalid parts of old test and adapt. At completion, save working test script with timestamp for future reference."
}
```

### Test Cycle Pattern
```bash
# Full rebuild and test cycle (runs Jest tests + starts server)
cd /Users/Shared/no-framework-pickler/benchmark
./start-server.sh

# Fast restart without tests (for quick iterations)
./start-server.sh -skipRebuild

# Test UI functionality with Puppeteer MCP Server
# Use Task tool with Puppeteer instructions to:
# - Take screenshots of UI state
# - Test interactive features (file search, JSON popups)
# - Verify file loading and status updates

# Check server status and stop when done
./check-server.sh
./stop-server.sh
```

### File Organization
- **Source Code**: `assets/` - Clean production code only
- **Test Outputs**: `target/` - Screenshots, test files, temporary outputs
- **Backup/Archive**: `/tmp/` - Old prototype code moved out of git

### Quality Standards
- All interactive features must be tested with Puppeteer screenshots
- Jest tests required for all data processing logic
- Server endpoints must be tested with curl
- No prototype/concept code in git - move to `/tmp/`

## Server Management

### Quick Commands
```bash
./start-server.sh              # Full cycle: tests + server start + health check
./start-server.sh -skipRebuild # Fast restart: skip tests, just restart server
./sanity-tests.sh              # Verify all endpoints work with actual data
./check-server.sh              # Check server status and port usage
./stop-server.sh               # Stop server cleanly using PID file
```

### Server Features
- **Java 21 Direct Execution**: Runs `java Server.java` without separate compilation
- **Health Check**: Waits for server to respond before declaring success
- **Safe Port Management**: Only kills processes we started (via PID file)
- **Integrated Testing**: Runs Jest tests before server start (unless skipped)

### Server Configuration
- **Assets Directory**: `assets/` (clean production code)
- **Data Directory**: `.` (current directory for .njson files)
- **Port**: 8080
- **CORS**: Enabled for local development

## Features

### File Search System
- **Search Bar**: Type to filter result files in real-time
- **Dropdown**: Shows matching files with 300ms debounce
- **File Loading**: Click any file to load its data immediately
- **Status Updates**: Bottom status bar shows current file and row count

### Data Visualization
- **Interactive Table**: Sortable benchmark results
- **Color Coding**: NFP (green), JDK (gray), PTB (blue) 
- **JSON Viewer**: Click file icons (📄) to view raw data
- **Performance Metrics**: Score, size, timestamp for each benchmark

### API Testing
```bash
# Test search endpoint
curl "http://localhost:8080/api/search?q=results-20250609"

# Test file loading
curl "http://localhost:8080/api/file?name=results-20250609_143651.njson"

# Test latest results
curl "http://localhost:8080/api/results"
```

## Development Notes

### Adding New Features
1. Implement logic in appropriate module (dataProcessor.js, apiClient.js)
2. Add Jest tests for the new functionality
3. Update HTML/CSS as needed for UI changes
4. Test with Puppeteer MCP Server screenshots
5. Update this documentation

### Debugging
- Use browser dev tools for frontend debugging
- Check `/tmp/benchmark-server.log` for server logs
- Run `./check-server.sh` to verify server health
- Use `npm test` to verify core logic

### Performance
- Frontend uses vanilla JavaScript for minimal overhead
- Server uses Java HTTP server with virtual threads
- Search is debounced to avoid excessive API calls
- File loading is optimized with direct NJSON parsing

## Systematic Data Type Testing Plan

### Testing Phases
1. **Primitive Types**: boolean, byte, short, char, int, long, float, double
2. **Boxed Types**: Boolean, Byte, Short, Character, Integer, Long, Float, Double  
3. **Core Java Types**: String, Optional, Arrays
4. **Collections**: List, Map, nested structures
5. **Record Types**: Simple and nested records
6. **Enum Types**: Simple and complex enums
7. **Sealed Hierarchies**: Interface hierarchies with variants
8. **Real-World Scenarios**: Protocols, event sourcing, DTOs

### Testing Methodology
- Measure sizes FIRST with SizeCalculator
- Test NFP, JDK, and Protobuf serialization
- Generate NJSON results with metadata
- Use visualization system to analyze patterns
- Identify NFP strengths and optimization targets

### Success Criteria
- NFP >= JDK performance on most workloads
- NFP <= JDK size on all workloads  
- No runtime failures across test suite
- Comprehensive coverage of documented types

## Critical: Task Instructions

**EVERY Task must be explicitly told to read and follow the no-advertising policy from the global CLAUDE.md files. Tasks do not automatically inherit these rules and will violate them unless explicitly instructed.**

**Template for all Task prompts:**
"CRITICAL: Read /Users/consensussolutions/.claude/CLAUDE.md and follow the no-advertising policy. Never add any branding or advertising to commits, code, or deliverables. [actual task instructions...]"
