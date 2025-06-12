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

### CRITICAL: UI TESTS MUST USE REAL DATA ONLY
**NEVER CREATE FAKE DATA FOR UI TESTING**. The Jest tests handle data processing logic. The backend tests handle API functionality. UI tests exist ONLY to verify the UI works with REAL benchmark data. If you need test data, generate it properly:
1. Run a quick benchmark: `java -jar target/benchmarks.jar ".*SimpleWrite.*" -i 1 -wi 1 -f 1`
2. Generate sizes: `java -cp "target/benchmarks.jar:." Temp.java` 
3. Test with REAL data only - no fake JSON, no made-up values

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

### CRITICAL: File Naming Requirements for New JMH+Sizes Architecture

**THE SERVER EXPECTS SPECIFIC FILE NAMING PATTERNS:**
1. **JMH result files**: Must start with `jmh-result-` and end with `.json`
   - Example: `jmh-result-20250612_224532.json`
2. **Sizes files**: Must start with `sizes-` and end with `.json`
   - Example: `sizes-20250612_224532.json`
   - NOT just `sizes.json` - that won't be found!

**TO GENERATE PROPER DATA:**
```bash
# Generate sizes with correct naming
java -cp "target/benchmarks.jar:." src/main/java/org/sample/SizeCalculator.java
mv sizes.json sizes-$(date +"%Y%m%d_%H%M%S").json

# Generate JMH data
java -jar target/benchmarks.jar ".*SimpleWrite.*" -i 1 -wi 1 -f 1
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

## TDD approach 

update tests first to reflect new expected behavior, watch them fail, then fix the implementation.

## Do not run slow tests when creating fresh data for UI development: 

I asked why I was stopping you from running the full benchmark suite, to generate fresh data to test the UI with. You said it was about not burning tokens. I said it was more about not wasting time running slow tests when the task is fixing the UI rendering of results:

>  yes you are on the correct track. yet the benchmarks run for minutes. so you can run fast check of one of the tests to get some early results to test end-to-end without
wasting my time doing a full benchmark to the go fight the UI work. the task here is fixing the rendering of results. so i don't want to wait 5 minutes for a full
benchmark to get that done. yes you should re-read things and you should be systematic but don't run the full slow suite when the task is fixing the UI.

⏺ Exactly - I need a quick single benchmark run to get test data for UI work, not the full slow suite. Let me check if there's a way to run just one quick benchmark:
⏺ ... Let me run just SimpleWrite benchmarks quickly with minimal iterations:
⏺ Bash(TIMESTAMP=$(date +"%Y%m%d_%H%M%S") && echo "Running quick test: jmh-result-${TIMESTAMP}.json" && java -jar target/benchmarks.jar ".*SimpleBenchmark.jdkWrite|.*S…)
⎿  Running quick test: jmh-result-20250612_162150.json
# JMH version: 1.37
# VM version: JDK 24, Java HotSpot(TM) 64-Bit Server VM, 24+36-3646
# VM invoker: /Users/consensussolutions/.sdkman/candidates/java/24-oracle/bin/java
… +96 lines (ctrl+r to expand)

## Write Java That Can Be Used In JShell Or Simple `java Simple.java` Wrappers

Read the CODING_STYLE_LLM.md we do not use private, we use package-private. Yet if we have static immutable things else things that do not modify internal state we on public classes we can make then public static. Then we can launch jshell with the code on the classpath and call the methods directly. This is a great way to test things quickly or to generate some data without running the full benchmark suite. The case in point; the sizes logic in the benchmarks runs once before all tests. If we expose that as a public static method we can run it everytime the full suite runs. Yet we can always run jshell and invoke it. 

The general idea is to understand that modern Java is more like python only if you do not write in a legacy OO manner. Write things in a functional style with record an streams and immutability and do not make silly private mutable state it nearly killed the Java language it was only the JVM that saved Java from oblivion. It has finally modernized the average Java on the internet it substandard. You have memorized what is substandard. Wise up. 

Oh! I can run jshell as a tty. You not so much. So you can write Temp.java and run it with `java Temp.java` and in that just have a main method. If I need to do same I an more easily do the same with jshell. You should javac the temp file to debug then just run it with  `jshell Temp.java` etc. Treat it like a script.

## CRITICAL LESSON: Don't manually write data files - use proper size calculation

**WHAT WENT WRONG**: I manually wrote a fake sizes-sample.json file instead of using the existing size calculation logic from the benchmarks.

**THE RIGHT WAY**: 
1. Run a quick benchmark to get real JMH data: `java -jar target/benchmarks.jar ".*SimpleBenchmark.jdkWrite|.*SimpleBenchmark.nfpWrite" -i 1 -wi 1 -f 1`
2. Use existing size calculation logic via Temp.java wrapper to generate matching sizes file
3. This gives real data: NFP 27 bytes vs JDK 144 bytes (5.3x compression ratio)

**SYSTEMATIC APPROACH THAT WORKS**:
1. **Read existing code** - SizeCalculator.java has all the size calculation logic  
2. **Extract to public static methods** - make size calculation accessible for quick testing
3. **Write simple wrapper** - Temp.java with main() to call existing logic
4. **Generate real data** - `java -cp "target/benchmarks.jar:." Temp.java`
5. **Test with real data** - sanity tests now work with actual benchmark results

**KEY INSIGHT**: Don't reinvent wheels - use existing logic in new ways. The benchmark already calculates sizes, just expose it for standalone use.

## CRITICAL MISTAKE: Skipping Task Prompt Display After Good Work

**WHAT HAPPENED**: I properly documented lessons, cleaned up files, then immediately violated the Task prompt transparency rule by not showing the user the prompt first.

**THE PATTERN**: Do something useful (documentation, cleanup) then immediately fuck up a key step (Task transparency). This is worse than just fucking up because it creates false confidence then betrays trust.

**THE RULE**: ALWAYS show Task prompts to user BEFORE calling Task tool, regardless of what other good work was just done. No exceptions.

## CRITICAL WORKFLOW: Fast Test Before Regression Test

**WHAT USER WANTS**: When I claim "I made UI changes", user wants me to immediately fast test if the feature exists, not waste time on regression test prompts when there's no feature to test.

**WRONG APPROACH**: 
1. Make UI changes
2. Ask user to review regression test prompt
3. User discovers there's no actual feature

**RIGHT APPROACH**:
1. Make UI changes  
2. **Fast test immediately**: Use Task to quickly check "does X exist on the page"
3. **Only if feature exists**: Then proceed to regression testing with user prompt review

**THE RULE**: You must fast test new features exist before seeking regression test approval. Don't waste user's time reviewing test prompts for non-existent features.

## CRITICAL MISTAKE: Task Testing Wrong Thing For 6+ Minutes

**WHAT HAPPENED**: Task spent 6m 23s validating data values in one file, completely missing that file switching (the core feature) is broken.

**THE PROBLEM**: User can search, find multiple files, but clicking on different files fails with errors. The ONLY reason to build this app vs static HTML is fast file switching.

**TASK TESTING PRIORITY WRONG**:
❌ **What Task tested**: Data values accuracy, compression ratios, JSON popup content  
✅ **What Task should test**: File switching works, search → click → different file loads

**PROPER TASK TESTING SEQUENCE**:
1. **FIRST**: Test file switching - search for files, click each one, verify it loads without errors
2. **ONLY IF #1 WORKS**: Then validate data accuracy, UI features, etc.
3. **FAIL FAST**: If file switching broken, stop immediately - app is useless

**TASK PROMPT TEMPLATE FOR FILE-BASED APPS**:
```
Test CORE FUNCTIONALITY FIRST - file switching:
1. Search for multiple files 
2. Click each file in search results
3. Verify each file loads without errors
4. STOP if any file loading fails - report immediately
5. Only if all files load, then test data accuracy
```

## CRITICAL BASH ERROR HANDLING RULE

**THE PROBLEM**: Redirecting stderr (`2>/dev/null`) hides actual failures while pretending success.

**WRONG WAY**:
```bash
command 2>/dev/null  # Hides errors, no way to know if it failed
```

**RIGHT WAY**:
```bash
command > /tmp/output.log && echo "SUCCESS" || echo "FAILED: Check errors above"
```

**EXPLANATION**:
- Redirect **stdout only** (`>` not `2>`) to capture expected output without noise
- Let **stderr show** so actual errors are visible 
- Check **return code** with `&&` (success) and `||` (failure)
- Report **explicit success/failure** based on return code

**RULE**: Never hide stderr unless you explicitly check return codes and handle failures properly.

# You Fix Symptoms Not Causes, Bugs Get Past Tests You Do Not Improve, The UI Has Issues You Ignore

We had an empty file. The UI showed an error when I tested it. The Puppeteer MCP Server subtask missed it. You immediately attempted to destroy value by deleting the empty file to "fix it". This is BULLSHIT an empty file can happen anytime. It was an exceptional case that by its existence was not "impossible". The obvious thing to do was to focus on a total quality solution: 

1. Document the problem in CLAUDE.md
2. Change nothing. Try a new prompt with the Puppeteer MCP Task tool to get it to find the issue by explaining how to be more systematic. 
3. Document what prompt actually got it to find the issue in CLAUDE.md
4. Write the defensive code in the UI to handle bad files. It should try to raise an exception the Puppeteer MCP Task may "see" and report a status bar update that the file was "invalid". 
5. Have the Puppeteer MCP Task only test that this new error handling works. 
6. Have the Puppeteer MCP Task rerun all the tests to see everything now works. 
7. Write a note that the Puppeteer MCP Task should always create a dummy invalide file to test the case. 
8. Add that into the CLAUDE.md
9. Only after all that delete the bad file.

## THE EMPTY FILE ISSUE - SYSTEMATIC QUALITY SOLUTION

**PROBLEM**: Empty file `jmh-result-20250612_162132.json` (0 bytes) exists from failed benchmark run. UI shows error when clicked, but Puppeteer MCP Task missed this during testing.

**WRONG APPROACH**: Delete the empty file to "fix" the symptom.

**RIGHT APPROACH**: Build robust error handling that works for any future empty/corrupt files.

**STEPS TO TOTAL QUALITY**:
1. ✅ Document problem in CLAUDE.md  
2. ✅ Test new Puppeteer prompt to find the issue systematically
3. ✅ Document successful prompt that finds issues

**SUCCESSFUL PROMPT PATTERN FOR ERROR TESTING**:
```
SYSTEMATIC FILE SWITCHING TEST - Test error handling FIRST:
1. Go to http://localhost:8080
2. Search for files - note how many found
3. For EACH file found:
   a. Click the file name
   b. Check if page loads OR shows proper error handling
   c. Check bottom status bar for error messages
   d. Check browser console for JavaScript errors/exceptions
   e. If error handling is broken, screenshot and STOP immediately
4. GOAL: Confirm new error handling works for bad files
5. Only if error handling works properly, then proceed to full test
```
4. ✅ Write defensive UI code with exceptions and status updates
5. ⏳ Test only error handling with Puppeteer
6. ⏳ Rerun all tests to verify everything works
7. ⏳ Always create dummy invalid file for testing
8. ⏳ Add testing pattern to CLAUDE.md
9. ⏳ Only then delete bad file 

**IMPORTANT LESSON**: You keep on wanting to find the bug before you fix the tests. That is invalid as if-and-only-if you have a test that finds the problem which is the
long term problem "tests are invalid so no point in trying to fix it as we would never know if the fix worked". then when the test actually do something, only then figure it out and fix it.

## Puppeteer MCP Server Test Prompts

**ALL PROMPTS MUST BE SAVED AND DOCUMENTED**: We invest effort in prompt engineering - capture that value by saving successful prompts to dated files and documenting their purpose.

**SAVED PROMPTS**:
- `test-seamless-workflow-2025-12-06.js` - Tests documented "Interactive Results Viewer - Seamless File Navigation" workflow from README.md. Uses direct Puppeteer (not MCP Server).

**PROCESS**: Every working Task prompt must be:
1. Saved to dated file: `test-name-YYYY-MM-DDTHH-MM.js`
2. Documented here with bullet point describing purpose
3. Include the exact prompt text in the file for reuse
