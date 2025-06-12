# NFP Benchmark Visualization System

## Quick Start

1. **Start the server:**
```bash
cd /Users/Shared/no-framework-pickler/benchmark
java Server.java
```

2. **Open browser:**
Navigate to http://localhost:8080

3. **View results:**
The latest benchmark results load automatically showing performance comparison data.

## Current UI Features

### Interactive Data Table
- **Benchmark results displayed in sortable table format**
- **Color-coded sources:**
  - NFP (No Framework Pickler): Green background
  - JDK (Java Development Kit): Gray background  
  - PTB (Protocol Buffers): Blue background

### File Search System
- **Search input box** for filtering result files
- **Real-time dropdown** showing matching files (300ms debounce)
- **Click to load** any historical result file
- **Status bar** shows current file and record count

### Data Columns
- **Test Type**: Benchmark category (Paxos, Simple, Tree, etc.)
- **Source**: Serialization library (NFP/JDK/PTB)
- **Score**: Performance in operations per second
- **Size**: Serialized data size in bytes
- **Timestamp**: When benchmark was run
- **Actions**: JSON file icon to view raw data

## Server Endpoints

The server provides self-documenting API via `/info` endpoint:

```bash
curl http://localhost:8080/info
```

### Current Endpoints
- `/` - Serves static files (index.html, CSS, JS). Main UI interface.
- `/api/results` - Returns latest benchmark results with filename in visualization format
- `/api/search` - Searches result files. Query param: ?q=searchterm. Returns array of matching filenames.
- `/api/file` - Loads specific result file. Query param: ?name=filename. Returns file data in visualization format.
- `/api/convert` - Converts NJSON format to visualization format. POST body: raw NJSON content.
- `/info` - Returns server configuration, status, and API documentation

## Data Sources

### Automatic Loading
- **Latest Results**: Automatically loads most recent `results-*.njson` file
- **File Search**: Search and load any historical benchmark file
- **Real-time Updates**: Status bar shows current file and row count

### Data Format
The system converts NJSON benchmark results to table format:
```
NJSON Input:  {"benchmark": "SimpleBenchmark.nfpWrite", "score": "1676220", ...}
Table Output: NFP | Simple | 1,676,220 ops/s | 71 bytes | 2025-06-09 14:36:51
```

## File Management

### Result Files
- **Naming Pattern**: `results-YYYYMMDD_HHMMSS.njson`
- **Location**: Benchmark directory (same as server)
- **Search**: Type partial filename to filter dropdown

### File Operations
- **Auto-discovery**: Server scans directory for `results-*.njson` files
- **Search filtering**: Real-time search through available files
- **Load on demand**: Click any file in dropdown to load its data

## Development & Testing

### Server Management
```bash
./start-server.sh              # Full cycle: tests + server + health check
./start-server.sh -skipRebuild # Fast restart without tests
./check-server.sh              # Check server status
./stop-server.sh               # Stop server cleanly
```

### Manual Testing
- **Search Functionality**: Type in search box, verify dropdown appears
- **File Loading**: Click files in dropdown, verify data updates
- **Color Coding**: Verify NFP/JDK/PTB sources have correct colors
- **JSON Viewer**: Click file icons (📄) to view raw benchmark data

### Automated Testing
Uses Puppeteer MCP Server with intelligent test evolution:
- Checks `/info` endpoint for current API
- Builds on previous successful test scripts  
- Takes screenshots for visual verification
- Saves working tests with timestamps

## Performance Data

### Current Benchmark Results
Visualization displays real performance comparisons:
- **NFP typically 3-15x faster** than JDK serialization
- **NFP typically 2-15x smaller** payloads than JDK
- **Comprehensive type support**: primitives, collections, records, enums

### Data Quality
- **Real measurements** from JMH benchmarks
- **Size calculations** from actual serialized byte arrays
- **Multiple test types** covering various data structures
- **Historical tracking** via timestamped result files

## Technical Details

### Architecture
- **Java 21 HTTP Server** with virtual threads
- **Vanilla JavaScript** frontend (no frameworks)
- **Self-documenting endpoints** via record-based registration
- **CORS enabled** for local development

### File Processing
- **NJSON parsing**: Handles newline-delimited JSON from benchmarks
- **Format conversion**: Transforms to visualization-friendly structure
- **Error handling**: Graceful degradation for malformed data
- **Caching**: Latest results cached for performance

Stop server with Ctrl+C when done.