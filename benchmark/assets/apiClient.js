// API client functions for testing

async function searchFiles(query) {
    const url = query ? `/api/search?q=${encodeURIComponent(query)}` : '/api/search';
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
}

async function loadResults() {
    const response = await fetch('/api/results');
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
}

async function loadSpecificFile(filename) {
    const response = await fetch(`/api/file?name=${encodeURIComponent(filename)}`);
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
}

// Export for Node.js testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        searchFiles,
        loadResults,
        loadSpecificFile
    };
}