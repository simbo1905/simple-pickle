// API client functions for JMH + Sizes architecture

async function searchJMHFiles(query) {
    const url = query ? `/api/search?type=jmh&q=${encodeURIComponent(query)}` : '/api/search?type=jmh';
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
}

async function loadJMHResults() {
    const response = await fetch('/api/jmh-results');
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
}

async function loadSpecificJMHFile(filename) {
    const response = await fetch(`/api/jmh-file?name=${encodeURIComponent(filename)}`);
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
}

async function loadLatestSizes() {
    const response = await fetch('/api/sizes');
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
}

async function loadServerInfo() {
    const response = await fetch('/info');
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.text();
}

// Legacy functions for backward compatibility
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
        searchJMHFiles,
        loadJMHResults,
        loadSpecificJMHFile,
        loadLatestSizes,
        loadServerInfo,
        // Legacy functions
        searchFiles,
        loadResults,
        loadSpecificFile
    };
}