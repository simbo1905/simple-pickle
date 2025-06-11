// Data processing functions separated for testability

function parseNJSONLine(line) {
    try {
        return JSON.parse(line);
    } catch (e) {
        return null;
    }
}

function parseNJSONContent(content) {
    const lines = content.split('\n').filter(line => line.trim());
    const parsed = lines.map(parseNJSONLine).filter(Boolean);
    return parsed;
}

function convertNJSONToVisualizationFormat(njsonData) {
    // Handle both array and NJSON string input
    const data = typeof njsonData === 'string' ? parseNJSONContent(njsonData) : njsonData;
    
    return data.map(item => ({
        benchmark: item.benchmark,
        primaryMetric: {
            score: item.score,
            scoreError: item.error === "NaN" ? 0 : parseFloat(item.error),
            scoreUnit: item.units || "ops/s"
        },
        ts: item.ts,
        size: item.size,
        src: item.src,
        comment: item.comment || ""
    }));
}

function extractTestType(benchmark) {
    const parts = benchmark.split('.');
    const lastPart = parts[parts.length - 1];
    
    // For benchmarks like SimpleBenchmark.jdkSerialize, extract from class name
    if (parts.length > 1 && parts[parts.length - 2].endsWith('Benchmark')) {
        const benchmarkClass = parts[parts.length - 2];
        return benchmarkClass.replace(/Benchmark$/i, '');
    }
    
    // Remove common suffixes
    return lastPart
        .replace(/Serialize$/i, '')
        .replace(/serialize$/i, '')
        .replace(/Jdk$/i, '')
        .replace(/Nfp$/i, '')
        .replace(/Protobuf$/i, '')
        .replace(/Read$/i, '')
        .replace(/Write$/i, '')
        .replace(/RoundTrip$/i, '');
}

function groupDataByTestAndSource(data) {
    const grouped = {};
    
    data.forEach(item => {
        const testType = extractTestType(item.benchmark);
        if (!grouped[testType]) {
            grouped[testType] = {};
        }
        if (!grouped[testType][item.src]) {
            grouped[testType][item.src] = [];
        }
        grouped[testType][item.src].push(item);
    });
    
    return grouped;
}

function calculateAverageScore(items) {
    if (!items || items.length === 0) return 0;
    const sum = items.reduce((acc, item) => acc + item.primaryMetric.score, 0);
    return sum / items.length;
}

function findBestPerformer(data) {
    if (!data || data.length === 0) return { score: 0, src: '-' };
    
    return data.reduce((best, item) => {
        const score = item.primaryMetric.score;
        return score > best.score ? { score, src: item.src } : best;
    }, { score: 0, src: '-' });
}

function findMostEfficient(data) {
    if (!data || data.length === 0) return { size: Infinity, src: '-' };
    
    return data.reduce((best, item) => {
        return item.size < best.size ? { size: item.size, src: item.src } : best;
    }, { size: Infinity, src: '-' });
}

// Export for Node.js testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        parseNJSONLine,
        parseNJSONContent,
        convertNJSONToVisualizationFormat,
        extractTestType,
        groupDataByTestAndSource,
        calculateAverageScore,
        findBestPerformer,
        findMostEfficient
    };
}