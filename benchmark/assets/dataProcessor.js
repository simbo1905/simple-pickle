// Data processing functions for JMH + Sizes architecture

function parseJMHResults(jmhData) {
    return jmhData.map(item => ({
        benchmark: item.benchmark,
        testType: extractTestType(item.benchmark),
        src: extractSource(item.benchmark),
        score: item.primaryMetric.score,
        scoreError: item.primaryMetric.scoreError || 0,
        scoreUnit: item.primaryMetric.scoreUnit || "ops/s"
    }));
}

function parseSizesData(sizesData) {
    return sizesData;
}

function combineResultsWithSizes(jmhResults, sizesData) {
    return jmhResults.map(result => {
        const size = sizesData[result.testType] && sizesData[result.testType][result.src] 
            ? sizesData[result.testType][result.src] 
            : -1;
        return {
            ...result,
            size: size
        };
    });
}

function extractTestType(benchmark) {
    const parts = benchmark.split('.');
    if (parts.length >= 2) {
        const className = parts[parts.length - 2];
        if (className.endsWith('Benchmark')) {
            return className.replace(/Benchmark$/, '');
        }
    }
    return benchmark;
}

function extractSource(benchmark) {
    const method = benchmark.split('.').pop();
    if (method === 'nfp') return 'NFP';
    if (method === 'jdk') return 'JDK';
    if (method === 'protobuf') return 'PTB';
    return method.toUpperCase();
}

function groupDataByTestAndSource(data) {
    const grouped = {};
    
    data.forEach(item => {
        if (!grouped[item.testType]) {
            grouped[item.testType] = {};
        }
        if (!grouped[item.testType][item.src]) {
            grouped[item.testType][item.src] = [];
        }
        grouped[item.testType][item.src].push(item);
    });
    
    return grouped;
}

function calculateSizeSummary(sizesData) {
    const testTypes = Object.keys(sizesData);
    const totalTests = testTypes.length;
    
    if (totalTests === 0) {
        return {
            testTypes: [],
            totalTests: 0,
            avgSizes: {},
            compressionRatio: {}
        };
    }
    
    // Calculate average sizes across all test types
    const avgSizes = {};
    const sources = ['NFP', 'JDK', 'PTB'];
    
    sources.forEach(src => {
        let total = 0;
        let count = 0;
        testTypes.forEach(testType => {
            if (sizesData[testType][src] !== undefined) {
                total += sizesData[testType][src];
                count++;
            }
        });
        avgSizes[src] = count > 0 ? total / count : 0;
    });
    
    // Calculate compression ratios (relative to JDK)
    const compressionRatio = {};
    if (avgSizes.JDK > 0) {
        compressionRatio.NFP = avgSizes.JDK / avgSizes.NFP;
        compressionRatio.PTB = Math.round((avgSizes.JDK / avgSizes.PTB) * 100) / 100;
    }
    
    return {
        testTypes,
        totalTests,
        avgSizes,
        compressionRatio
    };
}

function createFocusedSizeSummary(testType, sizeData) {
    if (!sizeData || (!sizeData.NFP && !sizeData.JDK)) {
        return null;
    }
    
    const focusedSizes = {
        testType,
        dataType: sizeData.dataType || testType,
        testData: sizeData.testData || '',
        sizes: {
            NFP: sizeData.NFP || 0,
            JDK: sizeData.JDK || 0,
            PTB: sizeData.PTB || 0
        }
    };
    
    return focusedSizes;
}

function findBestPerformer(data) {
    if (!data || data.length === 0) return { score: 0, src: '-' };
    
    return data.reduce((best, item) => {
        const score = item.score || item.primaryMetric?.score || 0;
        return score > best.score ? { score, src: item.src } : best;
    }, { score: 0, src: '-' });
}

function findMostEfficient(data) {
    if (!data || data.length === 0) return { size: Infinity, src: '-' };
    
    return data.reduce((best, item) => {
        return item.size < best.size ? { size: item.size, src: item.src } : best;
    }, { size: Infinity, src: '-' });
}

// Legacy NJSON functions for backward compatibility
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

function calculateAverageScore(items) {
    if (!items || items.length === 0) return 0;
    const sum = items.reduce((acc, item) => acc + item.primaryMetric.score, 0);
    return sum / items.length;
}

// Export for Node.js testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        parseJMHResults,
        parseSizesData,
        combineResultsWithSizes,
        extractTestType,
        groupDataByTestAndSource,
        calculateSizeSummary,
        findBestPerformer,
        findMostEfficient,
        // Legacy functions
        parseNJSONLine,
        parseNJSONContent,
        convertNJSONToVisualizationFormat,
        calculateAverageScore
    };
}