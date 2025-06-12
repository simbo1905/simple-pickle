const {
    parseJMHResults,
    parseSizesData,
    combineResultsWithSizes,
    extractTestType,
    groupDataByTestAndSource,
    calculateSizeSummary,
    findBestPerformer,
    findMostEfficient
} = require('./dataProcessor');

describe('parseJMHResults', () => {
    test('parses JMH JSON format', () => {
        const jmhData = [
            {
                benchmark: "org.sample.SimpleWriteBenchmark.nfp",
                primaryMetric: {
                    score: 1676220.0,
                    scoreError: 123.45,
                    scoreUnit: "ops/s"
                }
            }
        ];
        const result = parseJMHResults(jmhData);
        expect(result).toHaveLength(1);
        expect(result[0].benchmark).toBe("org.sample.SimpleWriteBenchmark.nfp");
        expect(result[0].testType).toBe("SimpleWrite");
        expect(result[0].src).toBe("NFP");
        expect(result[0].score).toBe(1676220.0);
    });

    test('handles multiple frameworks', () => {
        const jmhData = [
            { benchmark: "org.sample.SimpleWriteBenchmark.nfp", primaryMetric: { score: 100, scoreUnit: "ops/s" } },
            { benchmark: "org.sample.SimpleWriteBenchmark.jdk", primaryMetric: { score: 200, scoreUnit: "ops/s" } },
            { benchmark: "org.sample.SimpleWriteBenchmark.protobuf", primaryMetric: { score: 300, scoreUnit: "ops/s" } }
        ];
        const result = parseJMHResults(jmhData);
        expect(result).toHaveLength(3);
        expect(result.map(r => r.src)).toEqual(["NFP", "JDK", "PTB"]);
        expect(result.map(r => r.testType)).toEqual(["SimpleWrite", "SimpleWrite", "SimpleWrite"]);
    });
});

describe('parseSizesData', () => {
    test('parses sizes JSON format', () => {
        const sizesData = {
            "SimpleWrite": {"NFP": 71, "JDK": 1026, "PTB": 32},
            "Paxos": {"NFP": 89, "JDK": 1690, "PTB": 45}
        };
        const result = parseSizesData(sizesData);
        expect(result.SimpleWrite.NFP).toBe(71);
        expect(result.SimpleWrite.JDK).toBe(1026);
        expect(result.Paxos.PTB).toBe(45);
    });
});

describe('combineResultsWithSizes', () => {
    test('combines JMH results with size data', () => {
        const jmhResults = [
            { testType: "SimpleWrite", src: "NFP", score: 1676220, scoreUnit: "ops/s" },
            { testType: "SimpleWrite", src: "JDK", score: 1915354, scoreUnit: "ops/s" }
        ];
        const sizesData = {
            "SimpleWrite": {"NFP": 71, "JDK": 1026}
        };
        
        const result = combineResultsWithSizes(jmhResults, sizesData);
        expect(result).toHaveLength(2);
        expect(result[0].size).toBe(71);
        expect(result[1].size).toBe(1026);
    });

    test('handles missing size data', () => {
        const jmhResults = [
            { testType: "NewTest", src: "NFP", score: 1000, scoreUnit: "ops/s" }
        ];
        const sizesData = {};
        
        const result = combineResultsWithSizes(jmhResults, sizesData);
        expect(result[0].size).toBe(-1); // Default for missing data
    });
});

describe('extractTestType', () => {
    test('extracts clean test type from new benchmark names', () => {
        expect(extractTestType('org.sample.SimpleWriteBenchmark.nfp')).toBe('SimpleWrite');
        expect(extractTestType('org.sample.SimpleReadBenchmark.jdk')).toBe('SimpleRead');
        expect(extractTestType('org.sample.SimpleRoundTripBenchmark.protobuf')).toBe('SimpleRoundTrip');
        expect(extractTestType('org.sample.PaxosBenchmark.nfp')).toBe('Paxos');
    });

    test('handles edge cases', () => {
        expect(extractTestType('org.sample.ArrayBenchmark.jdk')).toBe('Array');
        expect(extractTestType('invalid.format')).toBe('invalid.format');
    });
});

describe('calculateSizeSummary', () => {
    test('calculates size summary from sizes data', () => {
        const sizesData = {
            "SimpleWrite": {"NFP": 71, "JDK": 1026, "PTB": 32},
            "Paxos": {"NFP": 89, "JDK": 1690, "PTB": 45}
        };
        
        const result = calculateSizeSummary(sizesData);
        expect(result).toEqual({
            testTypes: ["SimpleWrite", "Paxos"],
            totalTests: 2,
            avgSizes: {
                NFP: 80,   // (71 + 89) / 2
                JDK: 1358, // (1026 + 1690) / 2
                PTB: 38.5  // (32 + 45) / 2
            },
            compressionRatio: {
                NFP: 16.975, // 1358 / 80
                PTB: 35.27   // 1358 / 38.5
            }
        });
    });
});

describe('groupDataByTestAndSource', () => {
    test('groups combined data by test type and source', () => {
        const testData = [
            { testType: 'SimpleWrite', src: 'NFP', score: 100, size: 71 },
            { testType: 'SimpleWrite', src: 'JDK', score: 200, size: 1026 },
            { testType: 'Paxos', src: 'NFP', score: 300, size: 89 }
        ];

        const result = groupDataByTestAndSource(testData);
        expect(result.SimpleWrite).toBeDefined();
        expect(result.SimpleWrite.NFP).toHaveLength(1);
        expect(result.SimpleWrite.JDK).toHaveLength(1);
        expect(result.Paxos).toBeDefined();
        expect(result.Paxos.NFP).toHaveLength(1);
    });
});

describe('findBestPerformer', () => {
    test('finds item with highest score', () => {
        const data = [
            { score: 100, src: 'NFP' },
            { score: 300, src: 'JDK' },
            { score: 200, src: 'PTB' }
        ];
        const result = findBestPerformer(data);
        expect(result).toEqual({ score: 300, src: 'JDK' });
    });

    test('returns default for empty data', () => {
        expect(findBestPerformer([])).toEqual({ score: 0, src: '-' });
    });
});

describe('findMostEfficient', () => {
    test('finds item with smallest size', () => {
        const data = [
            { size: 100, src: 'NFP' },
            { size: 50, src: 'JDK' },
            { size: 75, src: 'PTB' }
        ];
        const result = findMostEfficient(data);
        expect(result).toEqual({ size: 50, src: 'JDK' });
    });

    test('returns default for empty data', () => {
        expect(findMostEfficient([])).toEqual({ size: Infinity, src: '-' });
    });
});