const {
    parseNJSONLine,
    parseNJSONContent,
    convertNJSONToVisualizationFormat,
    extractTestType,
    groupDataByTestAndSource,
    calculateAverageScore,
    findBestPerformer,
    findMostEfficient
} = require('./dataProcessor');

describe('parseNJSONLine', () => {
    test('parses valid JSON line', () => {
        const line = '{"benchmark": "test", "score": 100}';
        const result = parseNJSONLine(line);
        expect(result).toEqual({ benchmark: "test", score: 100 });
    });

    test('returns null for invalid JSON', () => {
        const line = 'not json';
        const result = parseNJSONLine(line);
        expect(result).toBeNull();
    });
});

describe('parseNJSONContent', () => {
    test('parses multiple NJSON lines', () => {
        const content = `{"benchmark": "test1", "score": 100}
{"benchmark": "test2", "score": 200}`;
        const result = parseNJSONContent(content);
        expect(result).toHaveLength(2);
        expect(result[0].benchmark).toBe("test1");
        expect(result[1].benchmark).toBe("test2");
    });

    test('ignores empty lines', () => {
        const content = `{"benchmark": "test1", "score": 100}

{"benchmark": "test2", "score": 200}
`;
        const result = parseNJSONContent(content);
        expect(result).toHaveLength(2);
    });

    test('filters out invalid lines', () => {
        const content = `{"benchmark": "test1", "score": 100}
invalid line
{"benchmark": "test2", "score": 200}`;
        const result = parseNJSONContent(content);
        expect(result).toHaveLength(2);
    });
});

describe('convertNJSONToVisualizationFormat', () => {
    const njsonItem = {
        benchmark: "org.sample.Test.testNfp",
        src: "NFP",
        score: 1000.5,
        error: "NaN",
        units: "ops/s",
        size: 100,
        ts: "2025-06-09T10:00:00Z",
        comment: "test run"
    };

    test('converts NJSON object to visualization format', () => {
        const result = convertNJSONToVisualizationFormat([njsonItem]);
        expect(result).toHaveLength(1);
        expect(result[0]).toEqual({
            benchmark: "org.sample.Test.testNfp",
            primaryMetric: {
                score: 1000.5,
                scoreError: 0,
                scoreUnit: "ops/s"
            },
            ts: "2025-06-09T10:00:00Z",
            size: 100,
            src: "NFP",
            comment: "test run"
        });
    });

    test('handles string input', () => {
        const njsonString = JSON.stringify(njsonItem);
        const result = convertNJSONToVisualizationFormat(njsonString);
        expect(result).toHaveLength(1);
        expect(result[0].src).toBe("NFP");
    });

    test('converts numeric error values', () => {
        const itemWithError = { ...njsonItem, error: "100.5" };
        const result = convertNJSONToVisualizationFormat([itemWithError]);
        expect(result[0].primaryMetric.scoreError).toBe(100.5);
    });
});

describe('extractTestType', () => {
    test('extracts test type from benchmark name', () => {
        expect(extractTestType('org.sample.SimpleBenchmark.jdkSerialize')).toBe('Simple');
        expect(extractTestType('org.sample.TreeBenchmark.treeNfp')).toBe('Tree');
        expect(extractTestType('org.sample.Test.testProtobuf')).toBe('test');
    });

    test('removes common suffixes', () => {
        expect(extractTestType('org.sample.Test.testJdk')).toBe('test');
        expect(extractTestType('org.sample.Test.testNfp')).toBe('test');
        expect(extractTestType('org.sample.Test.testRead')).toBe('test');
        expect(extractTestType('org.sample.Test.testWrite')).toBe('test');
        expect(extractTestType('org.sample.Test.testRoundTrip')).toBe('test');
    });
});

describe('groupDataByTestAndSource', () => {
    const testData = [
        { benchmark: 'org.Test.test1Nfp', src: 'NFP', primaryMetric: { score: 100 } },
        { benchmark: 'org.Test.test1Jdk', src: 'JDK', primaryMetric: { score: 200 } },
        { benchmark: 'org.Test.test2Nfp', src: 'NFP', primaryMetric: { score: 300 } }
    ];

    test('groups data by test type and source', () => {
        const result = groupDataByTestAndSource(testData);
        expect(result.test1).toBeDefined();
        expect(result.test1.NFP).toHaveLength(1);
        expect(result.test1.JDK).toHaveLength(1);
        expect(result.test2).toBeDefined();
        expect(result.test2.NFP).toHaveLength(1);
    });
});

describe('calculateAverageScore', () => {
    test('calculates average score', () => {
        const items = [
            { primaryMetric: { score: 100 } },
            { primaryMetric: { score: 200 } },
            { primaryMetric: { score: 300 } }
        ];
        expect(calculateAverageScore(items)).toBe(200);
    });

    test('returns 0 for empty array', () => {
        expect(calculateAverageScore([])).toBe(0);
        expect(calculateAverageScore(null)).toBe(0);
    });
});

describe('findBestPerformer', () => {
    test('finds item with highest score', () => {
        const data = [
            { primaryMetric: { score: 100 }, src: 'NFP' },
            { primaryMetric: { score: 300 }, src: 'JDK' },
            { primaryMetric: { score: 200 }, src: 'PTB' }
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