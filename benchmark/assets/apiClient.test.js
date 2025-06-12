// Mock fetch for testing
global.fetch = jest.fn();

const { 
    searchJMHFiles, 
    loadJMHResults, 
    loadSpecificJMHFile,
    loadLatestSizes,
    loadServerInfo
} = require('./apiClient');

describe('API Client', () => {
    beforeEach(() => {
        fetch.mockClear();
    });

    describe('searchJMHFiles', () => {
        test('should call search endpoint for JMH files', async () => {
            const mockResponse = ['jmh-result-20250612_143651.json', 'jmh-result-20250611_213824.json'];
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            const result = await searchJMHFiles();

            expect(fetch).toHaveBeenCalledWith('/api/search?type=jmh');
            expect(result).toEqual(mockResponse);
        });

        test('should call search endpoint with query for JMH files', async () => {
            const mockResponse = ['jmh-result-20250612_143651.json'];
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            const result = await searchJMHFiles('20250612');

            expect(fetch).toHaveBeenCalledWith('/api/search?type=jmh&q=20250612');
            expect(result).toEqual(mockResponse);
        });

        test('should handle URL encoding in query', async () => {
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve([])
            });

            await searchJMHFiles('test file');

            expect(fetch).toHaveBeenCalledWith('/api/search?type=jmh&q=test%20file');
        });

        test('should throw error on failed request', async () => {
            fetch.mockResolvedValueOnce({
                ok: false,
                status: 404,
                statusText: 'Not Found'
            });

            await expect(searchJMHFiles()).rejects.toThrow('HTTP 404: Not Found');
        });
    });

    describe('loadJMHResults', () => {
        test('should load latest JMH results with filename', async () => {
            const mockResponse = {
                filename: 'jmh-result-20250612_143651.json',
                data: [
                    {
                        benchmark: 'org.sample.SimpleWriteBenchmark.nfp',
                        primaryMetric: {
                            score: 1676220.0,
                            scoreError: 123.45,
                            scoreUnit: 'ops/s'
                        }
                    }
                ]
            };
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            const result = await loadJMHResults();

            expect(fetch).toHaveBeenCalledWith('/api/jmh-results');
            expect(result).toEqual(mockResponse);
            expect(result.filename).toBe('jmh-result-20250612_143651.json');
            expect(result.data).toHaveLength(1);
            expect(result.data[0].benchmark).toBe('org.sample.SimpleWriteBenchmark.nfp');
        });

        test('should throw error on failed request', async () => {
            fetch.mockResolvedValueOnce({
                ok: false,
                status: 500,
                statusText: 'Internal Server Error'
            });

            await expect(loadJMHResults()).rejects.toThrow('HTTP 500: Internal Server Error');
        });
    });

    describe('loadSpecificJMHFile', () => {
        test('should load specific JMH file by name', async () => {
            const mockResponse = {
                filename: 'jmh-result-20250611_213824.json',
                data: [
                    {
                        benchmark: 'org.sample.PaxosBenchmark.jdk',
                        primaryMetric: {
                            score: 31579.0,
                            scoreError: 456.78,
                            scoreUnit: 'ops/s'
                        }
                    }
                ]
            };
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            const result = await loadSpecificJMHFile('jmh-result-20250611_213824.json');

            expect(fetch).toHaveBeenCalledWith('/api/jmh-file?name=jmh-result-20250611_213824.json');
            expect(result).toEqual(mockResponse);
        });

        test('should handle URL encoding in filename', async () => {
            const mockResponse = {
                filename: 'jmh-result-test file.json',
                data: []
            };
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            await loadSpecificJMHFile('jmh-result-test file.json');

            expect(fetch).toHaveBeenCalledWith('/api/jmh-file?name=jmh-result-test%20file.json');
        });

        test('should throw error on failed request', async () => {
            fetch.mockResolvedValueOnce({
                ok: false,
                status: 404,
                statusText: 'File Not Found'
            });

            await expect(loadSpecificJMHFile('nonexistent.json')).rejects.toThrow('HTTP 404: File Not Found');
        });
    });

    describe('loadLatestSizes', () => {
        test('should load latest sizes data', async () => {
            const mockResponse = {
                filename: 'sizes-20250612_143651.json',
                data: {
                    "SimpleWrite": {"NFP": 71, "JDK": 1026, "PTB": 32},
                    "Paxos": {"NFP": 89, "JDK": 1690, "PTB": 45}
                }
            };
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            const result = await loadLatestSizes();

            expect(fetch).toHaveBeenCalledWith('/api/sizes');
            expect(result).toEqual(mockResponse);
            expect(result.data.SimpleWrite.NFP).toBe(71);
            expect(result.data.Paxos.JDK).toBe(1690);
        });

        test('should throw error on failed request', async () => {
            fetch.mockResolvedValueOnce({
                ok: false,
                status: 404,
                statusText: 'No sizes file found'
            });

            await expect(loadLatestSizes()).rejects.toThrow('HTTP 404: No sizes file found');
        });
    });

    describe('loadServerInfo', () => {
        test('should load server information', async () => {
            const mockResponse = `Server Info:
ServerInfo[server=NFP Benchmark Visualizer, version=2.0, startTime=2025-01-06T14:30:00Z, dataDir=., assetsDir=assets]
Uptime: PT2H30M

Endpoints:
/ GET - Serves static files (index.html, CSS, JS). Main UI interface.
/api/jmh-results GET - Returns latest JMH results with filename
/api/sizes GET - Returns latest sizes data
/info GET - Returns server configuration, status, and API documentation`;

            fetch.mockResolvedValueOnce({
                ok: true,
                text: () => Promise.resolve(mockResponse)
            });

            const result = await loadServerInfo();

            expect(fetch).toHaveBeenCalledWith('/info');
            expect(result).toBe(mockResponse);
        });

        test('should throw error on failed request', async () => {
            fetch.mockResolvedValueOnce({
                ok: false,
                status: 500,
                statusText: 'Server Error'
            });

            await expect(loadServerInfo()).rejects.toThrow('HTTP 500: Server Error');
        });
    });
});