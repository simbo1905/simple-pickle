// Mock fetch for testing
global.fetch = jest.fn();

const { searchFiles, loadResults, loadSpecificFile } = require('./apiClient');

describe('API Client', () => {
    beforeEach(() => {
        fetch.mockClear();
    });

    describe('searchFiles', () => {
        test('should call search endpoint without query', async () => {
            const mockResponse = ['results-20250609_143651.njson', 'results-20250603_213824.njson'];
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            const result = await searchFiles();

            expect(fetch).toHaveBeenCalledWith('/api/search');
            expect(result).toEqual(mockResponse);
        });

        test('should call search endpoint with query', async () => {
            const mockResponse = ['results-20250609_143651.njson'];
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            const result = await searchFiles('20250609');

            expect(fetch).toHaveBeenCalledWith('/api/search?q=20250609');
            expect(result).toEqual(mockResponse);
        });

        test('should handle URL encoding in query', async () => {
            const mockResponse = ['results-test file.njson'];
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            await searchFiles('test file');

            expect(fetch).toHaveBeenCalledWith('/api/search?q=test%20file');
        });

        test('should throw error on failed request', async () => {
            fetch.mockResolvedValueOnce({
                ok: false,
                status: 404,
                statusText: 'Not Found'
            });

            await expect(searchFiles()).rejects.toThrow('HTTP 404: Not Found');
        });
    });

    describe('loadResults', () => {
        test('should load latest results with filename', async () => {
            const mockResponse = {
                filename: 'results-20250609_143651.njson',
                data: [{ benchmark: 'test', score: 100 }]
            };
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            const result = await loadResults();

            expect(fetch).toHaveBeenCalledWith('/api/results');
            expect(result).toEqual(mockResponse);
            expect(result.filename).toBe('results-20250609_143651.njson');
            expect(result.data).toHaveLength(1);
        });

        test('should throw error on failed request', async () => {
            fetch.mockResolvedValueOnce({
                ok: false,
                status: 500,
                statusText: 'Internal Server Error'
            });

            await expect(loadResults()).rejects.toThrow('HTTP 500: Internal Server Error');
        });
    });

    describe('loadSpecificFile', () => {
        test('should load specific file by name', async () => {
            const mockResponse = {
                filename: 'results-20250603_213824.njson',
                data: [{ benchmark: 'test2', score: 200 }]
            };
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            const result = await loadSpecificFile('results-20250603_213824.njson');

            expect(fetch).toHaveBeenCalledWith('/api/file?name=results-20250603_213824.njson');
            expect(result).toEqual(mockResponse);
        });

        test('should handle URL encoding in filename', async () => {
            const mockResponse = {
                filename: 'results-test file.njson',
                data: []
            };
            fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockResponse)
            });

            await loadSpecificFile('results-test file.njson');

            expect(fetch).toHaveBeenCalledWith('/api/file?name=results-test%20file.njson');
        });

        test('should throw error on failed request', async () => {
            fetch.mockResolvedValueOnce({
                ok: false,
                status: 404,
                statusText: 'File Not Found'
            });

            await expect(loadSpecificFile('nonexistent.njson')).rejects.toThrow('HTTP 404: File Not Found');
        });
    });
});