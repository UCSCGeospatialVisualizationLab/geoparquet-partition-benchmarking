// Performance metrics collection utilities for Arrow.js client

class PerformanceMetrics {
    constructor() {
        this.reset();
    }

    reset() {
        this.startTime = null;
        this.httpRequests = [];
        this.arrowOperations = [];
        this.memorySnapshots = [];
        this.filterOperations = [];
    }

    startTest() {
        this.reset();
        this.startTime = performance.now();
        this.takeMemorySnapshot('test_start');
    }

    recordHttpRequest(url, startTime, endTime, bytesTransferred, ttfb = null) {
        this.httpRequests.push({
            url,
            startTime,
            endTime,
            duration: endTime - startTime,
            bytesTransferred,
            ttfb: ttfb || (endTime - startTime),
            timestamp: startTime
        });
    }

    recordArrowOperation(operation, startTime, endTime, rowCount = 0, details = {}) {
        this.arrowOperations.push({
            operation,
            startTime,
            endTime,
            duration: endTime - startTime,
            rowCount,
            details,
            timestamp: startTime
        });
    }

    recordFilterOperation(filterType, inputRows, outputRows, startTime, endTime) {
        this.filterOperations.push({
            filterType,
            inputRows,
            outputRows,
            startTime,
            endTime,
            duration: endTime - startTime,
            efficiency: outputRows / inputRows,
            timestamp: startTime
        });
    }

    takeMemorySnapshot(label) {
        if (performance.memory) {
            this.memorySnapshots.push({
                label,
                timestamp: performance.now(),
                usedJSHeapSize: performance.memory.usedJSHeapSize,
                totalJSHeapSize: performance.memory.totalJSHeapSize,
                jsHeapSizeLimit: performance.memory.jsHeapSizeLimit
            });
        } else {
            // Fallback for browsers without performance.memory
            this.memorySnapshots.push({
                label,
                timestamp: performance.now(),
                usedJSHeapSize: 0,
                totalJSHeapSize: 0,
                jsHeapSizeLimit: 0,
                note: 'performance.memory not available'
            });
        }
    }

    generateReport() {
        const endTime = performance.now();
        const totalTime = endTime - this.startTime;

        // Calculate HTTP metrics
        const totalBytesDownloaded = this.httpRequests.reduce((sum, req) => sum + req.bytesTransferred, 0);
        const avgTTFB = this.httpRequests.length > 0 
            ? this.httpRequests.reduce((sum, req) => sum + req.ttfb, 0) / this.httpRequests.length
            : 0;
        const httpRequestCount = this.httpRequests.length;

        // Calculate Arrow processing metrics
        const totalArrowTime = this.arrowOperations.reduce((sum, op) => sum + op.duration, 0);
        const totalRowsProcessed = this.arrowOperations.reduce((sum, op) => sum + op.rowCount, 0);

        // Calculate filter efficiency
        const totalRowsFiltered = this.filterOperations.reduce((sum, op) => sum + op.outputRows, 0);
        const filterEfficiency = this.filterOperations.length > 0
            ? this.filterOperations.reduce((sum, op) => sum + op.efficiency, 0) / this.filterOperations.length
            : 0;

        // Memory metrics
        const memoryPeak = this.memorySnapshots.length > 0 
            ? Math.max(...this.memorySnapshots.map(s => s.usedJSHeapSize))
            : 0;
        const memoryGrowth = this.memorySnapshots.length >= 2
            ? this.memorySnapshots[this.memorySnapshots.length - 1].usedJSHeapSize - this.memorySnapshots[0].usedJSHeapSize
            : 0;

        return {
            totalTime,
            httpMetrics: {
                requestCount: httpRequestCount,
                totalBytes: totalBytesDownloaded,
                avgTTFB,
                totalHttpTime: this.httpRequests.reduce((sum, req) => sum + req.duration, 0),
                requests: this.httpRequests
            },
            arrowMetrics: {
                totalProcessingTime: totalArrowTime,
                totalRowsProcessed,
                operations: this.arrowOperations
            },
            filterMetrics: {
                totalRowsFiltered,
                avgEfficiency: filterEfficiency,
                operations: this.filterOperations
            },
            memoryMetrics: {
                peakUsage: memoryPeak,
                growth: memoryGrowth,
                snapshots: this.memorySnapshots
            },
            // Derived metrics
            httpRequestCount,
            totalBytesDownloaded,
            avgTTFB,
            arrowParseTime: totalArrowTime,
            rowsExtracted: totalRowsFiltered,
            memoryPeak,
            efficiency: totalBytesDownloaded > 0 ? totalRowsFiltered / totalBytesDownloaded * 1000 : 0, // rows per KB
            timestamp: new Date().toISOString()
        };
    }
}

// Utility functions for performance measurement
function measureAsync(fn, label) {
    return async function(...args) {
        const start = performance.now();
        const result = await fn.apply(this, args);
        const end = performance.now();
        console.log(`${label}: ${(end - start).toFixed(2)}ms`);
        return result;
    };
}

function measureSync(fn, label) {
    return function(...args) {
        const start = performance.now();
        const result = fn.apply(this, args);
        const end = performance.now();
        console.log(`${label}: ${(end - start).toFixed(2)}ms`);
        return result;
    };
}

// Global performance metrics instance
window.perfMetrics = new PerformanceMetrics();
