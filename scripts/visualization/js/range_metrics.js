// HTTP Range Request and SQL query performance metrics for DuckDB-WASM client

class RangeRequestMetrics {
    constructor() {
        this.reset();
    }

    reset() {
        this.startTime = null;
        this.connectionTime = null;
        this.httpRequests = [];
        this.rangeRequests = [];
        this.sqlQueries = [];
        this.memorySnapshots = [];
        this.queryEvents = [];
        this.networkEvents = [];
    }

    startQuerySession() {
        this.reset();
        this.startTime = performance.now();
        this.takeMemorySnapshot('query_session_start');
        this.recordQueryEvent('session_started', 'DuckDB query session initiated');
    }

    recordConnectionTime(startTime, endTime, details = {}) {
        this.connectionTime = endTime - startTime;
        this.recordQueryEvent('connection_established', `DuckDB connection: ${this.connectionTime.toFixed(2)}ms`, {
            connectionTime: this.connectionTime,
            ...details
        });
    }

    recordHttpRequest(url, method, startTime, endTime, bytesTransferred, status = 200, headers = {}) {
        const request = {
            url,
            method,
            startTime,
            endTime,
            duration: endTime - startTime,
            bytesTransferred,
            status,
            headers,
            timestamp: startTime,
            requestId: this.httpRequests.length
        };
        
        this.httpRequests.push(request);
        this.recordNetworkEvent('http_request', `${method} ${url} - ${status}`, request);
    }

    recordRangeRequest(url, rangeStart, rangeEnd, startTime, endTime, bytesTransferred, metadata = {}) {
        const rangeRequest = {
            url,
            rangeStart,
            rangeEnd,
            rangeSize: rangeEnd - rangeStart + 1,
            startTime,
            endTime,
            duration: endTime - startTime,
            bytesTransferred,
            efficiency: bytesTransferred / (rangeEnd - rangeStart + 1),
            metadata,
            timestamp: startTime,
            requestId: this.rangeRequests.length
        };
        
        this.rangeRequests.push(rangeRequest);
        this.recordNetworkEvent('range_request', `Range ${rangeStart}-${rangeEnd} (${bytesTransferred} bytes)`, rangeRequest);
    }

    recordSQLQuery(query, startTime, endTime, rowsReturned, queryPlan = null, metadata = {}) {
        const sqlQuery = {
            query,
            startTime,
            endTime,
            duration: endTime - startTime,
            rowsReturned,
            queryPlan,
            metadata,
            timestamp: startTime,
            queryId: this.sqlQueries.length
        };
        
        this.sqlQueries.push(sqlQuery);
        this.recordQueryEvent('sql_executed', `Query returned ${rowsReturned} rows in ${(endTime - startTime).toFixed(2)}ms`, sqlQuery);
    }

    recordQueryEvent(eventType, description, details = {}) {
        this.queryEvents.push({
            eventType,
            description,
            timestamp: performance.now(),
            details
        });
    }

    recordNetworkEvent(eventType, description, details = {}) {
        this.networkEvents.push({
            eventType,
            description,
            timestamp: performance.now(),
            details
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

    generateRangeRequestReport() {
        const endTime = performance.now();
        const totalQueryTime = endTime - this.startTime;

        // HTTP request metrics
        const totalHttpRequests = this.httpRequests.length;
        const totalBytesTransferred = this.httpRequests.reduce((sum, req) => sum + req.bytesTransferred, 0);
        const avgHttpDuration = totalHttpRequests > 0 
            ? this.httpRequests.reduce((sum, req) => sum + req.duration, 0) / totalHttpRequests
            : 0;

        // Range request metrics
        const totalRangeRequests = this.rangeRequests.length;
        const totalRangeBytes = this.rangeRequests.reduce((sum, req) => sum + req.bytesTransferred, 0);
        const avgRangeEfficiency = totalRangeRequests > 0
            ? this.rangeRequests.reduce((sum, req) => sum + req.efficiency, 0) / totalRangeRequests
            : 0;
        const avgRangeSize = totalRangeRequests > 0
            ? this.rangeRequests.reduce((sum, req) => sum + req.rangeSize, 0) / totalRangeRequests
            : 0;

        // SQL query metrics
        const totalSqlQueries = this.sqlQueries.length;
        const totalRowsReturned = this.sqlQueries.reduce((sum, query) => sum + query.rowsReturned, 0);
        const avgQueryDuration = totalSqlQueries > 0
            ? this.sqlQueries.reduce((sum, query) => sum + query.duration, 0) / totalSqlQueries
            : 0;

        // Memory metrics
        const memoryPeak = this.memorySnapshots.length > 0 
            ? Math.max(...this.memorySnapshots.map(s => s.usedJSHeapSize))
            : 0;
        const memoryGrowth = this.memorySnapshots.length >= 2
            ? this.memorySnapshots[this.memorySnapshots.length - 1].usedJSHeapSize - this.memorySnapshots[0].usedJSHeapSize
            : 0;

        // Query efficiency calculations
        const queryEfficiency = this.calculateQueryEfficiency();
        const predicatePushdown = this.detectPredicatePushdown();
        const networkEfficiency = totalBytesTransferred > 0 
            ? (totalRangeBytes / totalBytesTransferred) * 100 
            : 0;

        return {
            // Core timing metrics
            totalQueryTime,
            connectionTime: this.connectionTime,
            
            // HTTP request metrics
            httpRequestCount: totalHttpRequests,
            totalBytesTransferred,
            avgHttpDuration,
            
            // Range request metrics
            rangeRequests: totalRangeRequests,
            totalRangeBytes,
            avgRangeEfficiency,
            avgRangeSize,
            
            // SQL query metrics
            sqlQueries: totalSqlQueries,
            totalRowsReturned,
            avgQueryDuration,
            
            // Efficiency metrics
            queryEfficiency,
            predicatePushdown,
            networkEfficiency,
            
            // Memory metrics
            memoryMetrics: {
                peakUsage: memoryPeak,
                growth: memoryGrowth,
                snapshots: this.memorySnapshots
            },
            
            // Detailed events for analysis
            httpRequests: this.httpRequests,
            rangeRequests: this.rangeRequests,
            sqlQueries: this.sqlQueries,
            queryEvents: this.queryEvents,
            networkEvents: this.networkEvents,
            
            // Summary metadata
            timestamp: new Date().toISOString(),
            testType: 'duckdb_wasm_range_requests'
        };
    }

    calculateQueryEfficiency() {
        // Calculate efficiency based on data selectivity and network usage
        if (this.sqlQueries.length === 0) return 0;
        
        const totalQueryTime = this.sqlQueries.reduce((sum, q) => sum + q.duration, 0);
        const totalRowsReturned = this.sqlQueries.reduce((sum, q) => sum + q.rowsReturned, 0);
        
        // Efficiency metric: rows per millisecond per MB transferred
        const totalMBTransferred = this.httpRequests.reduce((sum, req) => sum + req.bytesTransferred, 0) / 1024 / 1024;
        
        if (totalMBTransferred === 0 || totalQueryTime === 0) return 0;
        
        const efficiency = (totalRowsReturned / totalQueryTime) * (1 / totalMBTransferred) * 1000;
        return Math.min(efficiency * 10, 100); // Scale to 0-100%
    }

    detectPredicatePushdown() {
        // Analyze query patterns and range requests to detect pushdown
        const hasRangeRequests = this.rangeRequests.length > 0;
        const hasSpatialQueries = this.sqlQueries.some(q => 
            q.query.toLowerCase().includes('st_') || 
            q.query.toLowerCase().includes('where')
        );
        const hasSelectiveRanges = this.rangeRequests.some(r => r.rangeSize < 1024 * 1024); // < 1MB ranges
        
        return hasRangeRequests && hasSpatialQueries && hasSelectiveRanges;
    }

    getNetworkUtilization() {
        // Calculate network utilization patterns
        const requests = [...this.httpRequests, ...this.rangeRequests];
        if (requests.length === 0) return { concurrent: 0, sequential: 0 };
        
        requests.sort((a, b) => a.startTime - b.startTime);
        
        let concurrentRequests = 0;
        let maxConcurrent = 0;
        let sequentialGaps = 0;
        
        for (let i = 0; i < requests.length; i++) {
            const current = requests[i];
            
            // Check for concurrent requests
            const concurrent = requests.filter(r => 
                r.startTime <= current.endTime && 
                r.endTime >= current.startTime &&
                r !== current
            ).length;
            
            maxConcurrent = Math.max(maxConcurrent, concurrent + 1);
            
            // Check for sequential gaps
            if (i > 0) {
                const prev = requests[i - 1];
                if (current.startTime > prev.endTime) {
                    sequentialGaps++;
                }
            }
        }
        
        return {
            maxConcurrentRequests: maxConcurrent,
            sequentialGaps,
            avgRequestDuration: requests.reduce((sum, r) => sum + r.duration, 0) / requests.length
        };
    }
}

// Utility functions for DuckDB range request measurement
function measureSQLQuery(connection, sql, label, metrics) {
    return async function(sql, ...args) {
        const start = performance.now();
        metrics.recordQueryEvent('query_start', `Starting ${label}: ${sql.substring(0, 100)}...`);
        
        const result = await connection.query(sql, ...args);
        
        const end = performance.now();
        const rowCount = result?.numRows || 0;
        
        metrics.recordSQLQuery(sql, start, end, rowCount, null, { label });
        console.log(`${label}: ${(end - start).toFixed(2)}ms, ${rowCount} rows`);
        
        return result;
    };
}

function trackRangeRequestPattern(urlPattern, strategy, metrics) {
    // Monitor fetch requests for range patterns
    const originalFetch = window.fetch;
    let requestCount = 0;
    
    window.fetch = async function(resource, options = {}) {
        const resourceUrl = typeof resource === 'string' ? resource : resource.url;
        
        if (resourceUrl && resourceUrl.includes(urlPattern)) {
            const requestStart = performance.now();
            const requestId = requestCount++;
            
            // Check for range headers
            const headers = options.headers || {};
            const rangeHeader = headers.Range || headers.range;
            
            try {
                const response = await originalFetch(resource, options);
                const requestEnd = performance.now();
                
                const contentLength = parseInt(response.headers.get('content-length') || '0');
                const contentRange = response.headers.get('content-range') || '';
                const status = response.status;
                
                if (rangeHeader || contentRange) {
                    // Parse range header or content-range response
                    let rangeStart = 0, rangeEnd = contentLength - 1;
                    
                    if (rangeHeader) {
                        const rangeMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);
                        if (rangeMatch) {
                            rangeStart = parseInt(rangeMatch[1]);
                            rangeEnd = rangeMatch[2] ? parseInt(rangeMatch[2]) : contentLength - 1;
                        }
                    } else if (contentRange) {
                        const rangeMatch = contentRange.match(/bytes (\d+)-(\d+)\/(\d+)/);
                        if (rangeMatch) {
                            rangeStart = parseInt(rangeMatch[1]);
                            rangeEnd = parseInt(rangeMatch[2]);
                        }
                    }
                    
                    metrics.recordRangeRequest(
                        resourceUrl, 
                        rangeStart, 
                        rangeEnd, 
                        requestStart, 
                        requestEnd, 
                        contentLength,
                        { strategy, requestId, status }
                    );
                } else {
                    metrics.recordHttpRequest(
                        resourceUrl, 
                        options.method || 'GET', 
                        requestStart, 
                        requestEnd, 
                        contentLength, 
                        status,
                        Object.fromEntries(response.headers.entries())
                    );
                }
                
                return response;
            } catch (error) {
                const requestEnd = performance.now();
                metrics.recordHttpRequest(
                    resourceUrl, 
                    options.method || 'GET', 
                    requestStart, 
                    requestEnd, 
                    0, 
                    500,
                    { error: error.message }
                );
                throw error;
            }
        }
        
        return originalFetch(resource, options);
    };
}

// Global range request metrics instance
window.rangeMetrics = new RangeRequestMetrics();

// Export for ES module usage
export { 
    RangeRequestMetrics, 
    measureSQLQuery, 
    trackRangeRequestPattern 
};
