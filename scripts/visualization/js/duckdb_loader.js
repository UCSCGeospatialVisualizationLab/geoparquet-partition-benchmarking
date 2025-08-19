// DuckDB-WASM query orchestration and performance testing

/**
 * Main test function for DuckDB-WASM strategy
 */
async function testDuckDBStrategy(strategy, bbox) {
    console.log(`Starting DuckDB-WASM test for strategy: ${strategy}`);
    
    // Initialize range request metrics
    rangeMetrics.startQuerySession();
    
    try {
        // Step 1: Ensure DuckDB connection and configuration
        await ensureDuckDBConnection();
        rangeMetrics.takeMemorySnapshot('connection_ready');
        
        // Step 2: Execute strategy-specific query
        const queryResult = await executeDuckDBQuery(strategy, bbox);
        rangeMetrics.takeMemorySnapshot('query_completed');
        
        // Step 3: Process and analyze results
        const processedResult = await processQueryResults(queryResult, strategy);
        rangeMetrics.takeMemorySnapshot('results_processed');
        
        // Step 4: Generate comprehensive performance report
        const performanceReport = rangeMetrics.generateRangeRequestReport();
        
        // Step 5: Merge query results with performance metrics
        const finalResult = mergeDuckDBResults(processedResult, performanceReport, strategy);
        
        console.log(`DuckDB test ${strategy} completed:`, finalResult);
        return finalResult;
        
    } catch (error) {
        console.error(`DuckDB test ${strategy} failed:`, error);
        rangeMetrics.recordQueryEvent('test_failed', error.message, { strategy, error: error.message });
        throw error;
    }
}

/**
 * Ensure DuckDB connection is ready and configured
 */
async function ensureDuckDBConnection() {
    if (!window.connection) {
        throw new Error('DuckDB connection not available. Please refresh page.');
    }
    
    const connectionStart = performance.now();
    
    try {
        // Test connection with simple query
        await window.connection.query('SELECT 1 as test');
        
        // Ensure S3 and spatial configuration
        await DuckDBStrategies.configureDuckDBForS3(window.connection);
        
        const connectionEnd = performance.now();
        rangeMetrics.recordConnectionTime(connectionStart, connectionEnd, {
            configurationType: 's3_spatial'
        });
        
        console.log('DuckDB connection verified and configured');
        return true;
        
    } catch (error) {
        const connectionEnd = performance.now();
        rangeMetrics.recordConnectionTime(connectionStart, connectionEnd, {
            error: error.message
        });
        throw new Error(`DuckDB connection failed: ${error.message}`);
    }
}

/**
 * Execute DuckDB query for specific strategy
 */
async function executeDuckDBQuery(strategy, bbox) {
    console.log(`Executing DuckDB query for strategy: ${strategy}`);
    
    try {
        // Execute strategy with performance monitoring
        const result = await DuckDBStrategies.executeStrategyQuery(
            window.connection, 
            strategy, 
            bbox, 
            rangeMetrics
        );
        
        return result;
        
    } catch (error) {
        rangeMetrics.recordQueryEvent('query_execution_failed', `${strategy} execution failed: ${error.message}`);
        throw error;
    }
}

/**
 * Process query results and extract metadata
 */
async function processQueryResults(queryResult, strategy) {
    const processingStart = performance.now();
    
    try {
        const { result, querySpec, rowCount, queryTime } = queryResult;
        
        // Extract some sample data for validation
        const sampleData = [];
        if (result && result.toArray) {
            const rows = result.toArray();
            sampleData.push(...rows.slice(0, 5)); // First 5 rows as sample
        }
        
        // Analyze result metadata
        const resultAnalysis = {
            strategy,
            rowCount,
            queryTime,
            queryType: querySpec.queryType,
            sqlQuery: querySpec.sql.trim(),
            description: querySpec.description,
            sampleData,
            processingTime: performance.now() - processingStart
        };
        
        rangeMetrics.recordQueryEvent('results_processed', `Processed ${rowCount} rows from ${strategy}`, resultAnalysis);
        
        console.log(`Results processed for ${strategy}: ${rowCount} rows`);
        return resultAnalysis;
        
    } catch (error) {
        rangeMetrics.recordQueryEvent('result_processing_failed', `Result processing failed: ${error.message}`);
        throw new Error(`Result processing failed: ${error.message}`);
    }
}

/**
 * Merge DuckDB results with detailed performance metrics
 */
function mergeDuckDBResults(queryResults, performanceMetrics, strategy) {
    return {
        // Core identification
        strategy,
        testType: 'duckdb_wasm_range_requests',
        timestamp: performanceMetrics.timestamp,
        
        // Primary query metrics (for UI display)
        queryTime: queryResults.queryTime,
        connectionTime: performanceMetrics.connectionTime,
        httpRequestCount: performanceMetrics.httpRequestCount,
        rangeRequests: performanceMetrics.rangeRequests,
        bytesTransferred: performanceMetrics.totalBytesTransferred,
        rowsReturned: queryResults.rowCount,
        
        // Query details
        sqlQuery: queryResults.sqlQuery,
        queryType: queryResults.queryType,
        queryEfficiency: performanceMetrics.queryEfficiency,
        predicatePushdown: performanceMetrics.predicatePushdown,
        
        // Performance insights
        avgHttpDuration: performanceMetrics.avgHttpDuration,
        networkEfficiency: performanceMetrics.networkEfficiency,
        avgRangeSize: performanceMetrics.avgRangeSize,
        
        // Memory metrics
        memoryPeak: performanceMetrics.memoryMetrics.peakUsage,
        memoryGrowth: performanceMetrics.memoryMetrics.growth,
        
        // Sample data for validation
        sampleData: queryResults.sampleData.slice(0, 3), // Limit to 3 for space
        
        // Full metrics for detailed analysis
        detailedMetrics: {
            httpRequests: performanceMetrics.httpRequests,
            rangeRequests: performanceMetrics.rangeRequests,
            sqlQueries: performanceMetrics.sqlQueries,
            queryEvents: performanceMetrics.queryEvents,
            networkEvents: performanceMetrics.networkEvents,
            memorySnapshots: performanceMetrics.memoryMetrics.snapshots,
            networkUtilization: rangeMetrics.getNetworkUtilization()
        }
    };
}

/**
 * Query result validator
 */
function validateQueryResults(result, strategy, expectedRowRange) {
    const rowCount = result.rowCount;
    const validation = {
        strategy,
        rowCount,
        isValid: true,
        warnings: [],
        errors: []
    };
    
    // Row count validation
    if (rowCount === 0) {
        validation.warnings.push('No rows returned - check spatial filter');
    } else if (rowCount < 100) {
        validation.warnings.push(`Low row count (${rowCount}) - spatial filter may be too restrictive`);
    }
    
    // Sample data validation
    if (result.sampleData && result.sampleData.length > 0) {
        const sample = result.sampleData[0];
        
        // Check for expected columns
        const expectedColumns = ['StateAbbr', 'Tract', 'centroid'];
        for (const col of expectedColumns) {
            if (!(col in sample)) {
                validation.errors.push(`Missing expected column: ${col}`);
                validation.isValid = false;
            }
        }
        
        // Check for California data
        if (sample.StateAbbr !== 'CA') {
            validation.warnings.push(`Expected CA data, found: ${sample.StateAbbr}`);
        }
    }
    
    rangeMetrics.recordQueryEvent('results_validated', `Validation ${validation.isValid ? 'passed' : 'failed'}`, validation);
    
    return validation;
}

/**
 * Error handling for DuckDB operations
 */
function handleDuckDBError(error, context) {
    console.error(`DuckDB error in ${context}:`, error);
    
    rangeMetrics.recordQueryEvent('error', `DuckDB error: ${error.message}`, {
        context,
        error: error.message,
        stack: error.stack
    });
    
    // Common DuckDB errors and their meanings
    if (error.message.includes('Connection failed')) {
        throw new Error(`Database connection lost: ${error.message}`);
    } else if (error.message.includes('Invalid S3 configuration')) {
        throw new Error(`S3 access configuration error: ${error.message}`);
    } else if (error.message.includes('Spatial extension not loaded')) {
        throw new Error(`Spatial functions not available: ${error.message}`);
    } else if (error.message.includes('HTTP')) {
        throw new Error(`Network error during query execution: ${error.message}`);
    } else {
        throw new Error(`DuckDB query failed in ${context}: ${error.message}`);
    }
}

/**
 * Query performance analyzer
 */
class DuckDBQueryAnalyzer {
    constructor(strategy) {
        this.strategy = strategy;
        this.baseline = null;
    }
    
    analyzePerformance(result) {
        const analysis = {
            strategy: this.strategy,
            queryTime: result.queryTime,
            bytesPerMs: result.bytesTransferred / result.queryTime,
            rowsPerMs: result.rowsReturned / result.queryTime,
            rangeRequestEfficiency: this.calculateRangeEfficiency(result),
            networkUtilization: this.calculateNetworkUtilization(result)
        };
        
        // Compare with baseline if available
        if (this.baseline) {
            analysis.comparison = {
                timeImprovement: ((this.baseline.queryTime - result.queryTime) / this.baseline.queryTime) * 100,
                efficiencyGain: result.queryEfficiency - this.baseline.queryEfficiency
            };
        }
        
        this.baseline = result;
        return analysis;
    }
    
    calculateRangeEfficiency(result) {
        if (result.rangeRequests === 0) return 0;
        
        const totalRangeBytes = result.detailedMetrics.rangeRequests
            .reduce((sum, req) => sum + req.bytesTransferred, 0);
        const totalHttpBytes = result.bytesTransferred;
        
        return totalHttpBytes > 0 ? (totalRangeBytes / totalHttpBytes) * 100 : 0;
    }
    
    calculateNetworkUtilization(result) {
        const utilization = result.detailedMetrics.networkUtilization;
        return {
            concurrentRequests: utilization.maxConcurrentRequests,
            parallelEfficiency: Math.min(utilization.maxConcurrentRequests / 4, 1) * 100, // Assuming 4 threads
            sequentialGaps: utilization.sequentialGaps
        };
    }
}

// Make test function available globally
window.testDuckDBStrategy = testDuckDBStrategy;

// Export for ES module usage
export { 
    testDuckDBStrategy,
    executeDuckDBQuery,
    processQueryResults,
    mergeDuckDBResults,
    validateQueryResults,
    DuckDBQueryAnalyzer,
    handleDuckDBError
};
