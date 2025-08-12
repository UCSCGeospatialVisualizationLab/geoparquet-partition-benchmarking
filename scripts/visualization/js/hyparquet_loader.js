// Hyparquet streaming loader and orchestration

/**
 * Main test function for Hyparquet streaming strategy
 */
async function testHyparquetStrategy(strategy, bbox) {
    console.log(`Starting Hyparquet streaming test for strategy: ${strategy}`);
    
    // Initialize streaming metrics
    streamingMetrics.startStreaming();
    
    try {
        // Step 1: Execute streaming strategy
        const streamResults = await HyparquetStrategies[strategy](bbox);
        streamingMetrics.takeMemorySnapshot('streaming_completed');
        
        // Step 2: Combine results from multiple streams (if applicable)
        const combinedResult = await combineStreamResults(streamResults, strategy);
        streamingMetrics.takeMemorySnapshot('results_combined');
        
        // Step 3: Generate comprehensive performance report
        const performanceReport = streamingMetrics.generateStreamingReport();
        
        // Step 4: Merge strategy results with performance metrics
        const finalResult = mergeStreamingResults(combinedResult, performanceReport, strategy);
        
        console.log(`Hyparquet test ${strategy} completed:`, finalResult);
        return finalResult;
        
    } catch (error) {
        console.error(`Hyparquet test ${strategy} failed:`, error);
        streamingMetrics.recordStreamingEvent('test_failed', error.message, { strategy, error: error.message });
        throw error;
    }
}

/**
 * Combine results from multiple concurrent streams
 */
async function combineStreamResults(streamResults, strategy) {
    const combineStart = performance.now();
    
    // Handle single result vs array of results
    const results = Array.isArray(streamResults) ? streamResults.filter(r => r !== null) : [streamResults];
    
    if (results.length === 0) {
        throw new Error(`No successful streams for strategy: ${strategy}`);
    }
    
    // Aggregate metrics across all streams
    const combined = {
        strategy,
        streamCount: results.length,
        totalStreamingTime: Math.max(...results.map(r => r.streamingTime)),
        totalBytesStreamed: results.reduce((sum, r) => sum + r.totalBytesStreamed, 0),
        totalRowsCollected: results.reduce((sum, r) => sum + r.rowsCollected, 0),
        totalRowGroupsStreamed: results.reduce((sum, r) => sum + r.rowGroupsProcessed, 0),
        totalBatchesProcessed: results.reduce((sum, r) => sum + r.batchesProcessed, 0),
        earlyTermination: results.some(r => r.earlyTermination),
        avgStreamEfficiency: results.reduce((sum, r) => sum + r.streamEfficiency, 0) / results.length,
        avgDataTransferRate: results.reduce((sum, r) => sum + r.dataTransferRate, 0) / results.length,
        streamResults: results,
        combiningTime: performance.now() - combineStart
    };
    
    streamingMetrics.recordStreamingEvent('results_combined', `Combined ${results.length} streams`, combined);
    
    return combined;
}

/**
 * Merge streaming results with detailed performance metrics
 */
function mergeStreamingResults(streamResults, performanceMetrics, strategy) {
    const timeToFirstBatch = performanceMetrics.timeToFirstBatch;
    
    return {
        // Core identification
        strategy,
        testType: 'hyparquet_streaming',
        timestamp: performanceMetrics.timestamp,
        
        // Primary streaming metrics (for UI display)
        streamingTime: streamResults.totalStreamingTime,
        timeToFirstBatch,
        rowGroupsStreamed: streamResults.totalRowGroupsStreamed,
        batchesProcessed: streamResults.totalBatchesProcessed,
        totalBytesStreamed: streamResults.totalBytesStreamed,
        rowsCollected: streamResults.totalRowsCollected,
        streamEfficiency: streamResults.avgStreamEfficiency,
        earlyTermination: streamResults.earlyTermination,
        
        // Performance insights
        dataTransferRate: streamResults.avgDataTransferRate,
        avgRowsPerBatch: performanceMetrics.avgRowsPerBatch,
        avgProcessingRate: performanceMetrics.avgProcessingRate,
        
        // Memory metrics
        memoryPeak: performanceMetrics.memoryMetrics.peakUsage,
        memoryGrowth: performanceMetrics.memoryMetrics.growth,
        
        // Stream details
        streamCount: streamResults.streamCount,
        streamDetails: streamResults.streamResults,
        
        // Full metrics for detailed analysis
        detailedMetrics: {
            streamingRequests: performanceMetrics.streamingRequests,
            rowGroupBatches: performanceMetrics.rowGroupBatches,
            streamingEvents: performanceMetrics.streamingEvents,
            rowCollectionEvents: performanceMetrics.rowCollectionEvents,
            memorySnapshots: performanceMetrics.memoryMetrics.snapshots
        }
    };
}

/**
 * Simulate Hyparquet-like streaming behavior
 * In a real implementation, this would use actual Hyparquet APIs
 */
async function simulateHyparquetStreaming(url, options = {}) {
    const {
        maxRowGroups = 10,
        targetRows = 1000,
        chunkSize = 64 * 1024, // 64KB chunks
        progressCallback = null
    } = options;

    console.log(`Simulating Hyparquet streaming for: ${url}`);
    
    try {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const contentLength = parseInt(response.headers.get('content-length')) || 0;
        const reader = response.body.getReader();
        
        let totalBytesRead = 0;
        let rowGroupsProcessed = 0;
        let rowsCollected = 0;
        let chunks = [];
        
        while (true) {
            const { done, value } = await reader.read();
            
            if (done) break;
            
            chunks.push(value);
            totalBytesRead += value.length;
            
            // Process when we have enough data
            if (totalBytesRead >= chunkSize) {
                const combinedChunk = combineUint8Arrays(chunks);
                
                // Simulate row group processing
                const { rows, filteredRows } = await processRowGroup(combinedChunk);
                rowsCollected += filteredRows;
                rowGroupsProcessed++;
                
                // Progress callback
                if (progressCallback) {
                    progressCallback({
                        bytesRead: totalBytesRead,
                        totalBytes: contentLength,
                        rowGroupsProcessed,
                        rowsCollected,
                        progress: contentLength > 0 ? (totalBytesRead / contentLength) * 100 : 0
                    });
                }
                
                // Early termination check
                if (rowsCollected >= targetRows || rowGroupsProcessed >= maxRowGroups) {
                    reader.releaseLock();
                    break;
                }
                
                // Reset for next chunk
                chunks = [];
                totalBytesRead = 0;
            }
        }
        
        return {
            success: true,
            totalBytesRead,
            rowGroupsProcessed,
            rowsCollected,
            earlyTermination: rowsCollected >= targetRows || rowGroupsProcessed >= maxRowGroups
        };
        
    } catch (error) {
        console.error('Hyparquet streaming simulation failed:', error);
        throw error;
    }
}

/**
 * Simulate processing a row group chunk
 */
async function processRowGroup(chunkData) {
    // Simulate processing delay based on chunk size
    const processingTime = Math.min(chunkData.length / 1024 / 1024 * 5, 25); // 5ms per MB, max 25ms
    await new Promise(resolve => setTimeout(resolve, processingTime));
    
    // Simulate row extraction and filtering
    const estimatedRows = Math.floor(chunkData.length / 180); // ~180 bytes per row
    const filteredRows = Math.floor(estimatedRows * 0.12); // ~12% match Bay Area filter
    
    return {
        rows: estimatedRows,
        filteredRows,
        processingTime,
        chunkSize: chunkData.length
    };
}

/**
 * Combine multiple Uint8Array chunks
 */
function combineUint8Arrays(arrays) {
    const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
    const combined = new Uint8Array(totalLength);
    
    let offset = 0;
    for (const array of arrays) {
        combined.set(array, offset);
        offset += array.length;
    }
    
    return combined;
}

/**
 * Advanced streaming configuration based on strategy
 */
function getAdvancedStreamingConfig(strategy, bbox) {
    const baseConfig = HyparquetStrategies.getStreamingConfig(strategy);
    
    // Strategy-specific optimizations
    const optimizations = {
        no_partition: {
            useRangeRequests: true,
            preferredChunkSize: 128 * 1024, // 128KB chunks for large file
            earlyTerminationThreshold: 2000,
            concurrentStreams: 1
        },
        attribute_state: {
            useRangeRequests: false,
            preferredChunkSize: 64 * 1024, // 64KB chunks for medium file
            earlyTerminationThreshold: 1500,
            concurrentStreams: 1
        },
        spatial_h3_l3: {
            useRangeRequests: false,
            preferredChunkSize: 32 * 1024, // 32KB chunks for small files
            earlyTerminationThreshold: 400,
            concurrentStreams: 4 // Process hexagons in parallel
        },
        hybrid_state_h3: {
            useRangeRequests: false,
            preferredChunkSize: 48 * 1024, // 48KB chunks for hybrid files
            earlyTerminationThreshold: 500,
            concurrentStreams: 3
        }
    };
    
    return {
        ...baseConfig,
        ...optimizations[strategy],
        bbox,
        strategy
    };
}

/**
 * Streaming progress tracker for UI updates
 */
class StreamingProgressTracker {
    constructor(strategy, totalExpectedStreams = 1) {
        this.strategy = strategy;
        this.totalExpectedStreams = totalExpectedStreams;
        this.completedStreams = 0;
        this.activeStreams = new Map();
        this.startTime = performance.now();
    }
    
    startStream(streamId, description) {
        this.activeStreams.set(streamId, {
            description,
            startTime: performance.now(),
            bytesProcessed: 0,
            rowsCollected: 0
        });
        
        this.updateUI();
    }
    
    updateStream(streamId, progress) {
        if (this.activeStreams.has(streamId)) {
            const stream = this.activeStreams.get(streamId);
            Object.assign(stream, progress);
            this.updateUI();
        }
    }
    
    completeStream(streamId) {
        if (this.activeStreams.has(streamId)) {
            this.activeStreams.delete(streamId);
            this.completedStreams++;
            this.updateUI();
        }
    }
    
    updateUI() {
        const totalProgress = (this.completedStreams / this.totalExpectedStreams) * 100;
        
        // Update main progress bar if available
        if (typeof updateProgress === 'function') {
            updateProgress(totalProgress);
        }
        
        // Update status with streaming details
        const activeStreamCount = this.activeStreams.size;
        const totalRows = Array.from(this.activeStreams.values())
            .reduce((sum, stream) => sum + (stream.rowsCollected || 0), 0);
        
        const statusMessage = `Streaming ${this.strategy}: ${activeStreamCount} active, ${totalRows} rows collected`;
        
        if (typeof updateStatus === 'function') {
            updateStatus(statusMessage);
        }
    }
}

// Make test function available globally
window.testHyparquetStrategy = testHyparquetStrategy;

// Error handling for streaming operations
function handleStreamingError(error, context) {
    console.error(`Hyparquet streaming error in ${context}:`, error);
    
    streamingMetrics.recordStreamingEvent('error', `Streaming error: ${error.message}`, {
        context,
        error: error.message,
        stack: error.stack
    });
    
    // Common streaming errors and their meanings
    if (error.message.includes('net::ERR_INSUFFICIENT_RESOURCES')) {
        throw new Error(`Insufficient browser resources for streaming: ${error.message}`);
    } else if (error.message.includes('Content-Range')) {
        throw new Error(`Range request not supported: ${error.message}`);
    } else if (error.message.includes('CORS')) {
        throw new Error(`CORS policy prevents streaming: ${error.message}`);
    } else {
        throw new Error(`Hyparquet streaming failed in ${context}: ${error.message}`);
    }
}

// Export for ES module usage
export { 
    testHyparquetStrategy, 
    combineStreamResults, 
    mergeStreamingResults, 
    simulateHyparquetStreaming,
    StreamingProgressTracker,
    handleStreamingError 
};
