// Streaming performance metrics collection for Hyparquet client

class StreamingMetrics {
    constructor() {
        this.reset();
    }

    reset() {
        this.startTime = null;
        this.firstBatchTime = null;
        this.streamingRequests = [];
        this.rowGroupBatches = [];
        this.memorySnapshots = [];
        this.streamingEvents = [];
        this.rowCollectionEvents = [];
        this.terminationEvent = null;
    }

    startStreaming() {
        this.reset();
        this.startTime = performance.now();
        this.takeMemorySnapshot('streaming_start');
        this.recordStreamingEvent('stream_initiated', 'Streaming process started');
    }

    recordFirstBatch() {
        if (!this.firstBatchTime) {
            this.firstBatchTime = performance.now();
            this.recordStreamingEvent('first_batch', 'First row group batch received');
        }
    }

    recordStreamingRequest(url, startTime, endTime, bytesStreamed, rowGroupIndex = null) {
        const request = {
            url,
            startTime,
            endTime,
            duration: endTime - startTime,
            bytesStreamed,
            rowGroupIndex,
            timestamp: startTime,
            requestId: this.streamingRequests.length
        };
        
        this.streamingRequests.push(request);
        this.recordStreamingEvent('http_request', `Row group ${rowGroupIndex || 'unknown'} streamed`, request);
    }

    recordRowGroupBatch(batchIndex, rowsInBatch, startTime, endTime, metadata = {}) {
        const batch = {
            batchIndex,
            rowsInBatch,
            startTime,
            endTime,
            duration: endTime - startTime,
            processingRate: rowsInBatch / (endTime - startTime) * 1000, // rows per second
            timestamp: startTime,
            metadata
        };
        
        this.rowGroupBatches.push(batch);
        this.recordStreamingEvent('batch_processed', `Batch ${batchIndex}: ${rowsInBatch} rows`, batch);
    }

    recordRowCollection(rowsCollected, filterCondition, efficiency) {
        const collection = {
            rowsCollected,
            filterCondition,
            efficiency,
            timestamp: performance.now(),
            cumulativeRows: this.getTotalRowsCollected() + rowsCollected
        };
        
        this.rowCollectionEvents.push(collection);
        this.recordStreamingEvent('rows_collected', `Collected ${rowsCollected} rows (${efficiency.toFixed(1)}% efficiency)`, collection);
    }

    recordEarlyTermination(reason, targetRowCount, actualRowCount) {
        this.terminationEvent = {
            reason,
            targetRowCount,
            actualRowCount,
            timestamp: performance.now(),
            efficiencyGain: ((actualRowCount / targetRowCount) * 100).toFixed(1)
        };
        
        this.recordStreamingEvent('early_termination', `Terminated early: ${reason}`, this.terminationEvent);
    }

    recordStreamingEvent(eventType, description, details = {}) {
        this.streamingEvents.push({
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

    getTotalRowsCollected() {
        return this.rowCollectionEvents.reduce((sum, event) => sum + event.rowsCollected, 0);
    }

    generateStreamingReport() {
        const endTime = performance.now();
        const totalStreamingTime = endTime - this.startTime;
        const timeToFirstBatch = this.firstBatchTime ? this.firstBatchTime - this.startTime : 0;

        // Calculate streaming metrics
        const totalBytesStreamed = this.streamingRequests.reduce((sum, req) => sum + req.bytesStreamed, 0);
        const rowGroupsStreamed = this.streamingRequests.length;
        const batchesProcessed = this.rowGroupBatches.length;
        const rowsCollected = this.getTotalRowsCollected();

        // Calculate streaming efficiency (early termination benefit)
        const streamEfficiency = this.terminationEvent 
            ? parseFloat(this.terminationEvent.efficiencyGain)
            : (batchesProcessed > 0 ? 100 : 0);

        // Memory metrics
        const memoryPeak = this.memorySnapshots.length > 0 
            ? Math.max(...this.memorySnapshots.map(s => s.usedJSHeapSize))
            : 0;
        const memoryGrowth = this.memorySnapshots.length >= 2
            ? this.memorySnapshots[this.memorySnapshots.length - 1].usedJSHeapSize - this.memorySnapshots[0].usedJSHeapSize
            : 0;

        // Row processing metrics
        const avgRowsPerBatch = batchesProcessed > 0 
            ? this.rowGroupBatches.reduce((sum, batch) => sum + batch.rowsInBatch, 0) / batchesProcessed
            : 0;
        const avgProcessingRate = batchesProcessed > 0
            ? this.rowGroupBatches.reduce((sum, batch) => sum + batch.processingRate, 0) / batchesProcessed
            : 0;

        return {
            // Core timing metrics
            streamingTime: totalStreamingTime,
            timeToFirstBatch,
            
            // Streaming volume metrics
            rowGroupsStreamed,
            batchesProcessed,
            totalBytesStreamed,
            rowsCollected,
            
            // Efficiency metrics
            streamEfficiency,
            earlyTermination: !!this.terminationEvent,
            terminationReason: this.terminationEvent?.reason || null,
            
            // Performance metrics
            avgRowsPerBatch,
            avgProcessingRate,
            dataTransferRate: totalBytesStreamed / totalStreamingTime * 1000, // bytes per second
            
            // Memory metrics
            memoryMetrics: {
                peakUsage: memoryPeak,
                growth: memoryGrowth,
                snapshots: this.memorySnapshots
            },
            
            // Detailed events for analysis
            streamingRequests: this.streamingRequests,
            rowGroupBatches: this.rowGroupBatches,
            streamingEvents: this.streamingEvents,
            rowCollectionEvents: this.rowCollectionEvents,
            
            // Summary metadata
            timestamp: new Date().toISOString(),
            testType: 'hyparquet_streaming'
        };
    }
}

// Utility functions for streaming measurement
function measureStreamingAsync(fn, label, metrics) {
    return async function(...args) {
        const start = performance.now();
        metrics.recordStreamingEvent('function_start', `Starting ${label}`);
        
        const result = await fn.apply(this, args);
        
        const end = performance.now();
        metrics.recordStreamingEvent('function_end', `Completed ${label}: ${(end - start).toFixed(2)}ms`);
        console.log(`${label}: ${(end - start).toFixed(2)}ms`);
        return result;
    };
}

function trackRowGroupProgress(currentBatch, totalBatches, rowsInBatch, metrics) {
    const progress = (currentBatch / totalBatches) * 100;
    metrics.recordStreamingEvent('progress_update', `Batch ${currentBatch}/${totalBatches} (${progress.toFixed(1)}%)`, {
        currentBatch,
        totalBatches,
        progress,
        rowsInBatch
    });
    
    // Update UI progress if available
    if (typeof updateProgress === 'function') {
        updateProgress(progress);
    }
}

// Global streaming metrics instance
window.streamingMetrics = new StreamingMetrics();

// Export for ES module usage
export { StreamingMetrics, measureStreamingAsync, trackRowGroupProgress };
