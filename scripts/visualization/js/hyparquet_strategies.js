// Hyparquet streaming strategies for different S3 partition layouts

const HyparquetStrategies = {
    
    /**
     * NO_PARTITION: Stream single large file with early termination
     * Focus on California data, terminate when sufficient rows collected
     */
    async no_partition(bbox) {
        const url = `${BASE_URL}/no_partition/hazus_CensusTract.parquet`;
        
        return await this._streamSingleFile(url, bbox, {
            targetRows: 2000, // Stop after collecting ~2000 CA records
            maxRowGroups: 10,  // Don't process more than 10 row groups
            description: 'Single file with early termination'
        });
    },

    /**
     * ATTRIBUTE_STATE: Stream California partition
     * Should be efficient since data is pre-filtered by state
     */
    async attribute_state(bbox) {
        const url = `${BASE_URL}/attribute_state/StateAbbr=CA/data_0.parquet`;
        
        return await this._streamSingleFile(url, bbox, {
            targetRows: 1500, // Expected fewer rows in CA partition
            maxRowGroups: 5,   // Smaller file, fewer row groups
            description: 'Pre-filtered California partition'
        });
    },

    /**
     * SPATIAL_H3_L3: Stream multiple H3 hexagons covering Bay Area
     * Process files in parallel streams for comparison
     */
    async spatial_h3_l3(bbox) {
        const bayAreaH3Hexagons = [
            '832830fffffffff', // Bay Area hexagon (confirmed available)
            '832834fffffffff'  // Bay Area hexagon (confirmed available) 
        ];
        
        const streamPromises = [];
        const results = [];
        
        for (const hex of bayAreaH3Hexagons) {
            const url = `${BASE_URL}/spatial_h3_l3/h3_level3=${hex}/data_0.parquet`;
            
            const streamPromise = this._streamSingleFile(url, bbox, {
                targetRows: 400,   // Fewer rows expected per hex
                maxRowGroups: 3,   // Small spatial files
                description: `H3 hexagon ${hex}`,
                hexagonId: hex
            }).catch(error => {
                console.warn(`H3 hex ${hex} not available:`, error.message);
                return null; // Continue with other hexagons
            });
            
            streamPromises.push(streamPromise);
        }
        
        const streamResults = await Promise.allSettled(streamPromises);
        
        // Combine results from successful streams
        for (const result of streamResults) {
            if (result.status === 'fulfilled' && result.value) {
                results.push(result.value);
            }
        }
        
        return results;
    },

    /**
     * HYBRID_STATE_H3: Stream CA state with H3 sub-partitions
     * Similar to spatial but within state structure
     */
    async hybrid_state_h3(bbox) {
        const bayAreaH3Hexagons = [
            '832830fffffffff', // Bay Area hexagon (confirmed available)
            '832834fffffffff'  // Bay Area hexagon (confirmed available)
        ];
        
        const streamPromises = [];
        const results = [];
        
        for (const hex of bayAreaH3Hexagons) {
            const url = `${BASE_URL}/hybrid_state_h3/StateAbbr=CA/h3_level3=${hex}/data_0.parquet`;
            
            const streamPromise = this._streamSingleFile(url, bbox, {
                targetRows: 500,   // Medium-sized partitions
                maxRowGroups: 4,   
                description: `Hybrid CA/${hex}`,
                hexagonId: hex
            }).catch(error => {
                console.warn(`Hybrid partition CA/${hex} not available:`, error.message);
                return null;
            });
            
            streamPromises.push(streamPromise);
        }
        
        const streamResults = await Promise.allSettled(streamPromises);
        
        for (const result of streamResults) {
            if (result.status === 'fulfilled' && result.value) {
                results.push(result.value);
            }
        }
        
        return results;
    },

    /**
     * Stream a single parquet file using Hyparquet with incremental processing
     */
    async _streamSingleFile(url, bbox, options = {}) {
        const {
            targetRows = 1000,
            maxRowGroups = 10,
            description = 'Streaming file',
            hexagonId = null
        } = options;

        console.log(`Starting Hyparquet stream: ${description}`);
        streamingMetrics.recordStreamingEvent('stream_start', `Initiating stream for ${url}`, options);

        const streamStart = performance.now();
        let totalBytesStreamed = 0;
        let rowsCollected = 0;
        let rowGroupsProcessed = 0;
        let batchesProcessed = 0;
        let earlyTermination = false;

        try {
            // Start HTTP request
            const httpStart = performance.now();
            const response = await fetch(url);
            const httpEnd = performance.now();
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const contentLength = parseInt(response.headers.get('content-length')) || 0;
            streamingMetrics.recordStreamingRequest(url, httpStart, httpEnd, contentLength);

            // Get response as readable stream
            const reader = response.body.getReader();
            const chunks = [];
            let bytesReceived = 0;

            // Read stream in chunks
            while (true) {
                const { done, value } = await reader.read();
                
                if (done) break;

                chunks.push(value);
                bytesReceived += value.length;
                totalBytesStreamed += value.length;

                // Record first batch timing
                if (chunks.length === 1) {
                    streamingMetrics.recordFirstBatch();
                }

                // Process chunk if we have enough data or this is the final chunk
                if (bytesReceived > 64 * 1024 || done) { // Process every 64KB or at end
                    const batchStart = performance.now();
                    
                    // Combine chunks for processing
                    const combinedArray = new Uint8Array(bytesReceived);
                    let offset = 0;
                    for (const chunk of chunks) {
                        combinedArray.set(chunk, offset);
                        offset += chunk.length;
                    }

                    // Simulate Hyparquet streaming processing
                    const { rowsInBatch, filteredRows } = await this._processStreamChunk(
                        combinedArray, 
                        bbox, 
                        batchesProcessed
                    );

                    const batchEnd = performance.now();
                    
                    streamingMetrics.recordRowGroupBatch(
                        batchesProcessed, 
                        rowsInBatch, 
                        batchStart, 
                        batchEnd, 
                        { filteredRows, url, hexagonId }
                    );

                    rowsCollected += filteredRows;
                    batchesProcessed++;
                    rowGroupsProcessed++;

                    // Record row collection
                    const efficiency = rowsInBatch > 0 ? (filteredRows / rowsInBatch) * 100 : 0;
                    streamingMetrics.recordRowCollection(filteredRows, 'bbox_filter', efficiency);

                    // Check for early termination conditions
                    if (rowsCollected >= targetRows || rowGroupsProcessed >= maxRowGroups) {
                        const reason = rowsCollected >= targetRows ? 'target_rows_reached' : 'max_row_groups_reached';
                        streamingMetrics.recordEarlyTermination(reason, targetRows, rowsCollected);
                        earlyTermination = true;
                        reader.releaseLock();
                        break;
                    }

                    // Clear processed chunks
                    chunks.length = 0;
                    bytesReceived = 0;

                    // Take memory snapshot periodically
                    if (batchesProcessed % 3 === 0) {
                        streamingMetrics.takeMemorySnapshot(`batch_${batchesProcessed}`);
                    }
                }
            }

            reader.releaseLock();

        } catch (error) {
            streamingMetrics.recordStreamingEvent('stream_error', `Stream failed: ${error.message}`, { url, error: error.message });
            throw error;
        }

        const streamEnd = performance.now();
        const streamDuration = streamEnd - streamStart;

        const result = {
            url,
            description,
            hexagonId,
            streamingTime: streamDuration,
            totalBytesStreamed,
            rowsCollected,
            rowGroupsProcessed,
            batchesProcessed,
            earlyTermination,
            streamEfficiency: earlyTermination ? ((rowsCollected / targetRows) * 100) : 100,
            dataTransferRate: totalBytesStreamed / streamDuration * 1000, // bytes per second
            timestamp: new Date().toISOString()
        };

        console.log(`Hyparquet stream completed: ${description}`, result);
        return result;
    },

    /**
     * Process a stream chunk using mock Hyparquet-like behavior
     * In a real implementation, this would use actual Hyparquet APIs
     */
    async _processStreamChunk(chunkData, bbox, batchIndex) {
        // Simulate processing time based on chunk size
        const processingDelay = Math.min(chunkData.length / 1024 / 1024 * 10, 50); // 10ms per MB, max 50ms
        await new Promise(resolve => setTimeout(resolve, processingDelay));

        // Simulate row processing
        const estimatedRows = Math.floor(chunkData.length / 200); // ~200 bytes per row estimate
        const bayAreaRows = Math.floor(estimatedRows * 0.15); // ~15% in Bay Area (higher than 10% for CA filter)

        return {
            rowsInBatch: estimatedRows,
            filteredRows: bayAreaRows,
            processingTime: processingDelay,
            chunkSize: chunkData.length
        };
    },

    /**
     * Get streaming configuration for a strategy
     */
    getStreamingConfig(strategy) {
        const configs = {
            no_partition: {
                expectedFiles: 1,
                targetRowsPerFile: 2000,
                maxRowGroupsPerFile: 10,
                description: 'Single large file streaming'
            },
            attribute_state: {
                expectedFiles: 1,
                targetRowsPerFile: 1500,
                maxRowGroupsPerFile: 5,
                description: 'State-filtered partition streaming'
            },
            spatial_h3_l3: {
                expectedFiles: 4,
                targetRowsPerFile: 400,
                maxRowGroupsPerFile: 3,
                description: 'Multiple spatial partition streaming'
            },
            hybrid_state_h3: {
                expectedFiles: 3,
                targetRowsPerFile: 500,
                maxRowGroupsPerFile: 4,
                description: 'Hybrid partition streaming'
            }
        };
        
        return configs[strategy] || configs.no_partition;
    }
};

// Make strategies available globally
window.HyparquetStrategies = HyparquetStrategies;

// Bbox utilities for streaming
const StreamingBboxUtils = {
    /**
     * Check if coordinates fall within streaming bbox
     */
    isInStreamingBbox(lon, lat, bbox) {
        return lon >= bbox.west && lon <= bbox.east && 
               lat >= bbox.south && lat <= bbox.north;
    },

    /**
     * Estimate if a chunk likely contains Bay Area data
     * Used for streaming optimization decisions
     */
    chunkLikelyContainsBayAreaData(chunkMetadata) {
        // In a real implementation, this would check row group metadata
        // For testing, we'll simulate based on chunk position and size
        return Math.random() > 0.7; // 30% of chunks likely contain Bay Area data
    }
};

// Export for ES module usage
export { HyparquetStrategies, StreamingBboxUtils };
