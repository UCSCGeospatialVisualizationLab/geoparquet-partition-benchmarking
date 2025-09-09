// Optimized Hyparquet streaming loader with Hysnappy decompression
// Clean, efficient approach focused on performance

/**
 * Main test function for Hyparquet streaming strategy (optimized)
 */
async function testHyparquetStrategy(strategy, bbox) {
    console.log(`Starting optimized Hyparquet streaming test for strategy: ${strategy}`);
    
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
        // Aggregate decompression metrics
        totalDecompressionMetrics: results.reduce((acc, r) => {
            const metrics = r.decompressionMetrics || {};
            return {
                totalDecompressedBytes: acc.totalDecompressedBytes + (metrics.totalDecompressedBytes || 0),
                decompressionTime: acc.decompressionTime + (metrics.decompressionTime || 0),
                compressedChunks: acc.compressedChunks + (metrics.compressedChunks || 0)
            };
        }, { totalDecompressedBytes: 0, decompressionTime: 0, compressedChunks: 0 }),
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
        testType: 'hyparquet_hysnappy_streaming_optimized',
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
        
        // Decompression metrics (optimized Hysnappy integration)
        decompressionMetrics: streamResults.totalDecompressionMetrics || {},
        totalDecompressedBytes: streamResults.totalDecompressionMetrics?.totalDecompressedBytes || 0,
        decompressionTime: streamResults.totalDecompressionMetrics?.decompressionTime || 0,
        
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
 * Get Snappy decompression function from globally initialized hysnappy
 * Libraries are initialized once on page load for performance
 */
function getSnappyDecompressor() {
    const hysnappy = window.hysnappy;
    if (!hysnappy) {
        console.warn('Hysnappy not available - using basic hyparquet without decompression');
        return null;
    }
    
    const decompress = hysnappy.uncompress || hysnappy.decompress || hysnappy.decode;
    if (!decompress) {
        console.warn('No Snappy decompression function found in hysnappy');
        return null;
    }
    
    return decompress;
}

/**
 * Optimized Hyparquet streaming with Hysnappy decompression
 * Simple, efficient approach focused on performance
 */
async function hyparquetReadWithBbox(url, bbox, { targetRows, maxRowGroups }) {
    // Check if libraries are initialized
    if (!window.globalLibrariesInitialized) {
        throw new Error('Libraries not initialized - wait for page load');
    }

    const hy = window.hyparquet;
    const snappyDecompress = getSnappyDecompressor();
    
    const startTime = performance.now();
    let totalBytesStreamed = 0;
    let decompressionMetrics = {
        totalDecompressedBytes: 0,
        decompressionTime: 0,
        compressedChunks: 0
    };

    console.log(`Starting optimized Hyparquet streaming: ${url}`);
    streamingMetrics.recordStreamingEvent('optimized_stream_start', 'Starting efficient streaming', { 
        url, targetRows, maxRowGroups, hasSnappy: !!snappyDecompress 
    });

    try {
        // Simple approach: fetch file and use hyparquet's best available API
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const arrayBuffer = await response.arrayBuffer();
        totalBytesStreamed = arrayBuffer.byteLength;
        
        console.log(`Downloaded ${totalBytesStreamed} bytes, starting Hyparquet parsing...`);

        // Use the most straightforward hyparquet API path
        let table = null;
        const parseStart = performance.now();
        
        if (hy.parquetRead && typeof hy.parquetRead === 'function') {
            const options = { file: arrayBuffer };
            if (snappyDecompress) {
                options.snappy = snappyDecompress;
                console.log('Using Hyparquet with Hysnappy decompression');
            }
            table = await hy.parquetRead(options);
        } else if (hy.readParquet && typeof hy.readParquet === 'function') {
            // Alternative API
            console.log('Using fallback Hyparquet API');
            table = await hy.readParquet(arrayBuffer);
        } else {
            throw new Error('No compatible Hyparquet API found');
        }
        
        const parseEnd = performance.now();
        console.log(`Hyparquet parsing completed in ${(parseEnd - parseStart).toFixed(2)}ms`);
        
        return await processHyparquetTable(table, bbox, { 
            targetRows, 
            maxRowGroups, 
            totalBytesStreamed, 
            decompressionMetrics,
            url 
        });

    } catch (error) {
        streamingMetrics.recordStreamingEvent('optimized_stream_error', 'Streaming failed', { 
            url, error: error.message 
        });
        throw error;
    }
}

/**
 * Process Hyparquet table with optimized geometry filtering
 * Clean, efficient approach focused on performance
 */
async function processHyparquetTable(table, bbox, options) {
    const { targetRows, maxRowGroups, totalBytesStreamed, decompressionMetrics, url } = options;
    
    let rowsCollected = 0;
    let batchesProcessed = 0;
    let firstBatchRecorded = false;
    
    console.log('Processing Hyparquet table for bbox filtering...');
    const processStart = performance.now();

    try {
        const acc = getGeometryAccessors(table);
        if (!acc || !acc.length) {
            console.warn('No geometry data found in Hyparquet table');
            return { 
                totalBytesStreamed, 
                rowsCollected: 0, 
                rowGroupsProcessed: 1, 
                batchesProcessed: 1, 
                earlyTermination: false,
                decompressionMetrics
            };
        }

        console.log(`Processing ${acc.length} rows from Hyparquet table...`);
        if (!firstBatchRecorded) { 
            streamingMetrics.recordFirstBatch(); 
            firstBatchRecorded = true; 
        }

        const batchSize = 512;
        let currentBatch = 0;
        let batchStart = performance.now();
        
        for (let i = 0; i < acc.length; i++) {
            const geom = acc.getGeomAt(i);
            currentBatch++;
            
            if (geom) {
                try {
                    const env = wkbEnvelope(geom);
                    if (envIntersectsBbox(env, bbox)) {
                        rowsCollected++;
                    }
                } catch (geomError) {
                    // Skip invalid geometry
                    console.warn(`Invalid geometry at row ${i}:`, geomError.message);
                }
            }

            if (currentBatch >= batchSize || rowsCollected >= targetRows) {
                const batchEnd = performance.now();
                streamingMetrics.recordRowGroupBatch(batchesProcessed, currentBatch, batchStart, batchEnd, { 
                    filteredRows: rowsCollected, 
                    url, 
                    totalRows: i + 1 
                });
                batchesProcessed++;
                currentBatch = 0;
                batchStart = performance.now();
                
                // Early termination if we have enough rows
                if (rowsCollected >= targetRows) {
                    console.log(`Early termination: collected ${rowsCollected} rows (target: ${targetRows})`);
                    break;
                }
            }

            if ((i + 1) % 1000 === 0) {
                streamingMetrics.takeMemorySnapshot(`processing_row_${i + 1}`);
            }
        }

        // Process any remaining batch
        if (currentBatch > 0) {
            const batchEnd = performance.now();
            streamingMetrics.recordRowGroupBatch(batchesProcessed, currentBatch, batchStart, batchEnd, { 
                filteredRows: rowsCollected, 
                url, 
                completed: true 
            });
            batchesProcessed++;
        }

        const processEnd = performance.now();
        console.log(`Processed ${acc.length} rows, found ${rowsCollected} in bbox (${(processEnd - processStart).toFixed(2)}ms)`);

        return {
            totalBytesStreamed,
            rowsCollected,
            rowGroupsProcessed: Math.max(1, batchesProcessed), // Simulate row groups from batches
            batchesProcessed,
            earlyTermination: rowsCollected >= targetRows,
            decompressionMetrics
        };

    } catch (error) {
        streamingMetrics.recordStreamingEvent('table_processing_error', 'Failed to process table', { 
            error: error.message, url 
        });
        throw error;
    }
}

/**
 * Compute a simple axis-aligned bounding box for common WKB geometry types
 * Supported: Point(1), LineString(2), Polygon(3), MultiPolygon(6)
 */
function wkbEnvelope(wkbUint8) {
    const dv = new DataView(wkbUint8.buffer, wkbUint8.byteOffset, wkbUint8.byteLength);
    function readGeometry(offset) {
        const byteOrder = dv.getUint8(offset); offset += 1;
        const littleEndian = byteOrder === 1;
        const type = dv.getUint32(offset, littleEndian); offset += 4;

        function initEnv() {
            return { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
        }
        function addPoint(env, x, y) {
            if (x < env.minX) env.minX = x;
            if (y < env.minY) env.minY = y;
            if (x > env.maxX) env.maxX = x;
            if (y > env.maxY) env.maxY = y;
        }
        function readPoint(off) {
            const x = dv.getFloat64(off, littleEndian); off += 8;
            const y = dv.getFloat64(off, littleEndian); off += 8;
            return { off, x, y };
        }

        if (type === 1) { // Point
            const { off, x, y } = readPoint(offset);
            const env = initEnv();
            addPoint(env, x, y);
            return { off, env };
        }
        if (type === 2) { // LineString
            const n = dv.getUint32(offset, littleEndian); offset += 4;
            const env = initEnv();
            for (let i = 0; i < n; i++) {
                const p = readPoint(offset); offset = p.off; addPoint(env, p.x, p.y);
            }
            return { off: offset, env };
        }
        if (type === 3) { // Polygon
            const rings = dv.getUint32(offset, littleEndian); offset += 4;
            const env = initEnv();
            for (let r = 0; r < rings; r++) {
                const n = dv.getUint32(offset, littleEndian); offset += 4;
                for (let i = 0; i < n; i++) {
                    const p = readPoint(offset); offset = p.off; addPoint(env, p.x, p.y);
                }
            }
            return { off: offset, env };
        }
        if (type === 6) { // MultiPolygon
            const n = dv.getUint32(offset, littleEndian); offset += 4;
            const env = initEnv();
            for (let i = 0; i < n; i++) {
                const sub = readGeometry(offset);
                offset = sub.off;
                addPoint(env, sub.env.minX, sub.env.minY);
                addPoint(env, sub.env.maxX, sub.env.maxY);
            }
            return { off: offset, env };
        }

        // Unsupported type: return empty env
        return { off: offset, env: { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity } };
    }
    return readGeometry(0).env;
}

function envIntersectsBbox(env, bbox) {
    return !(env.maxX < bbox.west || env.minX > bbox.east || env.maxY < bbox.south || env.minY > bbox.north);
}

function toUint8(value) {
    if (!value) return null;
    if (value instanceof Uint8Array) return value;
    if (value instanceof ArrayBuffer) return new Uint8Array(value);
    if (ArrayBuffer.isView(value) && value.buffer) return new Uint8Array(value.buffer, value.byteOffset || 0, value.byteLength || value.length || undefined);
    try {
        // Some libs return { buffer, byteOffset, byteLength }
        if (value.buffer && typeof value.byteLength === 'number') {
            return new Uint8Array(value.buffer, value.byteOffset || 0, value.byteLength);
        }
    } catch (_) { /* ignore */ }
    return null;
}

// Try to discover geometry accessors from various hyparquet result shapes
function getGeometryAccessors(result) {
    if (!result) return null;
    // Case 0: Apache Arrow Table-like
    try {
        const isArrowTable = (typeof result.numRows === 'number') && (typeof result.getChild === 'function' || typeof result.getColumn === 'function');
        if (isArrowTable) {
            const getVec = (name) => (typeof result.getChild === 'function' ? result.getChild(name) : (typeof result.getColumn === 'function' ? result.getColumn(name) : null));
            const geomKeyCandidates = ['geometry', 'GEOMETRY', 'geom', 'GEOM'];
            let vec = null;
            for (const k of geomKeyCandidates) { vec = getVec(k); if (vec) break; }
            // Heuristic: scan columns by index if names are unknown
            if (!vec && typeof result.getChildAt === 'function' && result.schema && Array.isArray(result.schema.fields)) {
                for (let i = 0; i < result.schema.fields.length; i++) {
                    const v = result.getChildAt(i);
                    if (v && typeof v.get === 'function') {
                        const sample = v.get(0);
                        if (toUint8(sample)) { vec = v; break; }
                    }
                }
            }
            if (vec && typeof vec.get === 'function') {
                const length = result.numRows || (typeof vec.length === 'number' ? vec.length : 0);
                return { length, getGeomAt: (i) => toUint8(vec.get(i)) };
            }
        }
    } catch (_) { /* ignore */ }
    // Case 1: columnar: result.columns.{geometry}
    const cols = result.columns || result.cols || null;
    const geomKeyCandidates = ['geometry', 'GEOMETRY', 'geom', 'GEOM'];
    if (cols && (typeof cols === 'object' || cols instanceof Map)) {
        const getColByKey = (k) => (cols instanceof Map ? cols.get(k) : cols[k]);
        let key = geomKeyCandidates.find(k => !!getColByKey(k));
        if (!key) {
            // Heuristic: find first binary-like column
            const keys = cols instanceof Map ? Array.from(cols.keys()) : Object.keys(cols);
            key = keys.find(k => {
                const col = getColByKey(k);
                if (!col) return false;
                let sample = undefined;
                if (Array.isArray(col)) sample = col[0];
                else if (typeof col.get === 'function') sample = col.get(0);
                else if (col && col[0] !== undefined) sample = col[0];
                const u8 = toUint8(sample);
                return !!u8;
            });
        }
        if (key) {
            const column = getColByKey(key);
            const length = (column && typeof column.length === 'number') ? column.length : (typeof column.get === 'function' ? (column.length ?? 0) : 0);
            return {
                length,
                getGeomAt: (i) => {
                    const val = (column && typeof column.get === 'function') ? column.get(i) : column[i];
                    return toUint8(val);
                }
            };
        }
    }
    // Case 2: flat arrays: result.geometry
    for (const k of geomKeyCandidates) {
        if (result[k] && result[k].length != null) {
            const arr = result[k];
            return { length: arr.length, getGeomAt: (i) => toUint8(arr[i]) };
        }
    }
    // Case 3: row-wise: result.rows = [{ geometry: ... }]
    const rows = result.rows || result.data || null;
    if (Array.isArray(rows)) {
        return {
            length: rows.length,
            getGeomAt: (i) => {
                const row = rows[i] || {};
                const key = geomKeyCandidates.find(k => row[k] != null);
                return key ? toUint8(row[key]) : null;
            }
        };
    }
    // Case 4: iterable table/vectors
    if (result && typeof result[Symbol.iterator] === 'function' && !Array.isArray(result)) {
        try {
            const cache = Array.from(result);
            const key = geomKeyCandidates.find(k => cache[0] && cache[0][k] != null);
            if (key) {
                return { length: cache.length, getGeomAt: (i) => toUint8(cache[i][key]) };
            }
        } catch (_) {}
    }
    return null;
}

// Make test function available globally
window.testHyparquetStrategy = testHyparquetStrategy;

// Expose optimized hyparquet reader to other modules
window.hyparquetReadWithBbox = hyparquetReadWithBbox;

// Export for ES module usage
export { 
    testHyparquetStrategy, 
    combineStreamResults, 
    mergeStreamingResults, 
    hyparquetReadWithBbox,
    processHyparquetTable,
    getGeometryAccessors,
    wkbEnvelope,
    envIntersectsBbox
};
