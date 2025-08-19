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

/**
 * Create an AsyncBuffer-like object that performs HTTP Range requests
 */
async function createRangeAsyncBuffer(url) {
    const headResp = await fetch(url, { method: 'HEAD' });
    const contentLength = parseInt(headResp.headers.get('content-length') || '0', 10);
    if (!contentLength) {
        throw new Error('Could not determine content-length for range reads');
    }
    return {
        byteLength: contentLength,
        async slice(start, endOrLength) {
            let rangeHeader;
            if (typeof endOrLength === 'number') {
                // Interpret as end index if greater than start; otherwise as length
                if (endOrLength > start) {
                    rangeHeader = `bytes=${start}-${endOrLength - 1}`;
                } else {
                    rangeHeader = `bytes=${start}-${start + endOrLength - 1}`;
                }
            } else if (typeof endOrLength === 'undefined') {
                // Open-ended range
                rangeHeader = `bytes=${start}-`;
            } else {
                // Fallback: assume end index
                rangeHeader = `bytes=${start}-${endOrLength - 1}`;
            }
            const res = await fetch(url, { headers: { Range: rangeHeader } });
            if (!res.ok && res.status !== 206) {
                throw new Error(`Range request failed: HTTP ${res.status}`);
            }
            // Return ArrayBuffer as required by hyparquet internals (DataView construction)
            const ab = await res.arrayBuffer();
            return ab;
        },
    };
}

/**
 * Real Hyparquet decoding path: range-based row-group reads + bbox filtering
 * Tries multiple API entry points to be robust across Hyparquet builds
 */
async function hyparquetReadWithBbox(url, bbox, { targetRows, maxRowGroups }) {
    const hy = window.hyparquet || {};

    const openStart = performance.now();
    let reader = null;
    let usedFallbackFullRead = false;
    let totalBytesStreamed = 0;

    // Attempt to read via parquetRead using AsyncBuffer (preferred)
    let table = null;
    try {
        const asyncBuffer = await createRangeAsyncBuffer(url);
        if (hy.parquetRead) {
            table = await hy.parquetRead({ file: asyncBuffer });
        } else if (hy.readParquet) {
            table = await hy.readParquet(asyncBuffer);
        }
        // If not available, try opening a reader next
        if (!table && hy.ParquetReader && (hy.ParquetReader.openAsyncBuffer || hy.ParquetReader.openBuffer || hy.ParquetReader.open)) {
            const openFn = hy.ParquetReader.openAsyncBuffer || hy.ParquetReader.openBuffer || hy.ParquetReader.open;
            reader = await openFn(asyncBuffer);
        } else if (!table && hy.ParquetAsyncReader && hy.ParquetAsyncReader.open) {
            reader = await hy.ParquetAsyncReader.open(asyncBuffer);
        }
    } catch (e) {
        console.warn('Hyparquet range-based read/open failed; will try fallback', e);
    }

    // Fallback: full-file fetch, then create an in-memory AsyncBuffer
    if (!reader) {
        usedFallbackFullRead = true;
        try {
            const resp = await fetch(url);
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const fullBuf = new Uint8Array(await resp.arrayBuffer());
            totalBytesStreamed = fullBuf.byteLength;
            const baseOffset = fullBuf.byteOffset;
            const backing = fullBuf.buffer;
            const memAsyncBuffer = {
                byteLength: fullBuf.byteLength,
                async slice(start, endOrLength) {
                    const startIdx = typeof start === 'number' ? start : 0;
                    let endExclusive;
                    if (typeof endOrLength === 'number') {
                        if (endOrLength > startIdx) {
                            endExclusive = endOrLength;
                        } else {
                            endExclusive = startIdx + endOrLength;
                        }
                    } else if (typeof endOrLength === 'undefined') {
                        endExclusive = fullBuf.byteLength;
                    } else {
                        endExclusive = endOrLength;
                    }
                    const absStart = baseOffset + startIdx;
                    const absEnd = baseOffset + endExclusive;
                    return backing.slice(absStart, absEnd); // ArrayBuffer
                },
            };
            if (hy.ParquetReader && (hy.ParquetReader.openAsyncBuffer || hy.ParquetReader.openBuffer)) {
                const openFn = hy.ParquetReader.openAsyncBuffer || hy.ParquetReader.openBuffer;
                reader = await openFn(memAsyncBuffer);
            } else if (hy.ParquetReader && hy.ParquetReader.fromBuffer) {
                reader = await hy.ParquetReader.fromBuffer(fullBuf);
            } else if (hy.parquetRead) {
                // Try common signature: parquetRead({ file: AsyncBuffer })
                try {
                    table = await hy.parquetRead({ file: memAsyncBuffer });
                } catch (e1) {
                    // Try passing Uint8Array directly
                    table = await hy.parquetRead({ file: fullBuf });
                }
            } else if (hy.parquetReadBuffer) {
                table = await hy.parquetReadBuffer(fullBuf);
            } else if (hy.readParquetBuffer) {
                table = await hy.readParquetBuffer(fullBuf);
            } else if (hy.readParquet) {
                // Some builds accept AsyncBuffer in readParquet
                table = await hy.readParquet(memAsyncBuffer);
            } else {
                throw new Error('No suitable Hyparquet API for full read');
            }
        } catch (e) {
            streamingMetrics.recordStreamingEvent('hyparquet_open_error', 'Failed to open parquet', { error: e.message });
            throw e;
        }
    }

    const openEnd = performance.now();
    streamingMetrics.recordStreamingEvent('hyparquet_open', 'Opened parquet reader', {
        durationMs: openEnd - openStart,
        fallback: usedFallbackFullRead,
    });

    let rowsCollected = 0;
    let rowGroupsProcessed = 0;
    let batchesProcessed = 0;
    let firstBatchRecorded = false;

    // Helper to iterate rows from a table-like object
    function* iterateRowsFromTable(tbl) {
        const cols = tbl.columns || tbl;
        const geomCol = cols.geometry || cols.GEOMETRY || cols.geom || cols.GEOM || null;
        const len = tbl.length || (geomCol && geomCol.length) || 0;
        for (let i = 0; i < len; i++) {
            const geom = geomCol ? (geomCol[i] instanceof Uint8Array ? geomCol[i] : new Uint8Array(geomCol[i])) : null;
            yield { geometry: geom };
        }
    }

    // Path 1: We only have a full table (no reader)
    if (table && !reader) {
        const acc = getGeometryAccessors(table);
        let inBatch = 0;
        const batchStart = performance.now();
        if (acc && acc.length) {
            // Simulate batches from table rows to surface progress in UI
            for (let i = 0; i < acc.length; i++) {
                const geom = acc.getGeomAt(i);
                if (!firstBatchRecorded) { streamingMetrics.recordFirstBatch(); firstBatchRecorded = true; }
                inBatch++;
                if (geom) {
                    const env = wkbEnvelope(geom);
                    if (envIntersectsBbox(env, bbox)) rowsCollected++;
                }
                if (inBatch >= 512) {
                    const batchEnd = performance.now();
                    streamingMetrics.recordRowGroupBatch(batchesProcessed, inBatch, batchStart, batchEnd, { filteredRows: rowsCollected, url });
                    batchesProcessed++;
                    inBatch = 0;
                }
                if (rowsCollected >= targetRows) break;
            }
            if (inBatch > 0) {
                streamingMetrics.recordRowGroupBatch(batchesProcessed, inBatch, batchStart, performance.now(), { filteredRows: rowsCollected, url });
                batchesProcessed++;
            }
            // Surface a pseudo row-group count based on batches to avoid zero RGs in UI
            rowGroupsProcessed = Math.max(rowGroupsProcessed, Math.max(1, batchesProcessed));
        }
        return { totalBytesStreamed, rowsCollected, rowGroupsProcessed, batchesProcessed, earlyTermination: rowsCollected >= targetRows };
    }

    // Path 2: Iterate row groups via reader
    let rowGroupCount = 0;
    try {
        rowGroupCount =
            (reader.getRowGroupCount && reader.getRowGroupCount()) ||
            (reader.rowGroups && reader.rowGroups.length) ||
            (reader.metadata && reader.metadata.row_groups && reader.metadata.row_groups.length) ||
            (reader.getRowGroups && Array.isArray(reader.getRowGroups()) && reader.getRowGroups().length) ||
            0;
    } catch (e) {
        rowGroupCount = 0;
    }
    if (!rowGroupCount) {
        // Try to materialize a table from the reader if possible
        if (!table) {
            try {
                if (reader && reader.readTable) {
                    table = await reader.readTable({ columns: ['StateAbbr', 'Tract', 'geometry'] });
                } else if (reader && reader.readAll) {
                    table = await reader.readAll({ columns: ['StateAbbr', 'Tract', 'geometry'] });
                }
            } catch (e) {
                streamingMetrics.recordStreamingEvent('hyparquet_read_error', 'Reader table materialization failed', { error: e.message });
            }
        }
        // Fallback: try reading via table path (we may already have table)
        if (table) {
            if (!firstBatchRecorded) { streamingMetrics.recordFirstBatch(); firstBatchRecorded = true; }
            let inBatch = 0;
            const batchStart = performance.now();
            const rows = table.length || (table.columns && table.columns.geometry && table.columns.geometry.length) || 0;
            for (let i = 0; i < rows; i++) {
                const geom = (table.geometry && table.geometry[i]) || (table.columns && table.columns.geometry && table.columns.geometry[i]) || null;
                if (geom) {
                    const u8 = geom instanceof Uint8Array ? geom : new Uint8Array(geom);
                    const env = wkbEnvelope(u8);
                    if (envIntersectsBbox(env, bbox)) rowsCollected++;
                }
                inBatch++;
                if (inBatch >= 512) {
                    streamingMetrics.recordRowGroupBatch(batchesProcessed, inBatch, batchStart, performance.now(), { filteredRows: rowsCollected, url });
                    batchesProcessed++;
                    inBatch = 0;
                }
                if (rowsCollected >= targetRows) break;
            }
            return { totalBytesStreamed, rowsCollected, rowGroupsProcessed, batchesProcessed, earlyTermination: rowsCollected >= targetRows };
        }
        // As a last resort, attempt reader.read if present
        try {
            if (reader && reader.read) {
                const all = await reader.read({ columns: ['StateAbbr', 'Tract', 'geometry'] });
                if (!firstBatchRecorded) { streamingMetrics.recordFirstBatch(); firstBatchRecorded = true; }
                let filtered = 0;
                const rows = all.length || (all.columns && all.columns.geometry && all.columns.geometry.length) || 0;
                for (let i = 0; i < rows; i++) {
                    const geom = (all.geometry && all.geometry[i]) || (all.columns && all.columns.geometry && all.columns.geometry[i]) || null;
                    if (geom) {
                        const u8 = geom instanceof Uint8Array ? geom : new Uint8Array(geom);
                        const env = wkbEnvelope(u8);
                        if (envIntersectsBbox(env, bbox)) filtered++;
                    }
                    if ((rowsCollected + filtered) >= targetRows) break;
                }
                rowsCollected += filtered;
                streamingMetrics.recordRowGroupBatch(batchesProcessed, rows, performance.now(), performance.now(), { filteredRows: filtered, url });
                batchesProcessed++;
                return { totalBytesStreamed, rowsCollected, rowGroupsProcessed, batchesProcessed, earlyTermination: rowsCollected >= targetRows };
            }
        } catch (e) {
            streamingMetrics.recordStreamingEvent('hyparquet_read_error', 'Reader read() failed', { error: e.message });
        }
        return { totalBytesStreamed, rowsCollected, rowGroupsProcessed, batchesProcessed, earlyTermination: rowsCollected >= targetRows };
    }

    for (let rg = 0; rg < rowGroupCount; rg++) {
        const batchStart = performance.now();
        let rgData = null;
        try {
            if (reader.readRowGroup) {
                rgData = await reader.readRowGroup(rg, { columns: ['StateAbbr', 'Tract', 'geometry'] });
            } else if (reader.read) {
                rgData = await reader.read({ rowGroup: rg, columns: ['StateAbbr', 'Tract', 'geometry'] });
            } else if (reader.getRowGroup) {
                rgData = await reader.getRowGroup(rg);
            } else if (typeof reader.getRowGroups === 'function') {
                const groups = await reader.getRowGroups();
                rgData = groups && groups[rg] ? groups[rg] : null;
            }
        } catch (e) {
            streamingMetrics.recordStreamingEvent('rowgroup_read_error', `Failed to read row group ${rg}`, { error: e.message });
            continue;
        }

        if (!firstBatchRecorded) { streamingMetrics.recordFirstBatch(); firstBatchRecorded = true; }

        const acc = getGeometryAccessors(rgData);
        let filteredRows = 0;
        let rows = 0;
        if (acc && acc.length) {
            rows = acc.length;
            for (let i = 0; i < rows; i++) {
                const geom = acc.getGeomAt(i);
                if (geom) {
                    const env = wkbEnvelope(geom);
                    if (envIntersectsBbox(env, bbox)) filteredRows++;
                }
                if ((rowsCollected + filteredRows) >= targetRows) break;
            }
        }

        rowsCollected += filteredRows;
        rowGroupsProcessed++;
        const batchEnd = performance.now();
        streamingMetrics.recordRowGroupBatch(batchesProcessed, rows, batchStart, batchEnd, { filteredRows, url, rowGroupIndex: rg });
        batchesProcessed++;

        if (rowsCollected >= targetRows || rowGroupsProcessed >= maxRowGroups) break;

        if (batchesProcessed % 3 === 0) {
            streamingMetrics.takeMemorySnapshot(`rowgroup_${rg}`);
        }
    }

    return { totalBytesStreamed, rowsCollected, rowGroupsProcessed, batchesProcessed, earlyTermination: rowsCollected >= targetRows || rowGroupsProcessed >= maxRowGroups };
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

// Expose real hyparquet reader to other modules
window.hyparquetReadWithBbox = hyparquetReadWithBbox;

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
