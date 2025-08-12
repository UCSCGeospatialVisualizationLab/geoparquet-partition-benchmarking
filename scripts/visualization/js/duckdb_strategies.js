// DuckDB-WASM SQL query strategies for different S3 partition layouts

import { trackRangeRequestPattern } from './range_metrics.js';

// S3 base URL for parquet data access
const BASE_URL = 'https://s3-west.nrp-nautilus.io/vizlab-geodatalake-exports/hazus-performance-test';

const DuckDBStrategies = {
    
    /**
     * NO_PARTITION: Single file query with spatial WHERE clause
     * Tests HTTP range request efficiency on large file
     */
    async no_partition(connection, bbox) {
        const url = `${BASE_URL}/no_partition/hazus_CensusTract.parquet`;
        
        // Spatial filter query with geometry processing
        const sql = `
            SELECT 
                StateAbbr,
                Tract,
                ST_AsText(ST_Centroid(ST_GeomFromWKB(geometry))) as centroid
            FROM '${url}'
            WHERE StateAbbr = 'CA'
              AND ST_Intersects(
                  ST_GeomFromWKB(geometry),
                  ST_MakeEnvelope(${bbox.west}, ${bbox.south}, ${bbox.east}, ${bbox.north})
              )
            LIMIT 2000
        `;
        
        return {
            sql,
            url,
            queryType: 'spatial_filter',
            expectedRows: '1000-2000',
            description: 'Single file with spatial predicate pushdown'
        };
    },

    /**
     * ATTRIBUTE_STATE: Pre-filtered partition query
     * Should be efficient due to state-based partitioning
     */
    async attribute_state(connection, bbox) {
        const url = `${BASE_URL}/attribute_state/StateAbbr=CA/data_0.parquet`;
        
        // Simpler query since state filtering already done by partitioning
        const sql = `
            SELECT 
                StateAbbr,
                Tract,
                ST_AsText(ST_Centroid(ST_GeomFromWKB(geometry))) as centroid
            FROM '${url}'
            WHERE ST_Intersects(
                ST_GeomFromWKB(geometry),
                ST_MakeEnvelope(${bbox.west}, ${bbox.south}, ${bbox.east}, ${bbox.north})
            )
            LIMIT 1500
        `;
        
        return {
            sql,
            url,
            queryType: 'partition_filtered',
            expectedRows: '800-1500',
            description: 'Pre-filtered CA partition with spatial query'
        };
    },

    /**
     * SPATIAL_H3_L3: Union query across multiple H3 hexagon files
     * Tests DuckDB's ability to query multiple small files efficiently
     */
    async spatial_h3_l3(connection, bbox) {
        const hexagons = [
            '832830fffffffff', // Bay Area hexagon (confirmed available)
            '832834fffffffff'  // Bay Area hexagon (confirmed available)
        ];
        
        // Union query across multiple spatial partitions
        const unionQueries = hexagons.map(hex => `
            SELECT 
                StateAbbr,
                Tract,
                ST_AsText(ST_Centroid(ST_GeomFromWKB(geometry))) as centroid,
                '${hex}' as h3_partition
            FROM '${BASE_URL}/spatial_h3_l3/h3_level3=${hex}/data_0.parquet'
            WHERE ST_Intersects(
                ST_GeomFromWKB(geometry),
                ST_MakeEnvelope(${bbox.west}, ${bbox.south}, ${bbox.east}, ${bbox.north})
            )
        `).join('\nUNION ALL\n');
        
        const sql = `${unionQueries}\nLIMIT 1600`;
        
        return {
            sql,
            url: `${BASE_URL}/spatial_h3_l3/`, // Base URL for multiple files
            queryType: 'spatial_union',
            expectedRows: '400-1600',
            description: `Union query across ${hexagons.length} H3 hexagon partitions`
        };
    },

    /**
     * HYBRID_STATE_H3: Hierarchical query within CA state partitions
     * Combines benefits of state filtering with spatial sub-partitioning
     */
    async hybrid_state_h3(connection, bbox) {
        const hexagons = [
            '832830fffffffff', // Bay Area hexagon (confirmed available)
            '832834fffffffff'  // Bay Area hexagon (confirmed available)
        ];
        
        // Union query within CA state hierarchy
        const unionQueries = hexagons.map(hex => `
            SELECT 
                StateAbbr,
                Tract,
                ST_AsText(ST_Centroid(ST_GeomFromWKB(geometry))) as centroid,
                '${hex}' as h3_partition
            FROM '${BASE_URL}/hybrid_state_h3/StateAbbr=CA/h3_level3=${hex}/data_0.parquet'
            WHERE ST_Intersects(
                ST_GeomFromWKB(geometry),
                ST_MakeEnvelope(${bbox.west}, ${bbox.south}, ${bbox.east}, ${bbox.north})
            )
        `).join('\nUNION ALL\n');
        
        const sql = `${unionQueries}\nLIMIT 1200`;
        
        return {
            sql,
            url: `${BASE_URL}/hybrid_state_h3/StateAbbr=CA/`, // Base URL for CA hierarchy
            queryType: 'hybrid_union', 
            expectedRows: '300-1200',
            description: `Hierarchical query: CA state with ${hexagons.length} H3 sub-partitions`
        };
    },

    /**
     * Get DuckDB connection configuration for optimal S3 access
     */
    async configureDuckDBForS3(connection) {
        try {
            // Configure DuckDB for optimal S3 HTTP range requests
            await connection.query(`SET s3_region='us-west-1';`);
            await connection.query(`SET s3_endpoint='s3-west.nrp-nautilus.io';`);
            await connection.query(`SET s3_url_style='path';`);
            await connection.query(`SET s3_use_ssl=true;`);
            
            // Enable spatial extension
            await connection.query(`INSTALL spatial;`);
            await connection.query(`LOAD spatial;`);
            
            // Configure HTTP settings for range requests
            await connection.query(`SET http_timeout=30000;`);
            await connection.query(`SET http_retries=3;`);
            
            console.log('DuckDB S3 configuration completed');
            return true;
            
        } catch (error) {
            console.error('DuckDB S3 configuration failed:', error);
            throw new Error(`S3 configuration failed: ${error.message}`);
        }
    },

    /**
     * Execute strategy-specific query with performance monitoring
     */
    async executeStrategyQuery(connection, strategy, bbox, metrics) {
        console.log(`Executing DuckDB strategy: ${strategy}`);
        
        // Get strategy-specific SQL
        const querySpec = await this[strategy](connection, bbox);
        
        // Configure range request tracking
        trackRangeRequestPattern(querySpec.url, strategy, metrics);
        
        // Execute query with timing
        const queryStart = performance.now();
        metrics.recordQueryEvent('strategy_query_start', `Starting ${strategy} query`);
        
        try {
            const result = await connection.query(querySpec.sql);
            const queryEnd = performance.now();
            
            const rowCount = result?.numRows || 0;
            metrics.recordSQLQuery(
                querySpec.sql, 
                queryStart, 
                queryEnd, 
                rowCount,
                null, // query plan not available
                {
                    strategy,
                    queryType: querySpec.queryType,
                    expectedRows: querySpec.expectedRows,
                    description: querySpec.description
                }
            );
            
            console.log(`${strategy} query completed: ${rowCount} rows in ${(queryEnd - queryStart).toFixed(2)}ms`);
            
            return {
                result,
                querySpec,
                rowCount,
                queryTime: queryEnd - queryStart
            };
            
        } catch (error) {
            const queryEnd = performance.now();
            metrics.recordQueryEvent('strategy_query_error', `${strategy} query failed: ${error.message}`);
            throw new Error(`${strategy} query failed: ${error.message}`);
        }
    },

    /**
     * Get strategy configuration for optimization
     */
    getStrategyConfig(strategy) {
        const configs = {
            no_partition: {
                expectedFileSize: '160MB',
                rangeRequestOptimal: true,
                predicatePushdownExpected: true,
                concurrentRequests: 1,
                description: 'Single large file with spatial filtering'
            },
            attribute_state: {
                expectedFileSize: '3MB',
                rangeRequestOptimal: false,
                predicatePushdownExpected: true,
                concurrentRequests: 1,
                description: 'Pre-filtered state partition'
            },
            spatial_h3_l3: {
                expectedFileSize: '5MB each',
                rangeRequestOptimal: false,
                predicatePushdownExpected: true,
                concurrentRequests: 4,
                description: 'Multiple small spatial files'
            },
            hybrid_state_h3: {
                expectedFileSize: '8MB each',
                rangeRequestOptimal: false,
                predicatePushdownExpected: true,
                concurrentRequests: 3,
                description: 'Hierarchical state+spatial files'
            }
        };
        
        return configs[strategy] || configs.no_partition;
    },

    /**
     * Analyze query plan for pushdown detection (if available)
     */
    async analyzeQueryPlan(connection, sql) {
        try {
            const explainQuery = `EXPLAIN ANALYZE ${sql}`;
            const plan = await connection.query(explainQuery);
            
            // Look for filter pushdown indicators in the plan
            const planText = JSON.stringify(plan);
            const hasPushdown = planText.toLowerCase().includes('filter') || 
                             planText.toLowerCase().includes('predicate') ||
                             planText.toLowerCase().includes('projection');
            
            return {
                hasPlan: true,
                predicatePushdown: hasPushdown,
                planText: planText.substring(0, 500) // First 500 chars
            };
            
        } catch (error) {
            console.warn('Query plan analysis failed:', error.message);
            return {
                hasPlan: false,
                predicatePushdown: false,
                error: error.message
            };
        }
    }
};

// Make strategies available globally
window.DuckDBStrategies = DuckDBStrategies;

// Utility functions for DuckDB spatial queries
const DuckDBSpatialUtils = {
    /**
     * Create spatial filter SQL fragment
     */
    createSpatialFilter(bbox, geometryColumn = 'geometry') {
        return `ST_Intersects(
            ST_GeomFromWKB(${geometryColumn}),
            ST_MakeEnvelope(${bbox.west}, ${bbox.south}, ${bbox.east}, ${bbox.north})
        )`;
    },

    /**
     * Create centroid extraction SQL fragment
     */
    createCentroidExtraction(geometryColumn = 'geometry') {
        return `ST_AsText(ST_Centroid(ST_GeomFromWKB(${geometryColumn}))) as centroid`;
    },

    /**
     * Validate spatial query syntax
     */
    validateSpatialQuery(sql) {
        const spatialFunctions = [
            'ST_GeomFromWKB', 'ST_MakeEnvelope', 'ST_Intersects', 
            'ST_Centroid', 'ST_AsText'
        ];
        
        const hasSpatialFunctions = spatialFunctions.some(func => 
            sql.toUpperCase().includes(func.toUpperCase())
        );
        
        const hasFromClause = sql.toUpperCase().includes('FROM');
        const hasValidSyntax = sql.includes('SELECT') && hasFromClause;
        
        return {
            isValid: hasValidSyntax && hasSpatialFunctions,
            hasSpatialFunctions,
            hasValidSyntax,
            spatialFunctionsFound: spatialFunctions.filter(func => 
                sql.toUpperCase().includes(func.toUpperCase())
            )
        };
    }
};

// Export for ES module usage
export { DuckDBStrategies, DuckDBSpatialUtils };
