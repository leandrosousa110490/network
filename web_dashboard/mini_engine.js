
// Mini Data Engine for Standalone Dashboard
// Replicates backend logic using client-side JavaScript

window.MiniEngine = {
    data: [],
    colMap: {}, // Cache for column name resolution
    
    init: function(data) {
        this.data = data;
        this.colMap = {};
        console.log("MiniEngine Initialized. Rows:", data ? data.length : 0);
        if (data && data.length > 0) {
            console.log("Available Columns:", Object.keys(data[0]));
        }
    },

    getColumns: function() {
        if (!this.data || this.data.length === 0) return [];
        // Scan first 50 rows to collect all possible keys (handling sparse data)
        const keys = new Set();
        this.data.slice(0, 50).forEach(row => {
            Object.keys(row).forEach(k => keys.add(k));
        });
        return Array.from(keys).sort();
    },
    
    // Helper to resolve column names (handling case/whitespace mismatch)
    resolveCol: function(colName) {
        if (!colName) return colName;
        if (this.colMap[colName]) return this.colMap[colName];
        
        if (!this.data || this.data.length === 0) return colName;
        const row = this.data[0];
        if (row.hasOwnProperty(colName)) {
            this.colMap[colName] = colName;
            return colName;
        }
        
        // Try to find a match
        const lowerCol = String(colName).toLowerCase().trim();
        for (const key of Object.keys(row)) {
            if (String(key).toLowerCase().trim() === lowerCol) {
                console.warn(`Column mismatch resolved: '${colName}' -> '${key}'`);
                this.colMap[colName] = key;
                return key;
            }
            // Also try removing quotes if present in key but not colName or vice versa
            if (String(key).replace(/['"]/g, "") === String(colName).replace(/['"]/g, "")) {
                console.warn(`Column mismatch resolved (quotes): '${colName}' -> '${key}'`);
                this.colMap[colName] = key;
                return key;
            }
        }
        
        console.error(`Column not found: '${colName}'`);
        return colName; // Fallback
    },
    
    getUniqueValues: function(col) {
        if (!this.data) return {error: "No data"};
        const actualCol = this.resolveCol(col);
        try {
            const vals = [...new Set(this.data.map(d => d[actualCol]))]
                .filter(v => v !== null && v !== undefined)
                .sort()
                .slice(0, 100);
            return {success: true, values: vals};
        } catch (e) {
            return {success: false, error: e.message};
        }
    },
    
    getChartData: function(config, filters) {
        if (!this.data) return {error: "No data"};
        
        try {
            // 1. Resolve Columns
            const xCol = this.resolveCol(config.x);
            const legendCol = this.resolveCol(config.legend);
            
            let yCols = [];
            const yVal = config.y;
            if (Array.isArray(yVal)) {
                yCols = yVal.map(c => this.resolveCol(c));
            } else if (yVal) {
                yCols = [this.resolveCol(yVal)];
            }

            // 2. Filter Data
            let filtered = this.data;
            if (filters) {
                filtered = filtered.filter(row => {
                    for (const [col, vals] of Object.entries(filters)) {
                        const actualFilterCol = this.resolveCol(col);
                        if (vals && vals.length > 0) {
                            // Loose comparison for numbers/strings mismatch
                            // But usually data types should match. 
                            // Let's use includes.
                            // Check if vals includes the row value (loose eq)
                            const rowVal = row[actualFilterCol];
                            // Use simple includes if types match, or some loose check
                            if (!vals.includes(rowVal) && !vals.some(v => v == rowVal)) return false;
                        }
                    }
                    return true;
                });
            }
            
            // 3. Prepare Config
            const agg = config.agg || 'count';
            const timeGroup = config.timeGroup;
            const type = config.type;
            
            // Helper to parse date
            const getDateKey = (val, group) => {
                if (val === undefined || val === null || val === '') return 'Unknown';
                const d = new Date(val);
                if (isNaN(d.getTime())) return String(val); // Fallback if not date
                
                if (group === 'year') return d.getFullYear();
                if (group === 'month') return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2, '0')}-01`;
                if (group === 'day') return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
                return val;
            };
            
            // Table Widget
            if (type === 'table') {
                const cols = config.columns ? config.columns.map(c => this.resolveCol(c)) : Object.keys(this.data[0] || {}).slice(0, 10);
                return filtered.slice(0, 1000).map(row => {
                    const newRow = {};
                    cols.forEach(c => newRow[c] = row[c]);
                    return newRow;
                });
            }
            
            // Filter Widget
            if (type === 'filter') {
                const fCol = this.resolveCol(config.column);
                return {values: [...new Set(this.data.map(d => d[fCol]))].sort().slice(0, 100)};
            }
            
            // Pie Chart
            if (type === 'pie') {
                const yCol = yCols.length > 0 ? yCols[0] : null;
                // Group by X, agg Y
                const groups = {};
                filtered.forEach(row => {
                    const key = timeGroup ? getDateKey(row[xCol], timeGroup) : row[xCol];
                    // If key is undefined or null, label as "Unknown"
                    const safeKey = (key === undefined || key === null || key === 'undefined') ? "Unknown" : key;
                    
                    if (!groups[safeKey]) groups[safeKey] = 0;
                    
                    if (yCol && (agg === 'sum' || agg === 'avg')) {
                        groups[safeKey] += (Number(row[yCol]) || 0);
                    } else {
                        groups[safeKey] += 1;
                    }
                });
                
                // Convert to array
                const result = Object.entries(groups).map(([k, v]) => ({
                    label: k,
                    value: v
                }));
                
                // Sort and limit
                result.sort((a, b) => b.value - a.value);
                return result.slice(0, 20);
            }
            
            // Scatter (Raw)
            if (type === 'scatter') {
                const yTarget = yCols.length > 0 ? yCols[0] : null;
                return filtered.slice(0, 5000).map(row => {
                    const item = {
                        x: row[xCol],
                        color: legendCol ? row[legendCol] : undefined
                    };
                    if (yTarget) item.y = row[yTarget];
                    else item.y = 1;
                    
                    if (yCols.length > 1) {
                        yCols.forEach(yc => item[yc] = row[yc]);
                    }
                    return item;
                });
            }
            
            // Bar / Line (Aggregation)
            const groups = {}; // Key -> {x, color, count, sum_y, etc}
            
            filtered.forEach(row => {
                const xRaw = row[xCol];
                // Robust undefined check for X axis
                const x = timeGroup ? getDateKey(xRaw, timeGroup) : (xRaw === undefined ? 'Unknown' : xRaw);
                const color = legendCol ? row[legendCol] : null;
                
                const key = color ? `${x}###${color}` : String(x);
                
                if (!groups[key]) {
                    groups[key] = { 
                        x: x, 
                        color: color, 
                        count: 0,
                        sums: {},
                        counts: {} // for avg
                    };
                    yCols.forEach(yc => {
                        groups[key].sums[yc] = 0;
                        groups[key].counts[yc] = 0;
                    });
                }
                
                const g = groups[key];
                g.count++;
                
                yCols.forEach(yc => {
                    const val = Number(row[yc]);
                    if (!isNaN(val)) {
                        g.sums[yc] += val;
                        g.counts[yc]++;
                    }
                });
            });
            
            // Transform to Result Array
            let result = Object.values(groups).map(g => {
                const item = { x: g.x };
                if (g.color) item.color = g.color;
                
                if (agg === 'count') {
                    item.count = g.count;
                    item.y = g.count;
                } else if (agg === 'sum') {
                    if (yCols.length === 1) {
                        item.y = Number(g.sums[yCols[0]].toFixed(2));
                    } else {
                        yCols.forEach(yc => item[yc] = Number(g.sums[yc].toFixed(2)));
                    }
                } else if (agg === 'avg') {
                     if (yCols.length === 1) {
                        const yc = yCols[0];
                        item.y = g.counts[yc] ? Number((g.sums[yc] / g.counts[yc]).toFixed(2)) : 0;
                    } else {
                        yCols.forEach(yc => {
                            item[yc] = g.counts[yc] ? Number((g.sums[yc] / g.counts[yc]).toFixed(2)) : 0;
                        });
                    }
                }
                return item;
            });
            
            // Sort by X
            result.sort((a, b) => {
                if (a.x < b.x) return -1;
                if (a.x > b.x) return 1;
                return 0;
            });
            
            return result;
            
        } catch (e) {
            console.error("MiniEngine Error:", e);
            return {error: e.message};
        }
    }
};
