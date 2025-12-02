
// Global State
let dashboardState = {
    fileLoaded: false,
    filePath: null,
    fileType: null,
    columns: [],
    widgets: {}, // id -> config
    filters: {}, // col -> [values]
    nextWidgetId: 1
};

let grid = null;

// Initialize GridStack
document.addEventListener('DOMContentLoaded', function() {
    grid = GridStack.init({
        cellHeight: 100,
        margin: 10,
        handle: '.card-header', // Only drag by header
        disableOneColumnMode: true, // prevent mobile layout for now
        float: true,
        animate: false, // Reduce visual jumping
        resizable: { autoHide: true, handles: 'se' }
    });

    grid.on('added', function(e, items) {
        items.forEach(item => {
            renderWidget(item.id);
        });
    });

    grid.on('resizestop', function(e, el) {
        const widgetId = el.getAttribute('gs-id');
        const container = document.getElementById(widgetId);
        if (container) {
            const chartDiv = container.querySelector('.chart-wrapper');
            if (chartDiv) {
                Plotly.Plots.resize(chartDiv);
            }
        }
    });

    // Check for startup file
    checkStartupFile();
});

async function checkStartupFile() {
    try {
        // Use the same eel function name as in node.py for consistency if exposed, 
        // but dash.py exposes 'get_startup_file'
        if (eel.get_startup_file) {
            const startupFile = await eel.get_startup_file()();
            if (startupFile) {
                console.log("Startup file detected:", startupFile);
                
                // Auto-detect type
                const ext = startupFile.split('.').pop().toLowerCase();
                let type = 'csv';
                if (['parquet', 'pq'].includes(ext)) type = 'parquet';
                else if (['xlsx', 'xls'].includes(ext)) type = 'excel';
                
                // Load the file directly
                const loadRes = await eel.load_file(startupFile, type)();
                if (loadRes && loadRes.success) {
                    dashboardState.fileLoaded = true;
                    dashboardState.filePath = startupFile;
                    dashboardState.fileType = type;
                    dashboardState.columns = loadRes.columns;
                    
                    document.getElementById('fileInfo').innerText = `${type.toUpperCase()} loaded: ${loadRes.row_count.toLocaleString()} rows`;
                    document.getElementById('addFilterBtn').disabled = false;
                    dashboardState.filters = {};
                    refreshAllWidgets();
                }
            }
        }
    } catch (e) {
        console.error("Error checking startup file:", e);
    }
}

// File Operations
async function browseFile() {
    const res = await eel.browse_file()();
    if (res && res.success) {
        const loadRes = await eel.load_file(res.file_path, res.file_type)();
        if (loadRes && loadRes.success) {
            dashboardState.fileLoaded = true;
            dashboardState.filePath = res.file_path;
            dashboardState.fileType = res.file_type;
            dashboardState.columns = loadRes.columns;
            
            document.getElementById('fileInfo').innerText = `${res.file_type.toUpperCase()} loaded: ${loadRes.row_count.toLocaleString()} rows`;
            // document.getElementById('dashboardTitle').innerText = res.file_path.split(/[\\/]/).pop(); // Removed as per request
            
            // Enable controls
            document.getElementById('addFilterBtn').disabled = false;
            
            // Reset filters for new file
            dashboardState.filters = {};
            renderFiltersList();
            
            clearFilterWidgetsContent();
            
            // Refresh existing widgets if any (they might break if columns differ, but best effort)
            refreshAllWidgets();
        } else {
            alert('Error loading file: ' + (loadRes ? loadRes.error : 'Unknown error'));
        }
    }
}

let editingWidgetId = null;
let editingFilterCol = null;

// Modal Handling
function toggleChartSettings() {
    const type = document.getElementById('chartType').value;
    const chartSettings = document.getElementById('chartSettings');
    const tableSettings = document.getElementById('tableSettings');
    const textSettings = document.getElementById('textSettings');
    
    if (type === 'table') {
        chartSettings.style.display = 'none';
        tableSettings.style.display = 'block';
        textSettings.style.display = 'none';
        document.getElementById('addChartModal').querySelector('.modal-title').innerText = editingWidgetId ? 'Edit Table' : 'Add New Table';
    } else if (type === 'text') {
        chartSettings.style.display = 'none';
        tableSettings.style.display = 'none';
        textSettings.style.display = 'block';
        document.getElementById('addChartModal').querySelector('.modal-title').innerText = editingWidgetId ? 'Edit Text' : 'Add New Text';
    } else {
        chartSettings.style.display = 'block';
        tableSettings.style.display = 'none';
        textSettings.style.display = 'none';
        document.getElementById('addChartModal').querySelector('.modal-title').innerText = editingWidgetId ? 'Edit Chart' : 'Add New Chart';
    }
}

function showAddChartModal(type) {
    // Only require file for non-text widgets
    if (type !== 'text' && !dashboardState.fileLoaded) {
        alert('Please load a data file first.');
        return;
    }
    
    editingWidgetId = null;
    document.getElementById('chartType').value = type;
    updateColumnSelects();
    
    // Reset form
    document.getElementById('xAxis').value = '';
    document.getElementById('timeGroup').value = '';
    document.getElementById('yAxis').value = '';
    document.getElementById('aggregation').value = 'count';
    document.getElementById('legendCol').value = '';
    document.getElementById('showValues').checked = false;
    document.getElementById('chartTitle').value = '';
    document.getElementById('tableColumns').value = '';
    
    // Reset Text Form
    document.getElementById('textContent').value = '';
    document.getElementById('textSize').value = '1rem';
    document.getElementById('textAlign').value = 'left';
    document.getElementById('textBold').checked = false;
    document.getElementById('textItalic').checked = false;
    document.getElementById('textColor').value = '#212529';
    
    toggleChartSettings();
    
    const modalEl = document.getElementById('addChartModal');
    modalEl.querySelector('.btn-primary').innerText = 'Add Widget';
    
    const modal = new bootstrap.Modal(modalEl);
    modal.show();
}

function editWidget(widgetId) {
    const config = dashboardState.widgets[widgetId];
    if (!config) return;
    
    editingWidgetId = widgetId;
    updateColumnSelects();
    
    // Populate form
    document.getElementById('chartType').value = config.type;
    
    if (config.type === 'table') {
        const tableCols = config.columns || [];
        const colSelect = document.getElementById('tableColumns');
        Array.from(colSelect.options).forEach(opt => {
            opt.selected = tableCols.includes(opt.value);
        });
    } else if (config.type === 'text') {
        document.getElementById('textContent').value = config.text || '';
        document.getElementById('textSize').value = config.fontSize || '1rem';
        document.getElementById('textAlign').value = config.textAlign || 'left';
        document.getElementById('textBold').checked = config.isBold || false;
        document.getElementById('textItalic').checked = config.isItalic || false;
        document.getElementById('textColor').value = config.textColor || '#212529';
    } else {
        document.getElementById('xAxis').value = config.x;
        document.getElementById('timeGroup').value = config.timeGroup || '';
        
        const ySelect = document.getElementById('yAxis');
        const yValues = Array.isArray(config.y) ? config.y : (config.y ? [config.y] : []);
        Array.from(ySelect.options).forEach(opt => {
            opt.selected = yValues.includes(opt.value);
        });
        
        document.getElementById('aggregation').value = config.agg;
        document.getElementById('legendCol').value = config.legend || '';
        document.getElementById('showValues').checked = config.showValues || false;
    }
    
    document.getElementById('chartTitle').value = config.title;
    
    toggleChartSettings();
    
    const modalEl = document.getElementById('addChartModal');
    modalEl.querySelector('.btn-primary').innerText = 'Update Widget';
    
    const modal = new bootstrap.Modal(modalEl);
    modal.show();
}

function updateColumnSelects() {
    const selects = document.querySelectorAll('.column-select');
    selects.forEach(select => {
        const currentVal = select.value; // try to preserve if possible
        select.innerHTML = '';
        
        // Add default "None" for legend if it's the legend select
        if (select.id === 'legendCol') {
            const opt = document.createElement('option');
            opt.value = '';
            opt.text = 'None';
            select.appendChild(opt);
        }
        
        dashboardState.columns.forEach(col => {
            const option = document.createElement('option');
            option.value = col;
            option.text = col;
            select.appendChild(option);
        });
        
        // Restore value if valid
        if (currentVal && dashboardState.columns.includes(currentVal)) {
            select.value = currentVal;
        }
    });
}

function saveChartWidget() {
    const type = document.getElementById('chartType').value;
    
    // Get common values
    const title = document.getElementById('chartTitle').value;
    
    let config = {
        type: type,
        title: title || ''
    };

    if (type === 'table') {
        const tableColsSelect = document.getElementById('tableColumns');
        const tableCols = Array.from(tableColsSelect.selectedOptions).map(opt => opt.value);
        
        // If no columns selected, it will default to backend logic (first 10), 
        // but we can also force user to select? 
        // Backend handles empty list by selecting default, so we can just pass it.
        config.columns = tableCols;
        
    } else if (type === 'text') {
        config.text = document.getElementById('textContent').value;
        config.fontSize = document.getElementById('textSize').value;
        config.textAlign = document.getElementById('textAlign').value;
        config.isBold = document.getElementById('textBold').checked;
        config.isItalic = document.getElementById('textItalic').checked;
        config.textColor = document.getElementById('textColor').value;
        
    } else {
        // Chart specific validation
        const x = document.getElementById('xAxis').value;
        if (!x) {
            alert('Please select an X-axis column');
            return;
        }
        
        const timeGroup = document.getElementById('timeGroup').value;
        const ySelect = document.getElementById('yAxis');
        const y = Array.from(ySelect.selectedOptions).map(opt => opt.value);
        const agg = document.getElementById('aggregation').value;
        const legend = document.getElementById('legendCol').value;
        const showValues = document.getElementById('showValues').checked;
        
        config.x = x;
        config.timeGroup = timeGroup;
        config.y = y;
        config.agg = agg;
        config.legend = legend;
        config.showValues = showValues;
    }
    
    let widgetId;
    if (editingWidgetId) {
        widgetId = editingWidgetId;
    } else {
        widgetId = 'widget_' + dashboardState.nextWidgetId++;
    }
    
    config.id = widgetId;
    
    dashboardState.widgets[widgetId] = config;
    
    if (editingWidgetId) {
        // Update existing
        const el = document.querySelector(`.grid-stack-item[gs-id="${widgetId}"]`);
        if (el) {
             const titleEl = el.querySelector('.widget-title');
             if (titleEl) titleEl.innerText = config.title;
        }
        renderWidget(widgetId);
    } else {
        // Add new
        const gridItem = {
            x: 0, y: 0, w: 6, h: 4,
            id: widgetId,
            content: `
                <div class="card h-100 shadow-sm">
                    <div class="card-header position-relative py-2">
                        <div class="widget-title w-100 text-center fw-bold small text-uppercase text-truncate px-5">
                            ${config.title}
                        </div>
                        <div class="widget-controls position-absolute top-0 end-0 h-100 d-flex align-items-center pe-2">
                            <button class="btn btn-link btn-sm text-muted p-0 me-2" onclick="editWidget('${widgetId}')">
                                <i class="fas fa-edit"></i>
                            </button>
                            <button class="btn btn-link btn-sm text-muted p-0" onclick="removeWidget('${widgetId}')">
                                <i class="fas fa-times"></i>
                            </button>
                        </div>
                    </div>
                    <div class="card-body p-2 position-relative overflow-hidden" id="${widgetId}">
                        <div class="d-flex justify-content-center align-items-center h-100 text-muted">Initializing...</div>
                    </div>
                </div>
            `
        };
        grid.addWidget(gridItem);
    }
    
    // Hide modal
    const modalEl = document.getElementById('addChartModal');
    const modal = bootstrap.Modal.getInstance(modalEl);
    modal.hide();
}

function removeWidget(widgetId) {
    const el = document.querySelector(`.grid-stack-item[gs-id="${widgetId}"]`);
    if (el) {
        grid.removeWidget(el);
        delete dashboardState.widgets[widgetId];
    }
}

function restoreWidget(config) {
    let content = '';
    let w = 6, h = 4;
    
    if (config.type === 'filter') {
        const col = config.column;
        
        // Size defaults
        w = 3; h = 4;
        if (config.style === 'dropdown') {
            w = 3; h = 2;
        } else if (config.style === 'list' && config.orientation === 'horizontal') {
            w = 6; h = 2;
        }

        content = `
            <div class="card h-100 shadow-sm" style="overflow: visible;">
                <div class="card-header d-flex justify-content-between align-items-center py-1 px-2 bg-light">
                    <span class="fw-bold small text-truncate" title="Filter: ${col}"><i class="fas fa-filter me-1 text-muted"></i> ${col}</span>
                    <div class="widget-controls d-flex align-items-center">
                        <button class="btn btn-link btn-sm text-muted p-0 me-2" onclick="editFilterWidget('${col}')">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="btn btn-link btn-sm text-danger p-0" onclick="removeFilter('${col}')">
                            <i class="fas fa-times"></i>
                        </button>
                    </div>
                </div>
                <div class="card-body p-2 d-flex flex-column" id="filter-widget-${col}" style="overflow: visible;">
                    <div class="text-center"><div class="spinner-border spinner-border-sm"></div></div>
                </div>
            </div>
        `;
    } else {
        // Chart/Text/Table
        const widgetId = config.id;
        content = `
            <div class="card h-100 shadow-sm">
                <div class="card-header position-relative py-2">
                    <div class="widget-title w-100 text-center fw-bold small text-uppercase text-truncate px-5">
                        ${config.title || ''}
                    </div>
                    <div class="widget-controls position-absolute top-0 end-0 h-100 d-flex align-items-center pe-2">
                        <button class="btn btn-link btn-sm text-muted p-0 me-2" onclick="editWidget('${widgetId}')">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="btn btn-link btn-sm text-muted p-0" onclick="removeWidget('${widgetId}')">
                            <i class="fas fa-times"></i>
                        </button>
                    </div>
                </div>
                <div class="card-body p-2 position-relative overflow-hidden" id="${widgetId}">
                    <div class="d-flex justify-content-center align-items-center h-100 text-muted">Initializing...</div>
                </div>
            </div>
        `;
    }

    // Try to find grid position from saved config?
    // If config has x,y,w,h (it might not if we didn't save it explicitly to dashboardState, 
    // but GridStack updates DOM. We don't sync back to dashboardState on resize/drag unless we add listener.
    // Wait, dashboardState.widgets[id] only stores config, not layout!
    // Layout is managed by GridStack. 
    // If we export, we need to capture layout.
    // The export function in JS needs to capture current layout before sending to Python.
    
    // For restore (in standalone), the layout info should be in the config passed here.
    // So I need to update `exportDashboard` to inject layout info into `dashboardState`.
    
    const node = {
        w: config.gs_w || config.w || w,
        h: config.gs_h || config.h || h,
        x: config.gs_x || config.x, // fallback for old configs
        y: config.gs_y || config.y,
        content: content,
        id: config.id
    };
    
    // Safety check: if x/y are columns (strings), don't use them for layout!
    if (isNaN(parseInt(node.x))) delete node.x;
    if (isNaN(parseInt(node.y))) delete node.y;
    
    grid.addWidget(node);
    
    // Overflow fix for filters
    if (config.type === 'filter') {
        setTimeout(() => {
            const widgetEl = document.getElementById(`filter-widget-${config.column}`);
            if (widgetEl) {
                const contentEl = widgetEl.closest('.grid-stack-item-content');
                if (contentEl) {
                    contentEl.style.overflow = 'visible';
                    const itemEl = contentEl.closest('.grid-stack-item');
                    if (itemEl) itemEl.style.overflow = 'visible';
                }
            }
        }, 50);
    }
}

async function exportDashboard() {
    if (!dashboardState.fileLoaded) {
        alert('Please load data first.');
        return;
    }
    
    // Capture current layout
    const items = grid.getGridItems();
    items.forEach(item => {
        const id = item.getAttribute('gs-id');
        if (dashboardState.widgets[id]) {
            // Use gs_ prefix to avoid conflict with chart config x/y (columns)
            dashboardState.widgets[id].gs_x = item.getAttribute('gs-x');
            dashboardState.widgets[id].gs_y = item.getAttribute('gs-y');
            dashboardState.widgets[id].gs_w = item.getAttribute('gs-w');
            dashboardState.widgets[id].gs_h = item.getAttribute('gs-h');
        }
    });
    
    // Capture Dashboard Title from input if it exists, or global state
    const titleInput = document.getElementById('dashboardNameInput');
    if (titleInput) {
        dashboardState.dashboardTitle = titleInput.value.trim();
    }
    
    // Capture Alignment
    const alignInput = document.querySelector('input[name="titleAlign"]:checked');
    if (alignInput) {
        dashboardState.dashboardTitleAlign = alignInput.value;
    }
    
    const btn = document.getElementById('exportBtn');
    const originalText = btn.innerHTML;
    btn.innerHTML = '<div class="spinner-border spinner-border-sm"></div> Exporting...';
    btn.disabled = true;
    
    try {
        const res = await eel.export_dashboard(dashboardState)();
        if (res.success) {
            alert('Dashboard exported successfully to ' + res.path);
        } else {
            alert('Export failed: ' + res.error);
        }
    } catch (e) {
        alert('Export error: ' + e);
    }
    
    btn.innerHTML = originalText;
    btn.disabled = false;
}

// Core Render Function
async function renderWidget(widgetId) {
    const config = dashboardState.widgets[widgetId];
    if (!config) return;

    // Handle Filter Widgets
    if (config.type === 'filter') {
        await renderFilterWidgetContent(config.column, config.style, config.orientation);
        return;
    }

    const container = document.getElementById(widgetId);
    
    // Ensure container allows scrolling for table
    if (config.type === 'table') {
        // Remove padding for table view to maximize space
        container.classList.remove('p-2');
        container.classList.add('p-0');
        container.style.overflow = 'hidden'; // Let the wrapper handle scrolling
    } else {
        // Restore padding for charts and text
        container.classList.remove('p-0');
        container.classList.add('p-2');
        container.style.overflow = 'hidden';
    }
    
    // Text Widget Rendering (No backend call needed)
    if (config.type === 'text') {
        const style = `
            font-size: ${config.fontSize || '1rem'};
            text-align: ${config.textAlign || 'left'};
            font-weight: ${config.isBold ? 'bold' : 'normal'};
            font-style: ${config.isItalic ? 'italic' : 'normal'};
            color: ${config.textColor || '#212529'};
            white-space: pre-wrap;
            height: 100%;
            overflow: auto;
            display: flex;
            flex-direction: column;
            justify-content: ${config.textAlign === 'center' ? 'center' : (config.textAlign === 'right' ? 'center' : 'flex-start')}; 
        `;
        // Note: Flex justify-content 'center' centers vertically if direction is column. 
        // Wait, user usually means horizontal alignment with 'Left/Center/Right'.
        // text-align handles horizontal. 
        // Let's just use a simple div.
        
        const textStyle = `
            font-size: ${config.fontSize || '1rem'};
            text-align: ${config.textAlign || 'left'};
            font-weight: ${config.isBold ? 'bold' : 'normal'};
            font-style: ${config.isItalic ? 'italic' : 'normal'};
            color: ${config.textColor || '#212529'};
            white-space: pre-wrap;
            width: 100%;
        `;
        
        container.innerHTML = `<div style="height: 100%; overflow: auto;"><div style="${textStyle}">${config.text || ''}</div></div>`;
        return;
    }
    
    container.innerHTML = '<div class="d-flex justify-content-center align-items-center h-100 text-muted">Loading...</div>';

    const data = await eel.get_chart_data(config, dashboardState.filters)();
    
    if (data.error) {
        container.innerHTML = `<div class="text-danger p-3">${data.error}</div>`;
        return;
    }

    if (config.type === 'table') {
        if (!data || data.length === 0) {
             container.innerHTML = '<div class="d-flex justify-content-center align-items-center h-100 text-muted">No data available</div>';
             return;
        }
        
        // Render Table
        // Get columns from first row if available, or config
        let cols = [];
        if (config.columns && config.columns.length > 0) {
            cols = config.columns;
        } else {
            cols = Object.keys(data[0]);
        }
        
        // Wrapper for scrolling
        let tableHtml = `
            <div style="height: 100%; overflow: auto;">
                <table class="table table-sm table-striped table-hover table-bordered mb-0" style="font-size: 0.85rem;">
                    <thead class="table-light sticky-top" style="z-index: 1; top: 0;">
                        <tr>
                            ${cols.map(c => `<th>${c}</th>`).join('')}
                        </tr>
                    </thead>
                    <tbody>
                        ${data.map(row => `
                            <tr>
                                ${cols.map(c => `<td>${row[c] !== undefined ? row[c] : ''}</td>`).join('')}
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `;
        
        container.innerHTML = tableHtml;
        return;
    }

    // Transform for Plotly
    const traces = [];
    
    // Determine Y columns to plot
    let yCols = [];
    if (Array.isArray(config.y)) {
        yCols = config.y;
    } else if (config.y) {
        yCols = [config.y];
    }
    
    // If aggregation is 'count', we ignore the specific Y columns for plotting
    // because the value to plot is the count itself.
    if (config.agg === 'count') {
        yCols = [];
    }

    if (config.type === 'pie') {
        // Pie usually takes one value column
        const valCol = yCols.length > 0 ? yCols[0] : 'value'; // 'value' is default from backend if no Y
        traces.push({
            type: 'pie',
            labels: data.map(r => r.label),
            values: data.map(r => r[valCol] || r.value),
            textinfo: config.showValues ? 'label+value' : 'label+percent',
        });
    } else if (config.legend) {
        // Group by Legend Column
        // Data structure: {x, color, y/count}
        const groups = {};
        data.forEach(r => {
            const colorVal = r.color;
            if (!groups[colorVal]) groups[colorVal] = { x: [], y: [] };
            groups[colorVal].x.push(r.x);
            // Determine value key
            let val = r.count;
            if (val === undefined) val = r.y;
            groups[colorVal].y.push(val);
        });

        Object.keys(groups).forEach(gName => {
            const trace = {
                type: config.type === 'line' ? 'scatter' : config.type,
                mode: config.type === 'line' ? (config.showValues ? 'lines+markers+text' : 'lines+markers') : (config.type === 'scatter' ? 'markers' : undefined),
                x: groups[gName].x,
                y: groups[gName].y,
                name: gName
            };
            if (config.showValues) {
                trace.text = groups[gName].y;
                trace.textposition = 'auto';
            }
            traces.push(trace);
        });
    } else {
        // Standard (No Legend Column)
        if (yCols.length === 0) {
            // If count aggregation with no Y, backend returns 'count' or 'y'
            // Check if 'count' exists in data, else 'y'
            const key = data.length > 0 && 'count' in data[0] ? 'count' : 'y';
            const trace = {
                type: config.type === 'line' ? 'scatter' : config.type,
                mode: config.type === 'line' ? (config.showValues ? 'lines+markers+text' : 'lines+markers') : (config.type === 'scatter' ? 'markers' : undefined),
                x: data.map(r => r.x),
                y: data.map(r => r[key]),
                name: 'Count'
            };
            if (config.showValues) {
                trace.text = data.map(r => r[key]);
                trace.textposition = 'auto';
            }
            traces.push(trace);
        } else {
            // Multiple Y columns
            yCols.forEach(col => {
                const trace = {
                    type: config.type === 'line' ? 'scatter' : config.type,
                    mode: config.type === 'line' ? (config.showValues ? 'lines+markers+text' : 'lines+markers') : (config.type === 'scatter' ? 'markers' : undefined),
                    x: data.map(r => r.x),
                    y: data.map(r => r[col]),
                    name: col
                };
                if (config.showValues) {
                    trace.text = data.map(r => r[col]);
                    trace.textposition = 'auto';
                }
                traces.push(trace);
            });
        }
    }

    const layout = {
        margin: { t: 10, r: 10, b: 30, l: 40 },
        autosize: true,
        showlegend: (!!config.legend) || (traces.length > 1), // Only show legend if grouped or multiple traces
        xaxis: { title: config.x, automargin: true },
        yaxis: { title: config.agg === 'count' ? 'Count' : (config.legend ? (yCols[0] || 'Value') : (yCols.join(', ') || 'Value')), automargin: true }
    };

    container.innerHTML = '<div class="chart-wrapper"></div>';
    Plotly.newPlot(container.querySelector('.chart-wrapper'), traces, layout, {responsive: true, displayModeBar: false});
}

// Filters
function addFilter() {
    if (!dashboardState.fileLoaded) {
        alert('Please load a data file first.');
        return;
    }
    
    editingFilterCol = null;
    const modalEl = document.getElementById('filterModal');
    if (modalEl) {
        modalEl.querySelector('.modal-title').innerText = 'Add Filter';
        const btn = modalEl.querySelector('.btn-primary');
        if (btn) btn.innerText = 'Apply Filter';
    }

    const select = document.getElementById('filterColumn');
    select.innerHTML = '<option value="">Select Column...</option>';
    
    dashboardState.columns.forEach(col => {
        // Skip if already filtered? Maybe not, allow editing.
        const option = document.createElement('option');
        option.value = col;
        option.text = col;
        select.appendChild(option);
    });
    
    document.getElementById('filterValues').innerHTML = '<div class="text-muted small text-center p-2">Select a column first</div>';
    
    // Reset UI state
    document.getElementById('displayDropdown').checked = true;
    toggleListOrientationOption();
    
    const modal = new bootstrap.Modal(document.getElementById('filterModal'));
    modal.show();
}

function toggleListOrientationOption() {
    const isList = document.getElementById('displayList').checked;
    const optionDiv = document.getElementById('listOrientationOption');
    if (optionDiv) {
        optionDiv.style.display = isList ? 'block' : 'none';
    }
}

function updateDropdownButtonText() {
    const btn = document.getElementById('filterValuesDropdown');
    const checkboxes = document.querySelectorAll('.filter-value-checkbox:checked');
    const total = document.querySelectorAll('.filter-value-checkbox').length;
    
    if (checkboxes.length === 0) {
        btn.innerText = 'Select values...';
    } else if (checkboxes.length === total && total > 0) {
        btn.innerText = 'All Selected';
    } else {
        const count = checkboxes.length;
        const first = checkboxes[0].value;
        if (count === 1) {
            btn.innerText = first;
        } else {
            btn.innerText = `${count} selected`;
        }
    }
}

async function updateFilterValues() {
    const col = document.getElementById('filterColumn').value;
    const container = document.getElementById('filterValues');
    const btn = document.getElementById('filterValuesDropdown');
    
    if (!col) {
        container.innerHTML = '<div class="text-muted small text-center p-2">Select a column first</div>';
        btn.innerText = 'Select values...';
        btn.disabled = true;
        return;
    }
    
    btn.disabled = false;
    container.innerHTML = '<div class="text-center p-2"><div class="spinner-border spinner-border-sm text-primary"></div> Loading values...</div>';
    
    const res = await eel.get_unique_values(col)();
    
    if (res.success) {
        container.innerHTML = '';
        
        // Check if we already have values selected for this column
        // If undefined (new filter), default to ALL selected
        const storedFilters = dashboardState.filters[col];
        const isNewFilter = (storedFilters === undefined);
        
        if (res.values.length === 0) {
            container.innerHTML = '<div class="text-muted small p-2">No values found</div>';
            return;
        }
        
        // Add "Select All" option
        const allDiv = document.createElement('div');
        allDiv.className = 'form-check border-bottom pb-2 mb-2';
        allDiv.innerHTML = `
            <input class="form-check-input" type="checkbox" id="selectAllFilters" onchange="toggleAllFilters(this)">
            <label class="form-check-label fw-bold" for="selectAllFilters">Select All</label>
        `;
        container.appendChild(allDiv);
        
        res.values.forEach(val => {
            const div = document.createElement('div');
            div.className = 'form-check';
            const id = 'filter_val_' + String(val).replace(/[^a-zA-Z0-9]/g, '_');
            // Default to true if new filter, otherwise check if in stored filters
            const isChecked = isNewFilter ? true : storedFilters.includes(val);
            
            div.innerHTML = `
                <input class="form-check-input filter-value-checkbox" type="checkbox" value="${val}" id="${id}" ${isChecked ? 'checked' : ''} onchange="updateDropdownButtonText()">
                <label class="form-check-label text-break" for="${id}">${val}</label>
            `;
            container.appendChild(div);
        });

        updateDropdownButtonText();
        
        // Update Select All state
        const allCheckbox = document.getElementById('selectAllFilters');
        const allChecked = document.querySelectorAll('.filter-value-checkbox:checked').length === res.values.length;
        allCheckbox.checked = allChecked;

    } else {
        container.innerHTML = `<div class="text-danger small p-2">Error: ${res.error}</div>`;
    }
}

function toggleAllFilters(source) {
    const checkboxes = document.querySelectorAll('.filter-value-checkbox');
    checkboxes.forEach(cb => cb.checked = source.checked);
    updateDropdownButtonText();
}

function saveFilter() {
    const col = document.getElementById('filterColumn').value;
    if (!col) return;
    
    const checkboxes = document.querySelectorAll('.filter-value-checkbox:checked');
    const values = Array.from(checkboxes).map(cb => cb.value);
    
    // Get display style
    const displayStyle = document.querySelector('input[name="filterDisplayStyle"]:checked').value;
    
    // Get orientation if list style
    let orientation = 'vertical';
    if (displayStyle === 'list') {
        const orientEl = document.querySelector('input[name="filterListOrientation"]:checked');
        if (orientEl) orientation = orientEl.value;
    }

    // Check if filter widget already exists for this column
    const existingWidget = document.getElementById(`filter-widget-${col}`);
    if (existingWidget) {
        if (!editingFilterCol || (editingFilterCol && editingFilterCol !== col)) {
             alert(`A filter for column "${col}" already exists.`);
             return;
        }
    }

    // If editing and column changed, remove old
    if (editingFilterCol && editingFilterCol !== col) {
        removeFilter(editingFilterCol);
    }
    
    // If editing and column same, remove old to replace
    if (editingFilterCol && editingFilterCol === col) {
        removeFilter(col);
    }

    if (values.length > 0) {
        dashboardState.filters[col] = values;
        addFilterWidget(col, displayStyle, orientation);
    } else {
        // If nothing selected, do nothing (or remove?)
        delete dashboardState.filters[col];
    }
    
    refreshAllWidgets();
    
    const modalEl = document.getElementById('filterModal');
    const modal = bootstrap.Modal.getInstance(modalEl);
    modal.hide();
}

async function editFilterWidget(col) {
    editingFilterCol = col;
    const widgetId = `widget_filter_${col}`;
    const config = dashboardState.widgets[widgetId];
    
    if (!config) return;

    const select = document.getElementById('filterColumn');
    select.innerHTML = '';
    
    // Populate columns
    dashboardState.columns.forEach(c => {
        const option = document.createElement('option');
        option.value = c;
        option.text = c;
        select.appendChild(option);
    });
    
    select.value = col;
    
    // Trigger value loading
    await updateFilterValues();
    
    // Set Style
    const styleRadios = document.getElementsByName('filterDisplayStyle');
    styleRadios.forEach(r => {
        r.checked = (r.value === config.style);
    });
    
    // Set Orientation
    const orientationRadios = document.getElementsByName('filterListOrientation');
    orientationRadios.forEach(r => {
        r.checked = (r.value === config.orientation);
    });
    
    toggleListOrientationOption();
    
    // Update Modal UI
    const modalEl = document.getElementById('filterModal');
    modalEl.querySelector('.modal-title').innerText = 'Edit Filter';
    modalEl.querySelector('.btn-primary').innerText = 'Update Filter';
    
    const modal = new bootstrap.Modal(modalEl);
    modal.show();
}

function removeFilter(col) {
    delete dashboardState.filters[col];
    delete dashboardState.widgets[`widget_filter_${col}`];
    
    // Remove widget from grid
    const el = document.getElementById(`filter-widget-${col}`);
    if (el) {
        // Find the grid-stack-item parent
        const item = el.closest('.grid-stack-item');
        grid.removeWidget(item);
    }
    
    refreshAllWidgets();
}

// Function to add filter widget to grid
async function addFilterWidget(col, style, orientation = 'vertical') {
    // Calculate initial size based on style and orientation
    let w = 3, h = 4;
    if (style === 'dropdown') {
        w = 3; h = 2;
    } else if (style === 'list' && orientation === 'horizontal') {
        w = 6; h = 2; // Wider for horizontal list
    }

    const content = `
        <div class="card h-100 shadow-sm" style="overflow: visible;">
            <div class="card-header d-flex justify-content-between align-items-center py-1 px-2 bg-light">
                <span class="fw-bold small text-truncate" title="Filter: ${col}"><i class="fas fa-filter me-1 text-muted"></i> ${col}</span>
                <div class="widget-controls d-flex align-items-center">
                    <button class="btn btn-link btn-sm text-muted p-0 me-2" onclick="editFilterWidget('${col}')">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-link btn-sm text-danger p-0" onclick="removeFilter('${col}')">
                        <i class="fas fa-times"></i>
                    </button>
                </div>
            </div>
            <div class="card-body p-2 d-flex flex-column" id="filter-widget-${col}" style="overflow: visible;">
                <div class="text-center"><div class="spinner-border spinner-border-sm"></div></div>
            </div>
        </div>
    `;

    const node = {
        w: w,
        h: h,
        content: content,
        id: `widget_filter_${col}` // helpful for saving state later
    };
    
    grid.addWidget(node);
    
    // Allow overflow for filter widgets to support dropdowns
    // Use a small timeout to ensure DOM is updated
    setTimeout(() => {
        const widgetEl = document.getElementById(`filter-widget-${col}`);
        if (widgetEl) {
            const contentEl = widgetEl.closest('.grid-stack-item-content');
            if (contentEl) {
                contentEl.style.overflow = 'visible';
                // Also ensure the item itself doesn't clip if it has overflow hidden
                const itemEl = contentEl.closest('.grid-stack-item');
                if (itemEl) {
                    itemEl.style.overflow = 'visible';
                    // Ensure high z-index so it floats above other widgets
                    // But we can't permanently set high z-index or it might obscure others
                    // Bootstrap dropdowns with boundary='window' should handle this, 
                    // but overflow: visible on containers is crucial.
                }
            }
        }
    }, 50);
    
    // Update State
    dashboardState.widgets[node.id] = {
        id: node.id,
        type: 'filter',
        column: col,
        title: col,
        style: style,
        orientation: orientation
    };
    
    // Render inner content
    await renderFilterWidgetContent(col, style, orientation);
}

async function renderFilterWidgetContent(col, style, orientation = 'vertical') {
    const container = document.getElementById(`filter-widget-${col}`);
    if (!container) return;

    // Optimization: Check if already rendered to avoid scroll reset
    const listContainerId = style === 'dropdown' ? `dd_list_${col}` : `list_scroll_${col}`;
    const listContainer = document.getElementById(listContainerId);
    
    if (listContainer && listContainer.children.length > 0) {
        // Already rendered, just update checkboxes
        const currentFilters = dashboardState.filters[col] || [];
        
        const checkboxes = listContainer.querySelectorAll(`.filter-widget-chk-${col}`);
        checkboxes.forEach(cb => {
            cb.checked = currentFilters.includes(cb.value);
        });
        
        // Update Select All
        const selectAllId = style === 'dropdown' ? `dd_all_${col}` : `list_all_${col}`;
        const selectAll = document.getElementById(selectAllId);
        if (selectAll) {
             const checkedCount = listContainer.querySelectorAll(`.filter-widget-chk-${col}:checked`).length;
             selectAll.checked = (checkedCount === checkboxes.length);
        }
        
        // Update Dropdown Button Text
        if (style === 'dropdown') {
             const btn = container.querySelector('.dropdown-toggle');
             if (btn) {
                 const total = checkboxes.length;
                 const selected = currentFilters.length;
                 btn.innerText = (selected === total) ? 'All Selected' : `${selected} selected`;
             }
        }
        return;
    }

    // Get values again (or use cached if we had a robust cache, but fetching is safer for now)
    const res = await eel.get_unique_values(col)();
    
    if (!res.success) {
        container.innerHTML = `<div class="text-danger small">Error loading values</div>`;
        return;
    }

    container.innerHTML = '';
    const currentFilters = dashboardState.filters[col] || [];
    const allChecked = res.values.length === currentFilters.length;

    if (style === 'dropdown') {
        // Dropdown Mode
        // We need a unique ID for the dropdown
        const dropdownId = `dd_${col}_${Date.now()}`;
        
        const wrapper = document.createElement('div');
        wrapper.className = 'dropdown w-100';
        
        wrapper.innerHTML = `
            <button class="btn btn-outline-secondary dropdown-toggle w-100 text-start text-truncate" 
                    type="button" 
                    id="${dropdownId}" 
                    data-bs-toggle="dropdown" 
                    data-bs-auto-close="outside"
                    data-bs-boundary="window"
                    aria-expanded="false">
                ${allChecked ? 'All Selected' : `${currentFilters.length} selected`}
            </button>
            <div class="dropdown-menu w-100 shadow p-0" aria-labelledby="${dropdownId}">
                <div class="p-2 border-bottom">
                    <div class="form-check">
                        <input class="form-check-input" type="checkbox" id="dd_all_${col}" ${allChecked ? 'checked' : ''} onchange="toggleFilterWidgetAll('${col}', this)">
                        <label class="form-check-label fw-bold small" for="dd_all_${col}">Select All</label>
                    </div>
                </div>
                <div class="p-2 custom-scrollbar" style="max-height: 200px; overflow-y: auto;" id="dd_list_${col}">
                </div>
                <div class="p-2 border-top bg-light text-end">
                     <button class="btn btn-sm btn-primary py-0" onclick="applyFilterWidget('${col}')">Apply</button>
                </div>
            </div>
        `;
        
        container.appendChild(wrapper);
        
        const listDiv = wrapper.querySelector(`#dd_list_${col}`);
        res.values.forEach(val => {
            const div = document.createElement('div');
            div.className = 'form-check';
            const safeVal = String(val).replace(/[^a-zA-Z0-9]/g, '_');
            const id = `fw_chk_${col}_${safeVal}`;
            const isChecked = currentFilters.includes(val);
            
            div.innerHTML = `
                <input class="form-check-input filter-widget-chk-${col}" type="checkbox" value="${val}" id="${id}" ${isChecked ? 'checked' : ''}>
                <label class="form-check-label text-break small" for="${id}">${val}</label>
            `;
            listDiv.appendChild(div);
        });

    } else {
        // List Mode (Expanded)
        // Container itself is the list with fixed header/footer
        container.style.overflow = 'hidden'; // Prevent body scroll, use internal scroll
        container.className = 'card-body p-0 d-flex flex-column';
        
        // Set layout style based on orientation
        const layoutStyle = orientation === 'horizontal' 
            ? 'display: flex; flex-direction: row; flex-wrap: wrap; gap: 8px; align-content: flex-start; align-items: center;' 
            : 'display: flex; flex-direction: column; gap: 2px;';

        container.innerHTML = `
            <div class="p-2 border-bottom bg-white">
                <div class="form-check mb-0">
                    <input class="form-check-input" type="checkbox" id="list_all_${col}" ${allChecked ? 'checked' : ''} onchange="toggleFilterWidgetAll('${col}', this)">
                    <label class="form-check-label fw-bold small" for="list_all_${col}">Select All</label>
                </div>
            </div>
            <div class="p-2 flex-grow-1 custom-scrollbar" style="overflow-y: auto; ${layoutStyle}" id="list_scroll_${col}">
            </div>
            <div class="p-2 border-top bg-light text-end">
                 <button class="btn btn-sm btn-primary py-0" onclick="applyFilterWidget('${col}', true)">Apply</button>
            </div>
        `;

        const listDiv = container.querySelector(`#list_scroll_${col}`);

        res.values.forEach(val => {
            const div = document.createElement('div');
            // For horizontal layout, give items some width constraints or let them flow
            div.className = orientation === 'horizontal' 
                ? 'form-check mb-0 me-0 border rounded px-2 py-1 bg-light' // Box style for horizontal
                : 'form-check mb-0';
                
            const safeVal = String(val).replace(/[^a-zA-Z0-9]/g, '_');
            const id = `fw_chk_list_${col}_${safeVal}`;
            const isChecked = currentFilters.includes(val);
            
            div.innerHTML = `
                <input class="form-check-input filter-widget-chk-${col}" type="checkbox" value="${val}" id="${id}" ${isChecked ? 'checked' : ''}>
                <label class="form-check-label text-break small" for="${id}">${val}</label>
            `;
            listDiv.appendChild(div);
        });
    }
}

function toggleFilterWidgetAll(col, source, autoApply = false) {
    const checkboxes = document.querySelectorAll(`.filter-widget-chk-${col}`);
    checkboxes.forEach(cb => cb.checked = source.checked);
    if (autoApply) {
        applyFilterWidget(col, true);
    }
}

function applyFilterWidget(col, isListMode = false) {
    const checkboxes = document.querySelectorAll(`.filter-widget-chk-${col}:checked`);
    const values = Array.from(checkboxes).map(cb => cb.value);
    
    if (values.length > 0) {
        dashboardState.filters[col] = values;
    } else {
        // User deselected everything. 
        // In widget mode, maybe we keep the empty filter (shows no data)? 
        // Or remove filter? Usually shows no data.
        // Let's keep it empty array to show 0 results, effectively filtering everything out.
        dashboardState.filters[col] = []; 
    }

    // If dropdown mode, update button text
    if (!isListMode) {
        const btn = document.querySelector(`#filter-widget-${col} .dropdown-toggle`);
        if (btn) {
            const total = document.querySelectorAll(`.filter-widget-chk-${col}`).length;
            if (values.length === total) {
                btn.innerText = 'All Selected';
            } else {
                btn.innerText = `${values.length} selected`;
            }
            
            // Close dropdown
            const bsDropdown = bootstrap.Dropdown.getInstance(btn);
            if (bsDropdown) bsDropdown.hide();
        }
    }
    
    refreshAllWidgets();
}

function renderFiltersList() {
    // Deprecated: Filters are now widgets on the dashboard
    // Keeping empty function if anything calls it to avoid errors
    const container = document.getElementById('filtersContainer');
    if (container) container.innerHTML = '';
}

async function loadFilterDropdown(col) {
    const menu = document.getElementById(`menu_${col}`);
    const btn = document.getElementById(`dropdown_${col}`);
    
    // Scroll button into view to help user see context
    setTimeout(() => {
        btn.scrollIntoView({behavior: 'smooth', block: 'nearest'});
    }, 100);

    // If already loaded (has form-check children), don't reload
    if (menu.classList.contains('loaded')) return;
    
    menu.innerHTML = '<div class="text-center p-2"><div class="spinner-border spinner-border-sm text-primary"></div> Loading...</div>';
    
    const res = await eel.get_unique_values(col)();
    
    if (res.success) {
        menu.innerHTML = '';
        menu.classList.add('loaded');
        
        const currentFilters = dashboardState.filters[col] || [];
        
        // Select All Option
        const allDiv = document.createElement('div');
        allDiv.className = 'form-check border-bottom pb-2 mb-2';
        const allChecked = res.values.length === currentFilters.length;
        
        allDiv.innerHTML = `
            <input class="form-check-input" type="checkbox" id="selectAll_${col}" ${allChecked ? 'checked' : ''} onchange="toggleSidebarFilterAll('${col}', this)">
            <label class="form-check-label fw-bold small" for="selectAll_${col}">Select All</label>
        `;
        menu.appendChild(allDiv);
        
        // Values List Container (scrollable)
        const listContainer = document.createElement('div');
        listContainer.style.maxHeight = '250px'; // Increased height for better visibility
        listContainer.style.overflowY = 'auto';
        listContainer.className = 'custom-scrollbar'; // Optional styling hook
        
        res.values.forEach(val => {
            const div = document.createElement('div');
            div.className = 'form-check';
            // safe id
            const safeVal = String(val).replace(/[^a-zA-Z0-9]/g, '_');
            const id = `chk_${col}_${safeVal}`;
            const isChecked = currentFilters.includes(val);
            
            div.innerHTML = `
                <input class="form-check-input sidebar-filter-chk-${col}" type="checkbox" value="${val}" id="${id}" ${isChecked ? 'checked' : ''}>
                <label class="form-check-label text-break small" for="${id}">${val}</label>
            `;
            listContainer.appendChild(div);
        });
        menu.appendChild(listContainer);
        
        // Actions Footer
        const footer = document.createElement('div');
        footer.className = 'border-top pt-2 mt-2 d-flex justify-content-end sticky-bottom bg-white';
        footer.innerHTML = `
            <button class="btn btn-sm btn-primary py-0" onclick="applySidebarFilter('${col}')">Apply</button>
        `;
        menu.appendChild(footer);
        
    } else {
        menu.innerHTML = `<div class="text-danger small p-2">Error: ${res.error}</div>`;
    }
}

function toggleSidebarFilterAll(col, source) {
    const checkboxes = document.querySelectorAll(`.sidebar-filter-chk-${col}`);
    checkboxes.forEach(cb => cb.checked = source.checked);
}

function applySidebarFilter(col) {
    const checkboxes = document.querySelectorAll(`.sidebar-filter-chk-${col}:checked`);
    const values = Array.from(checkboxes).map(cb => cb.value);
    
    // Close dropdown (optional, or keep open)
    // To close, we can click the button or use BS API. 
    // Let's toggle the dropdown button to close it.
    const btn = document.getElementById(`dropdown_${col}`);
    if (btn) {
        const bsDropdown = bootstrap.Dropdown.getInstance(btn);
        if (bsDropdown) bsDropdown.hide();
    }

    if (values.length > 0) {
        dashboardState.filters[col] = values;
    } else {
        // If nothing selected, keep it but empty? Or remove?
        // User usually expects at least one value. If empty, maybe remove filter?
        // Let's remove filter if empty, consistent with previous logic.
        delete dashboardState.filters[col];
    }
    
    renderFiltersList();
    refreshAllWidgets();
}

function refreshAllWidgets() {
    Object.keys(dashboardState.widgets).forEach(id => {
        renderWidget(id);
    });
}

function clearFilterWidgetsContent() {
    Object.values(dashboardState.widgets).forEach(w => {
        if (w.type === 'filter') {
             const container = document.getElementById(`filter-widget-${w.column}`);
             if (container) container.innerHTML = ''; // Force re-render next time renderWidget is called
        }
    });
}

// Dashboard Name Handler
document.addEventListener('DOMContentLoaded', function() {
    const nameInput = document.getElementById('dashboardNameInput');
    const title = document.getElementById('dashboardTitle');
    
    if (nameInput && title) {
        nameInput.addEventListener('input', function() {
            title.innerText = this.value;
        });
    }
});

function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    const toggleBtn = document.querySelector('.sidebar-toggle-area');
    
    sidebar.classList.toggle('collapsed');
    
    // Show/Hide toggle button area based on sidebar state
    if (sidebar.classList.contains('collapsed')) {
        toggleBtn.style.display = 'block';
    } else {
        toggleBtn.style.display = 'none';
    }
    
    // Trigger resize for charts after transition
    setTimeout(() => {
        // Resize all plots
        document.querySelectorAll('.js-plotly-plot').forEach(el => {
            Plotly.Plots.resize(el);
        });
    }, 350); // slightly longer than CSS transition
}



// SQL Transform
function showSqlModal() {
    if (!dashboardState.fileLoaded) {
        alert('Please load a data file first.');
        return;
    }
    
    // Populate columns list
    const colsContainer = document.getElementById('sqlColumnsList');
    colsContainer.innerHTML = '';
    dashboardState.columns.forEach(col => {
        const span = document.createElement('span');
        span.className = 'badge bg-secondary me-1 mb-1';
        span.innerText = col;
        span.style.cursor = 'pointer';
        span.title = 'Click to copy';
        span.onclick = () => {
            const textarea = document.getElementById('sqlInput');
            const val = textarea.value;
            const start = textarea.selectionStart;
            const end = textarea.selectionEnd;
            textarea.value = val.substring(0, start) + col + val.substring(end);
            textarea.focus();
            textarea.selectionStart = textarea.selectionEnd = start + col.length;
        };
        colsContainer.appendChild(span);
    });
    
    document.getElementById('sqlInput').value = '';
    document.getElementById('sqlError').innerText = '';
    const modal = new bootstrap.Modal(document.getElementById('sqlTransformModal'));
    modal.show();
}

async function runSqlTransform() {
    const query = document.getElementById('sqlInput').value;
    if (!query) return;
    
    const btn = document.querySelector('#sqlTransformModal .btn-danger');
    const originalText = btn.innerText;
    btn.innerText = 'Executing...';
    btn.disabled = true;
    
    const res = await eel.transform_data(query)();
    
    if (res.success) {
        // Update local state
        dashboardState.columns = res.columns;
        
        // Update UI
        document.getElementById('fileInfo').innerText = `${dashboardState.fileType.toUpperCase()} loaded: ${res.row_count.toLocaleString()} rows`;
        
        // Close Modal
        const modalEl = document.getElementById('sqlTransformModal');
        const modal = bootstrap.Modal.getInstance(modalEl);
        modal.hide();
        
        clearFilterWidgetsContent();

        // Refresh all widgets
        Object.keys(dashboardState.widgets).forEach(id => {
            renderWidget(id);
        });
        
        alert(res.message);
    } else {
        document.getElementById('sqlError').innerText = res.error;
    }
    
    btn.innerText = originalText;
    btn.disabled = false;
}
