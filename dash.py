import eel
import duckdb
import polars as pl
import os
import sys
import json
import datetime

# Initialize Eel with web_dashboard folder
eel.init('web_dashboard')

# Global variables
current_data = None
current_file_path = None
current_file_type = None
startup_file_path = None

if len(sys.argv) > 1:
    startup_file_path = sys.argv[1]

@eel.expose
def get_startup_file():
    """Get the file path passed as command line argument"""
    return startup_file_path


@eel.expose
def load_file(file_path, file_type):
    """Load CSV, Excel, or Parquet file with dynamic file browser"""
    global current_data, current_file_path, current_file_type
    
    try:
        # Normalize path
        file_path = os.path.normpath(file_path)
        
        if not os.path.exists(file_path):
            return {
                'success': False,
                'error': f'File not found: {file_path}'
            }
        
        current_file_path = file_path
        current_file_type = file_type
        
        # Always use DuckDB for all file types (best for large data)
        if current_data:
            try:
                current_data.close()
            except:
                pass
        
        conn = duckdb.connect(':memory:')
        
        if file_type == 'csv':
            # Load CSV with DuckDB - handles billions of rows efficiently
            conn.execute(f"""
                CREATE TABLE data AS 
                SELECT * FROM read_csv_auto('{file_path}', 
                    sample_size=100000,
                    ignore_errors=true
                )
            """)
            
        elif file_type == 'parquet':
            # Load Parquet with DuckDB - zero-copy, super fast
            conn.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{file_path}')")
            
        elif file_type == 'excel':
            # Load Excel via Polars then to DuckDB
            df = pl.read_excel(file_path)
            conn.register('temp_df', df)
            conn.execute("CREATE TABLE data AS SELECT * FROM temp_df")
        
        current_data = conn
        
        # Get column info
        columns = conn.execute("DESCRIBE data").fetchall()
        column_names = [col[0] for col in columns]
        
        # Get row count efficiently
        row_count = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
        
        return {
            'success': True,
            'columns': column_names,
            'row_count': row_count,
            'message': f'Successfully loaded {file_type.upper()} file with {row_count:,} rows'
        }
        
    except Exception as e:
        import traceback
        return {
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }

@eel.expose
def browse_file():
    """Open native file browser dialog"""
    import tkinter as tk
    from tkinter import filedialog
    
    try:
        root = tk.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        
        file_path = filedialog.askopenfilename(
            title='Select Data File',
            filetypes=[
                ('All Supported', '*.csv *.parquet *.pq *.xlsx *.xls'),
                ('CSV Files', '*.csv'),
                ('Parquet Files', '*.parquet *.pq'),
                ('Excel Files', '*.xlsx *.xls'),
                ('All Files', '*.*')
            ]
        )
        
        root.destroy()
        
        if file_path:
            # Auto-detect file type
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                file_type = 'csv'
            elif ext in ['.parquet', '.pq']:
                file_type = 'parquet'
            elif ext in ['.xlsx', '.xls']:
                file_type = 'excel'
            else:
                file_type = 'csv'  # default
            
            return {
                'success': True,
                'file_path': file_path,
                'file_type': file_type
            }
        else:
            return {'success': False, 'cancelled': True}
            
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@eel.expose
def get_unique_values(column_name):
    """Get unique values for a column to populate filter options"""
    global current_data
    try:
        if current_data is None:
            return {'error': 'No data loaded'}
        
        # Get distinct values, limited to 100 to prevent UI overload
        query = f"SELECT DISTINCT \"{column_name}\" FROM data ORDER BY \"{column_name}\" LIMIT 100"
        result = current_data.execute(query).fetchall()
        values = [row[0] for row in result if row[0] is not None]
        
        return {
            'success': True,
            'values': values
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}

@eel.expose
def get_chart_data(chart_config, filters):
    """
    Get data for a specific chart based on configuration and global filters.
    chart_config: { type: 'bar'|'line'|'scatter'|'pie', x: col, y: col|list, agg: 'count'|'sum'|'avg', legend: col }
    filters: { col: [val1, val2], ... }
    """
    global current_data
    try:
        if current_data is None:
            return {'error': 'No data loaded'}
        
        where_clauses = []
        if filters:
            # Ensure filters is a dict (defensive)
            if isinstance(filters, str):
                try:
                    filters = json.loads(filters)
                except:
                    pass
                    
            if isinstance(filters, dict):
                for col, values in filters.items():
                    if values and len(values) > 0:
                        # Handle string escaping safely and quote column name
                        val_str = ', '.join([f"'{str(v).replace('\'', '\'\'')}'" for v in values])
                        where_clauses.append(f"\"{col}\" IN ({val_str})")
        
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        x_col = chart_config.get('x')
        time_group = chart_config.get('timeGroup')
        y_val = chart_config.get('y')
        legend_col = chart_config.get('legend')
        agg = chart_config.get('agg', 'count')
        chart_type = chart_config.get('type', 'bar')
        
        # Prepare X-axis expression with optional date grouping
        # We use TRY_CAST to handle potential string columns safely
        x_expr = f'"{x_col}"'
        if time_group == 'year':
            # Use CAST to INT for just year number (2024)
            x_expr = f"CAST(date_part('year', TRY_CAST(\"{x_col}\" AS DATE)) AS INT)"
        elif time_group == 'month':
            x_expr = f"date_trunc('month', TRY_CAST(\"{x_col}\" AS DATE))"
        elif time_group == 'day':
            x_expr = f"date_trunc('day', TRY_CAST(\"{x_col}\" AS DATE))"
        
        # Normalize y_val to list if it's a string
        y_cols = [y_val] if isinstance(y_val, str) else (y_val if y_val else [])
        
        if chart_type == 'table':
            # Table Widget: Fetch raw data for selected columns
            table_cols = chart_config.get('columns', [])
            if not table_cols:
                # Default to all columns if none specified, but limit to prevent overload
                cols_res = current_data.execute("DESCRIBE data").fetchall()
                table_cols = [c[0] for c in cols_res][:10] # Limit to first 10 columns by default
            
            # Secure column names
            safe_cols = [f'"{c}"' for c in table_cols]
            select_sql = ", ".join(safe_cols)
            
            query = f"SELECT {select_sql} FROM data {where_sql} LIMIT 1000"
            
        elif chart_type == 'filter':
            # Filter Widget: Return unique values for the column
            col_name = chart_config.get('column')
            if not col_name:
                return {'error': 'No column specified'}
            
            # Get distinct values
            query = f"SELECT DISTINCT \"{col_name}\" FROM data ORDER BY \"{col_name}\" LIMIT 100"
            result = current_data.execute(query).fetchall()
            values = [row[0] for row in result if row[0] is not None]
            return {'values': values}

        elif chart_type == 'pie':
            # Pie charts typically use one value column. Use the first one if multiple are sent.
            # y_col here acts as the value source.
            y_col = y_cols[0] if y_cols else None
            
            if y_col and agg in ['sum', 'avg']: # Pie doesn't really do avg well visually, but we'll support sum
                 query = f"""
                    SELECT {x_expr} as label, ROUND(SUM({y_col}), 2) as value 
                    FROM data {where_sql} 
                    GROUP BY label 
                    ORDER BY value DESC 
                    LIMIT 20
                """
            else: # Default to count
                query = f"""
                    SELECT {x_expr} as label, COUNT(*) as value 
                    FROM data {where_sql} 
                    GROUP BY label 
                    ORDER BY value DESC 
                    LIMIT 20
                """
        
        elif chart_type in ['bar', 'line']:
            # Group by X (and Legend if present)
            
            if legend_col:
                # With Legend, we group by X and Legend
                # We force single Y metric for simplicity when splitting by legend
                y_target = y_cols[0] if y_cols else None
                
                if agg == 'count':
                    query = f"""
                        SELECT {x_expr} as x, {legend_col} as color, COUNT(*) as count 
                        FROM data {where_sql} 
                        GROUP BY x, color 
                        ORDER BY x, color
                    """
                elif agg in ['sum', 'avg'] and y_target:
                    query = f"""
                        SELECT {x_expr} as x, {legend_col} as color, ROUND({agg.upper()}({y_target}), 2) as y
                        FROM data {where_sql} 
                        GROUP BY x, color 
                        ORDER BY x, color
                    """
                else: # Fallback
                     query = f"SELECT {x_expr} as x, {legend_col} as color, COUNT(*) as y FROM data {where_sql} GROUP BY x, color ORDER BY x"
            
            else:
                # No Legend - Standard
                if agg == 'count':
                     query = f"""
                        SELECT {x_expr} as x, COUNT(*) as count 
                        FROM data {where_sql} 
                        GROUP BY x 
                        ORDER BY x
                    """
                elif agg in ['sum', 'avg'] and y_cols:
                    # Generate dynamic aggregation for each Y column
                    select_parts = [f"{x_expr} as x"]
                    for col in y_cols:
                        select_parts.append(f"ROUND({agg.upper()}({col}), 2) as \"{col}\"")
                    
                    select_sql = ", ".join(select_parts)
                    query = f"""
                        SELECT {select_sql}
                        FROM data {where_sql} 
                        GROUP BY x 
                        ORDER BY x
                    """
                else: # Fallback or raw
                     query = f"SELECT {x_expr} as x, COUNT(*) as y FROM data {where_sql} GROUP BY x ORDER BY x"
        
        else: # Scatter or raw data
            # For scatter, we usually just plot raw points. 
            # We generally don't group dates for scatter unless requested, but scatter implies raw points.
            # If user requests time grouping on scatter, we essentially turn it into an aggregate plot which might be confusing.
            # But for consistency, let's apply it if requested, though scatter usually doesn't aggregate.
            # Wait, if I group by date, I must aggregate Y. Scatter without aggregation is just dots.
            # If user selects 'Day' grouping, they probably expect one dot per day?
            # If 'agg' is count/sum, it becomes a bubble chart or similar.
            # The current scatter implementation does NOT aggregate (it limits to 5000).
            # So let's IGNORE time grouping for Scatter for now to keep it simple, 
            # OR if we really want to support it, we'd have to switch to aggregation logic.
            # Let's stick to raw values for Scatter to avoid breaking its contract.
            
            y_target = y_cols[0] if y_cols else None
            
            if legend_col:
                if y_target:
                    query = f"SELECT {x_col} as x, {legend_col} as color, {y_target} as y FROM data {where_sql} LIMIT 5000"
                else:
                    query = f"SELECT {x_col} as x, {legend_col} as color, 1 as y FROM data {where_sql} LIMIT 5000"
            else:
                # If multiple Ys, we fetch them all.
                select_parts = [f"{x_col} as x"]
                for col in y_cols:
                    select_parts.append(f"{col} as \"{col}\"")
                
                if not y_cols: # Fallback
                    select_parts.append("1 as y")

                select_sql = ", ".join(select_parts)
                query = f"SELECT {select_sql} FROM data {where_sql} LIMIT 5000"

        result_df = current_data.execute(query).fetchdf()
        
        # Convert to JSON compatible format (handles Timestamps/Dates correctly)
        return json.loads(result_df.to_json(orient='records', date_format='iso'))
        
    except Exception as e:
        return {'error': str(e)}

@eel.expose
def transform_data(sql_query):
    """Execute SQL to transform the data"""
    global current_data
    try:
        if current_data is None:
            return {'success': False, 'error': 'No data loaded'}
        
        # Check if query is a SELECT (simplistic check)
        sql_query = sql_query.strip()
        if sql_query.upper().startswith("SELECT") or sql_query.upper().startswith("WITH"):
            # Create new table from result
            current_data.execute(f"CREATE OR REPLACE TABLE data AS {sql_query}")
        else:
            # Execute DDL/DML directly
            current_data.execute(sql_query)
            
        # Refresh metadata
        columns = current_data.execute("DESCRIBE data").fetchall()
        column_names = [col[0] for col in columns]
        
        row_count = current_data.execute("SELECT COUNT(*) FROM data").fetchone()[0]
        
        return {
            'success': True,
            'columns': column_names,
            'row_count': row_count,
            'message': f'Data transformed successfully. New row count: {row_count:,}'
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

@eel.expose
def export_dashboard(dashboard_state):
    import tkinter as tk
    from tkinter import filedialog
    
    if current_data is None:
        return {'success': False, 'error': 'No data loaded'}

    try:
        root = tk.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        
        path = filedialog.asksaveasfilename(
            defaultextension=".html",
            filetypes=[("HTML Files", "*.html")],
            title="Save Dashboard as HTML"
        )
        root.destroy()
        
        if not path:
            return {'success': False, 'error': 'Cancelled'}
            
        # 1. Get Full Data
        df = current_data.execute("SELECT * FROM data").fetchdf()
        data_json = df.to_json(orient='records', date_format='iso')
        
        # 2. Read Files
        base_dir = 'web_dashboard'
        with open(os.path.join(base_dir, 'standalone_template.html'), 'r', encoding='utf-8') as f:
            template = f.read()
        with open(os.path.join(base_dir, 'style.css'), 'r', encoding='utf-8') as f:
            css = f.read()
        with open(os.path.join(base_dir, 'script.js'), 'r', encoding='utf-8') as f:
            js = f.read()
        with open(os.path.join(base_dir, 'mini_engine.js'), 'r', encoding='utf-8') as f:
            mini_engine = f.read()
            
        # 3. Replace Placeholders
        # Replace "placeholders" (including quotes) with actual data objects
        final_html = template.replace('"{{DATA_JSON}}"', data_json)
        final_html = final_html.replace('"{{STATE_JSON}}"', json.dumps(dashboard_state))
        
        # Replace comment blocks with actual code
        final_html = final_html.replace('/* {{STYLE_CSS}} */', css)
        final_html = final_html.replace('/* {{SCRIPT_JS}} */', js)
        final_html = final_html.replace('/* {{MINI_ENGINE_JS}} */', mini_engine)
        
        with open(path, 'w', encoding='utf-8') as f:
            f.write(final_html)
            
        return {'success': True, 'path': path}
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

if __name__ == '__main__':
    eel.start('index.html', size=(1200, 800))
