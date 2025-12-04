import dash
from dash import dcc, html, Input, Output, State, callback_context
import plotly.express as px
import plotly.graph_objects as go
import plotly.utils
import pandas as pd
import json
import threading
import webbrowser
from datetime import datetime
import numpy as np
from io import StringIO

class PlotlyDashboard:
    def __init__(self, port=8050, initial_data=None):
        self.app = dash.Dash(__name__)
        self.port = port
        self.data = initial_data
        self.column_info = {}
        
        # Process initial data if provided
        if initial_data is not None:
            self.data_json = initial_data.to_json(date_format='iso', orient='split')
        else:
            self.data_json = None
        
        # Add global CSS to remove default browser margins/padding
        self.app.index_string = '''
        <!DOCTYPE html>
        <html>
            <head>
                {%metas%}
                <title>{%title%}</title>
                {%favicon%}
                {%css%}
                <style>
                    html, body {
                        margin: 0 !important;
                        padding: 0 !important;
                        height: 100vh !important;
                        overflow: hidden !important;
                    }
                    #react-entry-point {
                        height: 100vh !important;
                        margin: 0 !important;
                        padding: 0 !important;
                    }
                </style>
            </head>
            <body>
                {%app_entry%}
                <footer>
                    {%config%}
                    {%scripts%}
                    {%renderer%}
                </footer>
            </body>
        </html>
        '''
        
        self.setup_layout()
        self.setup_callbacks()
        
    def setup_layout(self):
        """Setup the dashboard layout with Tableau-like interface"""
        self.app.layout = html.Div([
            # Main Container with Flexbox Layout
            html.Div([
                # Control Panel (Left Side)
                html.Div([
                    html.H3("Chart Configuration", style={'color': '#34495e'}),
                    
                    # Title Input Section
                    html.Div([
                        html.Label("Dashboard Title:", style={'fontWeight': 'bold', 'marginBottom': '5px'}),
                        dcc.Input(
                            id='dashboard-title-input',
                            type='text',
                            value='Data Dashboard',
                            placeholder='Enter dashboard title...',
                            style={
                                'width': '100%',
                                'padding': '8px',
                                'marginBottom': '15px',
                                'border': '1px solid #bdc3c7',
                                'borderRadius': '4px'
                            }
                        )
                    ]),
                    
                    # Export Section
                    html.Div([
                        html.Label("Export Options:", style={'fontWeight': 'bold', 'marginBottom': '10px'}),
                        html.Div([
                            html.Button(
                                'Export as HTML',
                                id='export-html-btn',
                                n_clicks=0,
                                style={
                                    'backgroundColor': '#3498db',
                                    'color': 'white',
                                    'border': 'none',
                                    'padding': '8px 12px',
                                    'marginRight': '10px',
                                    'marginBottom': '10px',
                                    'borderRadius': '4px',
                                    'cursor': 'pointer',
                                    'fontSize': '12px'
                                }
                            ),
                            html.Button(
                                'Export as Image',
                                id='export-image-btn',
                                n_clicks=0,
                                style={
                                    'backgroundColor': '#e74c3c',
                                    'color': 'white',
                                    'border': 'none',
                                    'padding': '8px 12px',
                                    'marginBottom': '10px',
                                    'borderRadius': '4px',
                                    'cursor': 'pointer',
                                    'fontSize': '12px'
                                }
                            )
                        ], style={'display': 'flex', 'flexWrap': 'wrap'}),
                        
                        # Download components (hidden)
                        dcc.Download(id='download-html'),
                        dcc.Download(id='download-image'),
                        
                        # Status message
                        html.Div(id='export-status', style={'marginTop': '10px', 'fontSize': '12px'})
                    ], style={'marginBottom': '20px'}),
                    
                    # Available Fields Section
                html.Div([
                    html.H4("Available Fields", style={'color': '#2c3e50', 'marginBottom': '10px'}),
                    html.Div(
                        id='field-list',
                        style={
                            'border': '2px dashed #bdc3c7',
                            'borderRadius': '5px',
                            'padding': '10px',
                            'minHeight': '120px',
                            'backgroundColor': '#ffffff',
                            'marginBottom': '20px'
                        }
                    )
                ]),
                    
                    # Chart Type Selection
                    html.Label("Chart Type:", style={'fontWeight': 'bold', 'marginTop': '10px'}),
                    dcc.Dropdown(
                        id='chart-type',
                        options=[
                            {'label': 'Bar Chart', 'value': 'bar'},
                            {'label': 'Line Chart', 'value': 'line'},
                            {'label': 'Scatter Plot', 'value': 'scatter'},
                            {'label': 'Pie Chart', 'value': 'pie'},
                            {'label': 'Histogram', 'value': 'histogram'},
                            {'label': 'Box Plot', 'value': 'box'},
                            {'label': 'Heatmap', 'value': 'heatmap'},
                            {'label': 'Area Chart', 'value': 'area'}
                        ],
                        value='bar',
                        style={'marginBottom': '15px'}
                    ),
                    
                    # Drop Zones Section
                    html.Div([
                        # X-Axis Drop Zone
                        html.Div([
                            html.Div([
                                html.Label("X-Axis:", style={'fontWeight': 'bold', 'marginBottom': '5px', 'display': 'inline-block'}),
                                html.Button("×", id='clear-x-axis', 
                                          style={'marginLeft': '10px', 'backgroundColor': '#e74c3c', 'color': 'white', 
                                                'border': 'none', 'borderRadius': '50%', 'width': '20px', 'height': '20px',
                                                'fontSize': '12px', 'cursor': 'pointer', 'display': 'inline-block'})
                            ], style={'display': 'flex', 'alignItems': 'center'}),
                            html.Div(
                                id='x-axis-drop-zone',
                                children=[html.Div("Drop field here or click a field to assign to X-axis", style={'color': '#7f8c8d', 'fontStyle': 'italic'})],
                                style={
                                    'border': '2px dashed #3498db',
                                    'borderRadius': '5px',
                                    'padding': '10px',
                                    'minHeight': '40px',
                                    'backgroundColor': '#ecf0f1',
                                    'marginBottom': '15px',
                                    'textAlign': 'center'
                                }
                            )
                        ]),
                        
                        # Y-Axis Drop Zone
                        html.Div([
                            html.Div([
                                html.Label("Y-Axis (Multiple):", style={'fontWeight': 'bold', 'marginBottom': '5px', 'display': 'inline-block'}),
                                html.Button("×", id='clear-y-axis', 
                                          style={'marginLeft': '10px', 'backgroundColor': '#e74c3c', 'color': 'white', 
                                                'border': 'none', 'borderRadius': '50%', 'width': '20px', 'height': '20px',
                                                'fontSize': '12px', 'cursor': 'pointer', 'display': 'inline-block'})
                            ], style={'display': 'flex', 'alignItems': 'center'}),
                            html.Div(
                                id='y-axis-drop-zone',
                                children=[html.Div("Drop fields here or click fields to assign to Y-axis (supports multiple)", style={'color': '#7f8c8d', 'fontStyle': 'italic'})],
                                style={
                                    'border': '2px dashed #e74c3c',
                                    'borderRadius': '5px',
                                    'padding': '10px',
                                    'minHeight': '60px',
                                    'backgroundColor': '#ecf0f1',
                                    'marginBottom': '15px',
                                    'textAlign': 'center',
                                    'display': 'flex',
                                    'flexWrap': 'wrap',
                                    'gap': '5px',
                                    'alignItems': 'center',
                                    'justifyContent': 'center'
                                }
                            )
                        ]),
                        
                        # Color Drop Zone
                        html.Div([
                            html.Div([
                                html.Label("Color/Group By:", style={'fontWeight': 'bold', 'marginBottom': '5px', 'display': 'inline-block'}),
                                html.Button("×", id='clear-color', 
                                          style={'marginLeft': '10px', 'backgroundColor': '#e74c3c', 'color': 'white', 
                                                'border': 'none', 'borderRadius': '50%', 'width': '20px', 'height': '20px',
                                                'fontSize': '12px', 'cursor': 'pointer', 'display': 'inline-block'})
                            ], style={'display': 'flex', 'alignItems': 'center'}),
                            html.Div(
                                id='color-drop-zone',
                                children=[html.Div("Drop field here or click a field to assign to Color", style={'color': '#7f8c8d', 'fontStyle': 'italic'})],
                                style={
                                    'border': '2px dashed #f39c12',
                                    'borderRadius': '5px',
                                    'padding': '10px',
                                    'minHeight': '40px',
                                    'backgroundColor': '#ecf0f1',
                                    'marginBottom': '15px',
                                    'textAlign': 'center'
                                }
                            )
                        ]),
                        
                        # Size Drop Zone
                        html.Div([
                            html.Div([
                                html.Label("Size By:", style={'fontWeight': 'bold', 'marginBottom': '5px', 'display': 'inline-block'}),
                                html.Button("×", id='clear-size', 
                                          style={'marginLeft': '10px', 'backgroundColor': '#e74c3c', 'color': 'white', 
                                                'border': 'none', 'borderRadius': '50%', 'width': '20px', 'height': '20px',
                                                'fontSize': '12px', 'cursor': 'pointer', 'display': 'inline-block'})
                            ], style={'display': 'flex', 'alignItems': 'center'}),
                            html.Div(
                                id='size-drop-zone',
                                children=[html.Div("Drop field here or click a field to assign to Size", style={'color': '#7f8c8d', 'fontStyle': 'italic'})],
                                style={
                                    'border': '2px dashed #9b59b6',
                                    'borderRadius': '5px',
                                    'padding': '10px',
                                    'minHeight': '40px',
                                    'backgroundColor': '#ecf0f1',
                                    'marginBottom': '15px',
                                    'textAlign': 'center'
                                }
                            )
                        ])
                    ]),
                    
                    # Aggregation Function
                    html.Label("Aggregation:", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='aggregation',
                        options=[
                            {'label': 'Sum', 'value': 'sum'},
                            {'label': 'Count', 'value': 'count'},
                            {'label': 'Average', 'value': 'mean'},
                            {'label': 'Min', 'value': 'min'},
                            {'label': 'Max', 'value': 'max'},
                            {'label': 'None', 'value': 'none'}
                        ],
                        value='none',
                        style={'marginBottom': '15px'}
                    ),
                    
                    # Data Labels Toggle
                    html.Div([
                        html.Label("Show Data Labels:", style={'fontWeight': 'bold', 'marginBottom': '5px'}),
                        dcc.Checklist(
                            id='show-data-labels',
                            options=[{'label': 'Display values on chart', 'value': 'show'}],
                            value=[],  # Empty by default (unchecked)
                            style={'marginBottom': '15px'}
                        )
                    ]),
                    
                    # Filter Controls
                    html.Div(id='filter-controls'),
                    
                    # Hidden inputs to store dropped values
                    dcc.Store(id='x-axis-store'),
                    dcc.Store(id='y-axis-store'),
                    dcc.Store(id='color-store'),
                    dcc.Store(id='size-store'),
                    dcc.Store(id='selected-field-store'),  # Store for currently selected field
                    dcc.Store(id='drag-drop-trigger'),  # Store to trigger drag-drop events
                    
                ], style={
                    'width': '25%', 
                    'padding': '20px', 
                    'backgroundColor': '#ecf0f1', 
                    'height': '100vh',
                    'boxSizing': 'border-box',
                    'flex': '0 0 25%',
                    'overflowY': 'auto'
                }),
                
                # Main Chart Area (Right Side)
                html.Div([
                    dcc.Graph(
                        id='main-chart',
                        style={'height': 'calc(100vh - 80px)', 'width': '100%'}
                    )
                ], style={
                    'width': '75%', 
                    'padding': '0',
                    'margin': '0',
                    'boxSizing': 'border-box',
                    'flex': '1',
                    'height': '100vh'
                })
                
            ], style={
                'display': 'flex',
                'flexDirection': 'row',
                'height': '100vh',
                'margin': '0',
                'padding': '0'
            }),
            
            # Hidden div to store data
            html.Div(id='data-store', children=self.data_json, style={'display': 'none'})
        ])
    
    def setup_callbacks(self):
        """Setup all dashboard callbacks"""
        
        # Field selection callback - click to select a field
        @self.app.callback(
            [Output('selected-field-store', 'data'),
             Output('field-list', 'children')],
            [Input({'type': 'field-item', 'field': dash.dependencies.ALL}, 'n_clicks')],
            [State('selected-field-store', 'data'),
             State('data-store', 'children')]
        )
        def handle_field_selection(n_clicks_list, selected_field, data_json):
            ctx = callback_context
            if not ctx.triggered or not any(n_clicks_list):
                # Initial load - create field items
                if data_json:
                    data = pd.read_json(StringIO(data_json), orient='split')
                    columns = data.columns.tolist()
                    
                    field_items = []
                    for col in columns:
                        is_selected = selected_field and selected_field.get('field') == col
                        field_items.append(
                            html.Div(
                                col,
                                id={'type': 'field-item', 'field': col},
                                **{'data-field': col},  # Add data attribute for drag handling
                                draggable='true',  # Enable HTML5 dragging
                                style={
                                    'padding': '8px 12px',
                                    'margin': '2px',
                                    'backgroundColor': '#e74c3c' if is_selected else '#3498db',
                                    'color': 'white',
                                    'borderRadius': '4px',
                                    'cursor': 'grab',  # Change cursor to indicate draggable
                                    'fontSize': '12px',
                                    'fontWeight': 'bold',
                                    'textAlign': 'center',
                                    'userSelect': 'none',
                                    'display': 'inline-block',
                                    'minWidth': '80px',
                                    'border': '2px solid #c0392b' if is_selected else '2px solid transparent'
                                },
                                n_clicks=0
                            )
                        )
                    return selected_field, field_items
                return selected_field, []
            
            # Find which field was clicked
            triggered_id = ctx.triggered[0]['prop_id']
            if 'field-item' in triggered_id:
                field_name = eval(triggered_id.split('.')[0])['field']
                new_selected = {'field': field_name}
                
                # Recreate field items with updated selection
                if data_json:
                    data = pd.read_json(StringIO(data_json), orient='split')
                    columns = data.columns.tolist()
                    
                    field_items = []
                    for col in columns:
                        is_selected = col == field_name
                        field_items.append(
                            html.Div(
                                col,
                                id={'type': 'field-item', 'field': col},
                                style={
                                    'padding': '8px 12px',
                                    'margin': '2px',
                                    'backgroundColor': '#e74c3c' if is_selected else '#3498db',
                                    'color': 'white',
                                    'borderRadius': '4px',
                                    'cursor': 'pointer',
                                    'fontSize': '12px',
                                    'fontWeight': 'bold',
                                    'textAlign': 'center',
                                    'userSelect': 'none',
                                    'display': 'inline-block',
                                    'minWidth': '80px',
                                    'border': '2px solid #c0392b' if is_selected else '2px solid transparent'
                                },
                                n_clicks=0
                            )
                        )
                    return new_selected, field_items
                
            return selected_field, dash.no_update
        
        # Drop zone click callbacks - click to assign selected field
        @self.app.callback(
            [Output('x-axis-drop-zone', 'children'),
             Output('x-axis-store', 'data'),
             Output('selected-field-store', 'data', allow_duplicate=True)],
            [Input('x-axis-drop-zone', 'n_clicks')],
            [State('selected-field-store', 'data')],
            prevent_initial_call=True
        )
        def handle_x_axis_assignment(n_clicks, selected_field):
            if not n_clicks or not selected_field or not selected_field.get('field'):
                return dash.no_update, dash.no_update, dash.no_update
            
            field_name = selected_field['field']
            return [
                html.Div(
                    field_name,
                    style={
                        'padding': '8px 12px',
                        'backgroundColor': '#3498db',
                        'color': 'white',
                        'borderRadius': '4px',
                        'fontSize': '12px',
                        'fontWeight': 'bold'
                    }
                )
            ], {'field': field_name}, None  # Clear selection after assignment
            
            return [html.Div("Click a field to assign to X-axis", style={'color': '#7f8c8d', 'fontStyle': 'italic'})], None
        
        @self.app.callback(
            [Output('y-axis-drop-zone', 'children'),
             Output('y-axis-store', 'data'),
             Output('selected-field-store', 'data', allow_duplicate=True)],
            [Input('y-axis-drop-zone', 'n_clicks')],
            [State('selected-field-store', 'data'),
             State('y-axis-store', 'data')],
            prevent_initial_call=True
        )
        def handle_y_axis_assignment(n_clicks, selected_field, current_y_data):
            if not n_clicks or not selected_field or not selected_field.get('field'):
                return dash.no_update, dash.no_update, dash.no_update
            
            field_name = selected_field['field']
            
            # Handle multiple Y-axis fields
            if current_y_data and 'fields' in current_y_data:
                current_fields = current_y_data['fields']
                if field_name not in current_fields:
                    current_fields.append(field_name)
            else:
                current_fields = [field_name]
            
            # Create visual elements for each field
            field_elements = []
            for field in current_fields:
                field_elements.append(
                    html.Div(
                        field,
                        style={
                            'padding': '6px 10px',
                            'backgroundColor': '#e74c3c',
                            'color': 'white',
                            'borderRadius': '4px',
                            'fontSize': '11px',
                            'fontWeight': 'bold',
                            'margin': '2px',
                            'display': 'inline-block'
                        }
                    )
                )
            
            return field_elements, {'fields': current_fields}, None  # Clear selection after assignment
        
        # Callback to process drag-and-drop events
        @self.app.callback(
            [Output('x-axis-store', 'data', allow_duplicate=True),
             Output('y-axis-store', 'data', allow_duplicate=True),
             Output('color-store', 'data', allow_duplicate=True),
             Output('size-store', 'data', allow_duplicate=True),
             Output('x-axis-drop-zone', 'children', allow_duplicate=True),
             Output('y-axis-drop-zone', 'children', allow_duplicate=True),
             Output('color-drop-zone', 'children', allow_duplicate=True),
             Output('size-drop-zone', 'children', allow_duplicate=True)],
            Input('drag-drop-trigger', 'data'),
            [State('y-axis-store', 'data')],
            prevent_initial_call=True
        )
        def process_drag_drop(drag_data, current_y_data):
            if not drag_data or not drag_data.get('field') or not drag_data.get('target'):
                return [dash.no_update] * 8
            
            field_name = drag_data['field']
            target = drag_data['target']
            
            # Create the field display element
            field_display = [
                html.Div(
                    field_name,
                    style={
                        'padding': '8px 12px',
                        'backgroundColor': '#3498db' if target == 'x-axis' else 
                                         '#e74c3c' if target == 'y-axis' else
                                         '#f39c12' if target == 'color' else '#9b59b6',
                        'color': 'white',
                        'borderRadius': '4px',
                        'fontSize': '12px',
                        'fontWeight': 'bold'
                    }
                )
            ]
            
            # Update the appropriate store and display
            if target == 'x-axis':
                return ({'field': field_name}, dash.no_update, dash.no_update, dash.no_update,
                       field_display, dash.no_update, dash.no_update, dash.no_update)
            elif target == 'y-axis':
                # Handle multiple Y-axis fields for drag-drop
                if current_y_data and 'fields' in current_y_data:
                    current_fields = current_y_data['fields']
                    if field_name not in current_fields:
                        current_fields.append(field_name)
                else:
                    current_fields = [field_name]
                
                # Create visual elements for each field
                field_elements = []
                for field in current_fields:
                    field_elements.append(
                        html.Div(
                            field,
                            style={
                                'padding': '6px 10px',
                                'backgroundColor': '#e74c3c',
                                'color': 'white',
                                'borderRadius': '4px',
                                'fontSize': '11px',
                                'fontWeight': 'bold',
                                'margin': '2px',
                                'display': 'inline-block'
                            }
                        )
                    )
                
                return (dash.no_update, {'fields': current_fields}, dash.no_update, dash.no_update,
                       dash.no_update, field_elements, dash.no_update, dash.no_update)
            elif target == 'color':
                return (dash.no_update, dash.no_update, {'field': field_name}, dash.no_update,
                       dash.no_update, dash.no_update, field_display, dash.no_update)
            elif target == 'size':
                return (dash.no_update, dash.no_update, dash.no_update, {'field': field_name},
                       dash.no_update, dash.no_update, dash.no_update, field_display)
            
            return [dash.no_update] * 8
        
        @self.app.callback(
            [Output('color-drop-zone', 'children'),
             Output('color-store', 'data'),
             Output('selected-field-store', 'data', allow_duplicate=True)],
            [Input('color-drop-zone', 'n_clicks')],
            [State('selected-field-store', 'data')],
            prevent_initial_call=True
        )
        def handle_color_assignment(n_clicks, selected_field):
            if not n_clicks or not selected_field or not selected_field.get('field'):
                return dash.no_update, dash.no_update, dash.no_update
            
            field_name = selected_field['field']
            return [
                html.Div(
                    field_name,
                    style={
                        'padding': '8px 12px',
                        'backgroundColor': '#f39c12',
                        'color': 'white',
                        'borderRadius': '4px',
                        'fontSize': '12px',
                        'fontWeight': 'bold'
                    }
                )
            ], {'field': field_name}, None  # Clear selection after assignment
        
        @self.app.callback(
            [Output('size-drop-zone', 'children'),
             Output('size-store', 'data'),
             Output('selected-field-store', 'data', allow_duplicate=True)],
            [Input('size-drop-zone', 'n_clicks')],
            [State('selected-field-store', 'data')],
            prevent_initial_call=True
        )
        def handle_size_assignment(n_clicks, selected_field):
            if not n_clicks or not selected_field or not selected_field.get('field'):
                return dash.no_update, dash.no_update, dash.no_update
            
            field_name = selected_field['field']
            return [
                html.Div(
                    field_name,
                    style={
                        'padding': '8px 12px',
                        'backgroundColor': '#9b59b6',
                        'color': 'white',
                        'borderRadius': '4px',
                        'fontSize': '12px',
                        'fontWeight': 'bold'
                    }
                )
            ], {'field': field_name}, None  # Clear selection after assignment
        
        @self.app.callback(
            Output('main-chart', 'figure'),
            [Input('chart-type', 'value'),
             Input('x-axis-store', 'data'),
             Input('y-axis-store', 'data'),
             Input('color-store', 'data'),
             Input('size-store', 'data'),
             Input('aggregation', 'value'),
             Input('show-data-labels', 'value'),
             Input('dashboard-title-input', 'value'),
             Input('data-store', 'children')]
        )
        def update_chart(chart_type, x_axis_data, y_axis_data, color_data, size_data, aggregation, show_labels, dashboard_title, data_json):
            # Extract field names from store data
            x_axis = x_axis_data.get('field') if x_axis_data else None
            
            # Handle multiple Y-axis fields
            if y_axis_data and 'fields' in y_axis_data:
                y_axis = y_axis_data['fields']  # List of fields
            elif y_axis_data and 'field' in y_axis_data:
                y_axis = [y_axis_data['field']]  # Single field as list for compatibility
            else:
                y_axis = None
                
            color_by = color_data.get('field') if color_data else None
            size_by = size_data.get('field') if size_data else None
            
            # Check if data labels should be shown
            show_data_labels = 'show' in show_labels if show_labels else False
            
            if not data_json or not x_axis:
                return go.Figure().add_annotation(
                    text="Please load data and drag fields to X-axis",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, xanchor='center', yanchor='middle',
                    showarrow=False, font=dict(size=20)
                )
            
            try:
                data = pd.read_json(StringIO(data_json), orient='split')
                
                # Apply aggregation if specified and we have Y-axis fields
                if aggregation != 'none' and y_axis:
                    if aggregation == 'count':
                        data = data.groupby(x_axis).size().reset_index(name='count')
                        y_axis = ['count']
                    else:
                        # For multiple Y fields, aggregate each one
                        if len(y_axis) == 1:
                            agg_func = getattr(data.groupby(x_axis)[y_axis[0]], aggregation)
                            data = agg_func().reset_index()
                        else:
                            # Aggregate multiple Y fields
                            agg_dict = {field: aggregation for field in y_axis}
                            data = data.groupby(x_axis).agg(agg_dict).reset_index()
                
                # Create the chart based on type
                fig = self.create_chart(data, chart_type, x_axis, y_axis, color_by, size_by, show_data_labels, dashboard_title)
                
                # Update layout
                y_title = ', '.join(y_axis) if y_axis and isinstance(y_axis, list) else str(y_axis) if y_axis else 'Y-axis'
                fig.update_layout(
                    xaxis_title=x_axis,
                    yaxis_title=y_title,
                    template="plotly_white",
                    margin=dict(l=0, r=0, t=20, b=0),
                    autosize=True
                )
                
                return fig
                
            except Exception as e:
                return go.Figure().add_annotation(
                    text=f"Error creating chart: {str(e)}",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, xanchor='center', yanchor='middle',
                    showarrow=False, font=dict(size=16, color="red")
                )
        
        # Clear button callbacks
        @self.app.callback(
            [Output('x-axis-drop-zone', 'children', allow_duplicate=True),
             Output('x-axis-store', 'data', allow_duplicate=True)],
            [Input('clear-x-axis', 'n_clicks')],
            prevent_initial_call=True
        )
        def clear_x_axis(n_clicks):
            if n_clicks:
                return [html.Div("Drop field here or click a field to assign to X-axis", 
                               style={'color': '#7f8c8d', 'fontStyle': 'italic'})], None
            return dash.no_update, dash.no_update
        
        @self.app.callback(
            [Output('y-axis-drop-zone', 'children', allow_duplicate=True),
             Output('y-axis-store', 'data', allow_duplicate=True)],
            [Input('clear-y-axis', 'n_clicks')],
            prevent_initial_call=True
        )
        def clear_y_axis(n_clicks):
            if n_clicks:
                return [html.Div("Drop fields here or click fields to assign to Y-axis (Multiple)", 
                               style={'color': '#7f8c8d', 'fontStyle': 'italic'})], None
            return dash.no_update, dash.no_update
        
        @self.app.callback(
            [Output('color-drop-zone', 'children', allow_duplicate=True),
             Output('color-store', 'data', allow_duplicate=True)],
            [Input('clear-color', 'n_clicks')],
            prevent_initial_call=True
        )
        def clear_color(n_clicks):
            if n_clicks:
                return [html.Div("Drop field here or click a field to assign to Color", 
                               style={'color': '#7f8c8d', 'fontStyle': 'italic'})], None
            return dash.no_update, dash.no_update
        
        @self.app.callback(
            [Output('size-drop-zone', 'children', allow_duplicate=True),
             Output('size-store', 'data', allow_duplicate=True)],
            [Input('clear-size', 'n_clicks')],
            prevent_initial_call=True
        )
        def clear_size(n_clicks):
            if n_clicks:
                return [html.Div("Drop field here or click a field to assign to Size", 
                               style={'color': '#7f8c8d', 'fontStyle': 'italic'})], None
            return dash.no_update, dash.no_update
        
        # Export callbacks
        @self.app.callback(
            [Output('download-html', 'data'),
             Output('export-status', 'children')],
            [Input('export-html-btn', 'n_clicks')],
            [State('main-chart', 'figure'),
             State('dashboard-title-input', 'value'),
             State('data-store', 'children')]
        )
        def export_html(n_clicks, figure, title, data_json):
            if n_clicks and n_clicks > 0:
                try:
                    # Create standalone HTML
                    html_content = self.create_standalone_html(figure, title, data_json)
                    filename = f"{title.replace(' ', '_')}_dashboard.html"
                    
                    return dict(content=html_content, filename=filename), "✓ HTML exported successfully!"
                except Exception as e:
                    return dash.no_update, f"❌ Export failed: {str(e)}"
            return dash.no_update, ""
        
        @self.app.callback(
            [Output('download-image', 'data'),
             Output('export-status', 'children', allow_duplicate=True)],
            [Input('export-image-btn', 'n_clicks')],
            [State('main-chart', 'figure'),
             State('dashboard-title-input', 'value')],
            prevent_initial_call=True
        )
        def export_image(n_clicks, figure, title):
            if n_clicks and n_clicks > 0:
                try:
                    # Try to import required dependencies
                    try:
                        import plotly.io as pio
                        import kaleido
                    except ImportError as ie:
                        # If kaleido is not available, try alternative method
                        return dash.no_update, f"❌ Export failed: Missing dependency. Please install: pip install kaleido"
                    
                    # Ensure title is not None
                    if not title:
                        title = "Data_Dashboard"
                    
                    # Convert figure to image
                    img_bytes = pio.to_image(
                        figure, 
                        format='png', 
                        width=1200, 
                        height=800,
                        engine='kaleido'
                    )
                    
                    # Create safe filename
                    safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    filename = f"{safe_title.replace(' ', '_')}_chart.png"
                    
                    # Encode as base64 for download
                    import base64
                    img_b64 = base64.b64encode(img_bytes).decode()
                    
                    return dict(
                        content=img_b64, 
                        filename=filename, 
                        base64=True,
                        type='image/png'
                    ), "✓ Image exported successfully!"
                    
                except Exception as e:
                    error_msg = str(e)
                    if "kaleido" in error_msg.lower():
                        return dash.no_update, "❌ Export failed: Please install kaleido: pip install kaleido"
                    else:
                        return dash.no_update, f"❌ Export failed: {error_msg}"
            return dash.no_update, ""
    
    def create_standalone_html(self, figure, title, data_json):
        """Create a standalone HTML file with the dashboard"""
        import plotly.offline as pyo
        import plotly.graph_objects as go
        
        # Normalize figure object to ensure it has proper structure
        if figure is None:
            # Create empty figure if none provided
            normalized_figure = {'data': [], 'layout': {}}
        elif hasattr(figure, 'to_dict'):
            # Handle plotly Figure objects
            normalized_figure = figure.to_dict()
        elif isinstance(figure, dict):
            # Handle dictionary figures
            normalized_figure = figure.copy()
            # Ensure it has data and layout keys
            if 'data' not in normalized_figure:
                normalized_figure['data'] = []
            if 'layout' not in normalized_figure:
                normalized_figure['layout'] = {}
        else:
            # Fallback: try to convert to dict
            try:
                normalized_figure = dict(figure)
                if 'data' not in normalized_figure:
                    normalized_figure['data'] = []
                if 'layout' not in normalized_figure:
                    normalized_figure['layout'] = {}
            except:
                # Last resort: create empty figure
                normalized_figure = {'data': [], 'layout': {}}
        
        # Create the HTML content
        html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        html, body {{
            height: 100vh;
            width: 100vw;
            font-family: Arial, sans-serif;
            background-color: #f8f9fa;
            overflow-x: hidden;
        }}
        .container {{
            width: 100vw;
            height: 100vh;
            background-color: white;
            display: flex;
            flex-direction: column;
        }}
        .chart-container {{
            flex: 1;
            width: 100%;
            min-height: 0;
            padding: 10px;
        }}
        .footer {{
            text-align: center;
            padding: 10px;
            color: #7f8c8d;
            font-size: 12px;
            background-color: #ecf0f1;
            border-top: 1px solid #bdc3c7;
            flex-shrink: 0;
        }}
        @media print {{
            .container {{
                height: 100vh;
                width: 100vw;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div id="chart" class="chart-container"></div>

    </div>
    
    <script>
        try {{
            var figure = {json.dumps(normalized_figure, cls=plotly.utils.PlotlyJSONEncoder)};
            
            // Configure the plot to use full container size
            var config = {{
                responsive: true,
                displayModeBar: true,
                displaylogo: false,
                modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d']
            }};
            
            // Ensure figure has proper structure
            if (!figure.data) {{
                figure.data = [];
            }}
            if (!figure.layout) {{
                figure.layout = {{}};
            }}
            
            // Update layout to fill container
            figure.layout.autosize = true;
            figure.layout.margin = figure.layout.margin || {{}};
            figure.layout.margin.l = 50;
            figure.layout.margin.r = 50;
            figure.layout.margin.t = 50;
            figure.layout.margin.b = 50;
            
            // Create the plot
            Plotly.newPlot('chart', figure.data, figure.layout, config);
            
            // Resize handler for responsive behavior
            window.addEventListener('resize', function() {{
                Plotly.Plots.resize('chart');
            }});
            
        }} catch (error) {{
            console.error('Error creating chart:', error);
            document.getElementById('chart').innerHTML = '<div style="padding: 20px; text-align: center; color: red;">Error loading chart: ' + error.message + '</div>';
        }}
    </script>
</body>
</html>
"""
        return html_template
    
    def create_chart(self, data, chart_type, x_axis, y_axis, color_by, size_by, show_data_labels=False, dashboard_title=None):
        """Create different types of charts based on selection"""
        
        # Validate that color_by column exists and handle None values
        if color_by and color_by not in data.columns:
            color_by = None
        
        # Validate that size_by column exists and handle None values
        if size_by and size_by not in data.columns:
            size_by = None
        
        # Validate that size_by column is numeric (required for scatter plot sizing)
        if size_by and size_by in data.columns:
            import pandas as pd
            # Try to convert to numeric, coercing errors to NaN
            numeric_size = pd.to_numeric(data[size_by], errors='coerce')
            # If all values are NaN after conversion, skip size parameter
            if numeric_size.isna().all():
                size_by = None
            else:
                # Update the data with numeric values for sizing
                data = data.copy()
                data[size_by] = numeric_size
        
        # Handle multiple Y-axis fields
        if isinstance(y_axis, list) and len(y_axis) > 1:
            # For multiple Y-axis, create a figure with multiple traces
            fig = go.Figure()
            
            for i, y_field in enumerate(y_axis):
                if y_field not in data.columns:
                    continue
                    
                if chart_type == 'line':
                    if color_by:
                        # Group by color field and create separate traces
                        for color_val in data[color_by].unique():
                            subset = data[data[color_by] == color_val]
                            fig.add_trace(go.Scatter(
                                x=subset[x_axis],
                                y=subset[y_field],
                                mode='lines+markers',
                                name=f"{y_field} - {color_val}",
                                legendgroup=f"{y_field}",
                                showlegend=False
                            ))
                    else:
                        fig.add_trace(go.Scatter(
                            x=data[x_axis],
                            y=data[y_field],
                            mode='lines+markers',
                            name=y_field,
                            showlegend=False
                        ))
                elif chart_type == 'bar':
                    if color_by:
                        # Group by color field
                        for color_val in data[color_by].unique():
                            subset = data[data[color_by] == color_val]
                            fig.add_trace(go.Bar(
                                x=subset[x_axis],
                                y=subset[y_field],
                                name=f"{y_field} - {color_val}",
                                legendgroup=f"{y_field}",
                                showlegend=False
                            ))
                    else:
                        fig.add_trace(go.Bar(
                            x=data[x_axis],
                            y=data[y_field],
                            name=y_field,
                            showlegend=False
                        ))
                elif chart_type == 'scatter':
                    if color_by:
                        for color_val in data[color_by].unique():
                            subset = data[data[color_by] == color_val]
                            fig.add_trace(go.Scatter(
                                x=subset[x_axis],
                                y=subset[y_field],
                                mode='markers',
                                name=f"{y_field} - {color_val}",
                                legendgroup=f"{y_field}",
                                showlegend=False
                            ))
                    else:
                        fig.add_trace(go.Scatter(
                            x=data[x_axis],
                            y=data[y_field],
                            mode='markers',
                            name=y_field,
                            showlegend=False
                        ))
            
            # Update layout for multiple Y-axis charts
            fig.update_layout(
                title={
                    'text': dashboard_title or "Data Dashboard",
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': {'size': 16, 'color': '#2c3e50'}
                },
                xaxis_title=x_axis,
                yaxis_title=', '.join(y_axis),
                template="plotly_white",
                margin=dict(l=0, r=0, t=60, b=0),
                autosize=True
            )
            
            return fig
        
        # Handle single Y-axis (convert to single value if it's a list with one item)
        if isinstance(y_axis, list) and len(y_axis) == 1:
            y_axis = y_axis[0]
        
        try:
            if chart_type == 'bar':
                fig = px.bar(data, x=x_axis, y=y_axis, color=color_by, 
                           text=y_axis if show_data_labels else None)
            
            elif chart_type == 'line':
                fig = px.line(data, x=x_axis, y=y_axis, color=color_by)
                if show_data_labels:
                    fig.update_traces(mode='lines+markers+text', textposition='top center')
            
            elif chart_type == 'scatter':
                fig = px.scatter(data, x=x_axis, y=y_axis, color=color_by, size=size_by,
                               text=y_axis if show_data_labels else None)
                if show_data_labels:
                    fig.update_traces(textposition='top center')
            
            elif chart_type == 'pie':
                if y_axis:
                    fig = px.pie(data, names=x_axis, values=y_axis)
                    if show_data_labels:
                        fig.update_traces(textinfo='label+percent+value')
                    fig.update_layout(showlegend=False)
                else:
                    # Count occurrences if no y_axis specified
                    value_counts = data[x_axis].value_counts()
                    fig = px.pie(values=value_counts.values, names=value_counts.index)
                    if show_data_labels:
                        fig.update_traces(textinfo='label+percent+value')
                    fig.update_layout(showlegend=False)
            
            elif chart_type == 'histogram':
                fig = px.histogram(data, x=x_axis, color=color_by)
                if show_data_labels:
                    fig.update_traces(texttemplate='%{y}', textposition='outside')
                fig.update_layout(showlegend=False)
            
            elif chart_type == 'box':
                fig = px.box(data, x=x_axis, y=y_axis, color=color_by)
                fig.update_layout(showlegend=False)
            
            elif chart_type == 'heatmap':
                # Create correlation heatmap for numeric columns
                numeric_data = data.select_dtypes(include=[np.number])
                if len(numeric_data.columns) > 1:
                    corr_matrix = numeric_data.corr()
                    fig = px.imshow(corr_matrix, text_auto=True, aspect="auto")
                else:
                    fig = go.Figure().add_annotation(
                        text="Need at least 2 numeric columns for heatmap",
                        xref="paper", yref="paper", x=0.5, y=0.5,
                        showarrow=False
                    )
            
            elif chart_type == 'area':
                fig = px.area(data, x=x_axis, y=y_axis, color=color_by)
                fig.update_layout(showlegend=False)
            
            else:
                fig = go.Figure()
                
        except Exception as e:
            # If chart creation fails, return an error figure
            fig = go.Figure().add_annotation(
                text=f"Error creating {chart_type} chart: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5, xanchor='center', yanchor='middle',
                showarrow=False, font=dict(size=16, color="red")
            )
        
        # Update layout for all single Y-axis charts
        if 'fig' in locals():
            fig.update_layout(
                title={
                    'text': dashboard_title or "Data Dashboard",
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': {'size': 16, 'color': '#2c3e50'}
                },
                template="plotly_white",
                margin=dict(l=0, r=0, t=60, b=0),
                autosize=True
            )
        
        return fig
    
    def load_data(self, data_df):
        """Load data into the dashboard"""
        self.data = data_df
        # Store data in the hidden div
        data_json = data_df.to_json(date_format='iso', orient='split')
        return data_json
    
    def run_dashboard(self, debug=False):
        """Run the dashboard server"""
        def run_server():
            try:
                # Run with use_reloader=False to prevent issues with threading
                # and set threaded=True for better performance
                self.app.run(
                    debug=debug, 
                    port=self.port, 
                    host='127.0.0.1',
                    use_reloader=False,  # Disable reloader to prevent subprocess issues
                    threaded=True  # Enable threading for better performance
                )
            except Exception as e:
                print(f"Dashboard server error: {e}")
        
        # Run in a separate daemon thread so it doesn't block the main application
        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        
        # Open browser after a short delay to ensure server is ready
        def open_browser():
            import time
            time.sleep(0.5)  # Wait for server to start
            webbrowser.open(f'http://127.0.0.1:{self.port}')
        
        browser_thread = threading.Thread(target=open_browser, daemon=True)
        browser_thread.start()
        
        return server_thread

def create_dashboard_with_data(data_df, title="Dashboard", port=None):
    """Create and launch dashboard with data"""
    # Find an available port if not specified
    if port is None:
        import socket
        port = 8050
        max_attempts = 100
        for attempt in range(max_attempts):
            try:
                # Try to bind to the port to check if it's available
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('127.0.0.1', port))
                    # Port is available
                    break
            except OSError:
                # Port is in use, try next one
                port += 1
        else:
            # Could not find available port
            raise RuntimeError(f"Could not find available port after {max_attempts} attempts")
    
    # Create dashboard with initial data
    dashboard = PlotlyDashboard(port=port, initial_data=data_df)
    
    # Clientside callback for drag-and-drop functionality
    dashboard.app.clientside_callback(
        """
        function(trigger) {
            // Store for drag data
            let dragData = null;
            
            // Initialize drag and drop event listeners
            setTimeout(function() {
                // Add dragstart listeners to field items
                const fieldItems = document.querySelectorAll('[data-field]');
                fieldItems.forEach(function(item) {
                    // Remove existing listeners to avoid duplicates
                    item.removeEventListener('dragstart', item._dragStartHandler);
                    item.removeEventListener('dragend', item._dragEndHandler);
                    
                    item._dragStartHandler = function(e) {
                        const fieldName = e.target.getAttribute('data-field');
                        e.dataTransfer.setData('text/plain', fieldName);
                        e.target.style.opacity = '0.5';
                    };
                    
                    item._dragEndHandler = function(e) {
                        e.target.style.opacity = '1';
                    };
                    
                    item.addEventListener('dragstart', item._dragStartHandler);
                    item.addEventListener('dragend', item._dragEndHandler);
                });
                
                // Add drop zone listeners
                const dropZones = [
                    {id: 'x-axis-drop-zone', target: 'x-axis'},
                    {id: 'y-axis-drop-zone', target: 'y-axis'},
                    {id: 'color-drop-zone', target: 'color'},
                    {id: 'size-drop-zone', target: 'size'}
                ];
                
                dropZones.forEach(function(zone) {
                    const element = document.getElementById(zone.id);
                    if (element) {
                        // Remove existing listeners
                        element.removeEventListener('dragover', element._dragOverHandler);
                        element.removeEventListener('dragleave', element._dragLeaveHandler);
                        element.removeEventListener('drop', element._dropHandler);
                        
                        element._dragOverHandler = function(e) {
                            e.preventDefault();
                            e.target.style.backgroundColor = '#2ecc71';
                            e.target.style.borderColor = '#27ae60';
                        };
                        
                        element._dragLeaveHandler = function(e) {
                            e.target.style.backgroundColor = '#95a5a6';
                            e.target.style.borderColor = '#7f8c8d';
                        };
                        
                        element._dropHandler = function(e) {
                            e.preventDefault();
                            const fieldName = e.dataTransfer.getData('text/plain');
                            e.target.style.backgroundColor = '#95a5a6';
                            e.target.style.borderColor = '#7f8c8d';
                            
                            // Update the drag-drop-trigger store
                            dragData = {
                                field: fieldName,
                                target: zone.target,
                                timestamp: Date.now()
                            };
                            
                            // Trigger a state update
                            window.dash_clientside.set_props('drag-drop-trigger', {data: dragData});
                        };
                        
                        element.addEventListener('dragover', element._dragOverHandler);
                        element.addEventListener('dragleave', element._dragLeaveHandler);
                        element.addEventListener('drop', element._dropHandler);
                    }
                });
            }, 100);
            
            return window.dash_clientside.no_update;
        }
        """,
        Output('drag-drop-trigger', 'data'),
        Input('field-list', 'children')
    )
    
    # Set the title
    dashboard.app.title = title
    
    # Run the dashboard
    thread = dashboard.run_dashboard()
    
    # Return dashboard, thread, and port information
    return dashboard, thread, dashboard.port

if __name__ == "__main__":
    import sys
    import os
    
    # Check if a file path is provided as an argument
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        if os.path.exists(file_path):
            try:
                # Load data from Parquet file
                df = pd.read_parquet(file_path)
                dashboard_title = f"Dashboard - {os.path.basename(file_path)}"
                
                dashboard, thread, port = create_dashboard_with_data(df, title=dashboard_title)
                print(f"Dashboard running at http://127.0.0.1:{port}")
                
                # Keep the main thread alive
                try:
                    thread.join()
                except KeyboardInterrupt:
                    print("Dashboard stopped")
            except Exception as e:
                print(f"Error loading file: {e}")
        else:
            print(f"File not found: {file_path}")
    else:
        # Test with sample data
        sample_data = pd.DataFrame({
            'Category': ['A', 'B', 'C', 'A', 'B', 'C'] * 10,
            'Value': np.random.randint(1, 100, 60),
            'Date': pd.date_range('2023-01-01', periods=60, freq='D'),
            'Region': ['North', 'South'] * 30
        })
        
        dashboard, thread, port = create_dashboard_with_data(sample_data)
        print(f"Dashboard running at http://127.0.0.1:{port}")
        
        # Keep the main thread alive
        try:
            thread.join()
        except KeyboardInterrupt:
            print("Dashboard stopped")