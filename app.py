import argparse
import json
import os
import sys
import urllib.request
from typing import List, Optional

import pandas as pd

# ===== Easy configuration variables (edit these directly) =====
DEFAULT_INPUT = 'sample_network.xlsx'  # File path to CSV/XLSX/XLS
DEFAULT_COLUMNS = ['Network', 'Segment', 'Device', 'Role']  # Ordered columns to explore
DEFAULT_DESC = 'Description'  # Description column (optional, set to None to disable)
DEFAULT_TITLE = 'Dynamic Network Explorer'  # Page title
DEFAULT_OUTPUT = 'network_output.html'  # Output HTML path
DEFAULT_EMBED_LIB = True  # Embed Cytoscape JS for offline viewing


def read_table(path: str) -> pd.DataFrame:
    lower = path.lower()
    if lower.endswith('.csv'):
        return pd.read_csv(path)
    if lower.endswith('.xlsx') or lower.endswith('.xls'):
        return pd.read_excel(path)
    # Fallback: try CSV
    return pd.read_csv(path)


def validate_columns(df: pd.DataFrame, levels: List[str], desc_col: Optional[str]):
    missing = [c for c in levels if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in input: {missing}")
    if desc_col and desc_col not in df.columns:
        raise ValueError(f"Description column '{desc_col}' not found in input")


def rows_for_export(df: pd.DataFrame, levels: List[str], desc_col: Optional[str]):
    cols = levels[:] + ([desc_col] if desc_col else [])
    out = []
    for _, row in df[cols].iterrows():
        rec = {c: ('' if pd.isna(row[c]) else str(row[c])) for c in cols}
        out.append(rec)
    return out


def fetch_cytoscape_js(version: str = "3.30.2") -> Optional[str]:
    url = f"https://unpkg.com/cytoscape@{version}/dist/cytoscape.min.js"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return resp.read().decode('utf-8')
    except Exception:
        return None


def build_html(rows_json: str, levels: List[str], desc_col: Optional[str], title: str,
               embed_lib: bool = True, cy_version: str = "3.30.2",
               source_name: Optional[str] = None, columns_display: Optional[List[str]] = None) -> str:
    cytoscape_script_inline = fetch_cytoscape_js(cy_version) if embed_lib else None

    # Refined styles for a nicer control panel and buttons
    css = """
    html, body { height: 100%; margin: 0; }
    .app { height: 100vh; width: 100vw; overflow: hidden; background: #f5f7fb; }
    #controls {
      position: fixed; top: 16px; left: 16px; z-index: 1000;
      background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248,249,251,0.96));
      backdrop-filter: blur(6px);
      border: 1px solid #e3e6eb; border-radius: 14px; padding: 16px 16px 14px 16px;
      box-shadow: 0 12px 28px rgba(16,24,40,0.18);
      min-width: 360px; font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, sans-serif;
    }
    #controls h3 { margin: 0 0 10px 0; font-weight: 700; letter-spacing: 0.2px; }
    .row { display: flex; gap: 10px; align-items: center; }
    .btn { border: 1px solid #cbd5e1; background: linear-gradient(180deg, #ffffff, #f8fafc); color: #0f172a; border-radius: 10px; padding: 9px 14px; cursor: pointer; font-weight: 600; box-shadow: 0 1px 2px rgba(16,24,40,0.06); transition: transform .12s ease, box-shadow .12s ease, background .12s ease, border-color .12s ease; }
    .btn:hover { border-color: #94a3b8; background: linear-gradient(180deg, #f8fafc, #eef2f7); transform: translateY(-1px); box-shadow: 0 4px 12px rgba(16,24,40,0.12); }
    .btn:active { transform: translateY(0); box-shadow: 0 1px 2px rgba(16,24,40,0.06); }
    #backBtn { background: linear-gradient(180deg, #ffffff, #f1f5f9); }
    #resetBtn { background: linear-gradient(180deg, #2563eb, #1d4ed8); color: #ffffff; border-color: #1e40af; }
    #resetBtn:hover { box-shadow: 0 6px 16px rgba(37,99,235,0.25); }
    #resetBtn:active { box-shadow: 0 2px 8px rgba(37,99,235,0.20); }
    .meta { margin-top: 6px; font-size: 13px; color: #334155; }
    .meta .label { font-weight: 600; color: #0f172a; }
    #breadcrumb { margin-top: 10px; font-size: 14px; color: #111827; }
    /* Hide meta/source/columns and breadcrumb path as requested */
    .meta, #breadcrumb { display: none; }
    #descPanel { margin-top: 8px; font-size: 13px; color: #111827; max-height: 30vh; overflow-y: auto; }
    #cy { position: fixed; top: 0; left: 0; right: 0; bottom: 0; }
    """

    # JavaScript application logic
    js = f"""
    const LEVELS = {json.dumps(levels)};
    const DESC_COL = {json.dumps(desc_col)};
    const DATA = {rows_json};
    let filters = [];

    function filterRows(rows, filters) {{
      if (!filters || filters.length === 0) return rows;
      return rows.filter(r => filters.every((val, idx) => String(r[LEVELS[idx]]) === String(val)));
    }}

    function uniqueValues(rows, col) {{
      const set = new Set();
      rows.forEach(r => {{ if (r[col] !== undefined && r[col] !== null && String(r[col]).length > 0) set.add(String(r[col])); }});
      return Array.from(set).sort();
    }}

    function buildElements(rows, levels, filters, descCol) {{
      const elements = [];
      const layout = {{
        name: 'concentric',
        fit: true,
        padding: 160,
        animate: true,
        spacingFactor: 1.5,
        minNodeSpacing: 120,
        nodeDimensionsIncludeLabels: true,
        concentric: function(n) {{
          const k = n.data('kind');
          if (k === 'center') return 3;
          if (k === 'next') return 2;
          if (k === 'level0') return 1;
          if (k === 'desc') return 0;
          return 1;
        }},
        levelWidth: function(nodes) {{ return 280; }}
      }};
      const BLUE = '#2563eb';
      const BLUE_DARK = '#1e40af';
      const LABEL = '#1f2937'; // dark grey for better visibility
      const stylesheet = [
        {{ selector: 'node', style: {{
            label: 'data(label)',
            'text-valign': 'top',
            'text-halign': 'center',
            'font-size': '16px',
            color: LABEL,
            'background-color': BLUE,
            width: 30,
            height: 30,
            opacity: 0.95,
            'text-wrap': 'wrap',
            'text-max-width': '160px',
            'text-margin-y': -12,
            'text-margin-x': 6,
            'text-outline-color': '#f8fafc',
            'text-outline-width': 2
          }} }},
        {{ selector: 'node.center', style: {{ 'background-color': BLUE, width: 60, height: 60, 'font-weight': '600', color: LABEL, 'border-width': 3, 'border-color': BLUE_DARK }} }},
        {{ selector: 'node.level0', style: {{ 'background-color': BLUE, width: 38, height: 38, color: LABEL }} }},
        {{ selector: 'node.next', style: {{ 'background-color': BLUE, width: 34, height: 34, color: LABEL }} }},
        {{ selector: 'node.desc', style: {{ 'background-color': BLUE, width: 28, height: 28, color: LABEL, 'text-valign': 'bottom', 'text-margin-y': 12 }} }},
        {{ selector: 'edge', style: {{ 'line-color': '#60a5fa', width: 3, opacity: 0.85, 'curve-style': 'bezier', 'control-point-step-size': 80 }} }},
        {{ selector: 'node:selected', style: {{ 'border-width': 4, 'border-color': BLUE_DARK }} }},
      ];

      if (!rows || rows.length === 0 || !levels || levels.length === 0) {{
        return [[], layout, stylesheet];
      }}

      const dfFiltered = filterRows(rows, filters);

      if (!filters || filters.length === 0) {{
        const firstValues = uniqueValues(rows, levels[0]);
        firstValues.forEach(v => elements.push({{ data: {{ id: `l0-${{v}}`, label: v, value: v, kind: 'level0' }}, classes: 'level0' }}));
        return [elements, layout, stylesheet];
      }}

      const currentLevelIdx = filters.length - 1;
      const centerValue = String(filters[filters.length - 1]);
      const centerId = `center-${{currentLevelIdx}}-${{centerValue}}`;
      elements.push({{ data: {{ id: centerId, label: centerValue, value: centerValue, kind: 'center' }}, classes: 'center' }});

      if (filters.length < levels.length) {{
        const nextCol = levels[filters.length];
        const nextValues = uniqueValues(dfFiltered, nextCol);
        nextValues.forEach(v => {{
          const nid = `next-${{filters.length}}-${{v}}`;
          elements.push({{ data: {{ id: nid, label: v, value: v, kind: 'next' }}, classes: 'next' }});
          elements.push({{ data: {{ source: centerId, target: nid }} }});
        }});
        return [elements, layout, stylesheet];
      }}

      if (descCol) {{
        const descValues = uniqueValues(dfFiltered, descCol);
        descValues.forEach(dv => {{
          const did = `desc-${{dv}}`;
          elements.push({{ data: {{ id: did, label: dv.slice(0, 60), value: dv, kind: 'desc' }}, classes: 'desc' }});
          elements.push({{ data: {{ source: centerId, target: did }} }});
        }});
      }}
      return [elements, layout, stylesheet];
    }}

    let cy = null;

    function boxesOverlap(a, b) {{
      return !(a.x2 < b.x1 || b.x2 < a.x1 || a.y2 < b.y1 || b.y2 < a.y1);
    }}

    function anyOverlap(cy) {{
      const nodes = cy.nodes();
      for (let i = 0; i < nodes.length; i++) {{
        const bbi = nodes[i].boundingBox({{ includeLabels: true }});
        for (let j = i + 1; j < nodes.length; j++) {{
          const bbj = nodes[j].boundingBox({{ includeLabels: true }});
          if (boxesOverlap(bbi, bbj)) return true;
        }}
      }}
      return false;
    }}

    function nodePriority(n) {{
      const k = n.data('kind');
      if (k === 'center') return 3;
      if (k === 'level0' || k === 'next') return 2;
      return 1; // desc or others
    }}

    function resolveOverlaps(cy, margin = 12, maxPasses = 8) {{
      let pass = 0;
      while (pass < maxPasses) {{
        let moved = false;
        const nodes = cy.nodes();
        cy.batch(() => {{
          for (let i = 0; i < nodes.length; i++) {{
            const a = nodes[i];
            const bbA = a.boundingBox({{ includeLabels: true }});
            for (let j = i + 1; j < nodes.length; j++) {{
              const b = nodes[j];
              const bbB = b.boundingBox({{ includeLabels: true }});
              if (!boxesOverlap(bbA, bbB)) continue;
              const target = nodePriority(a) <= nodePriority(b) ? a : b;
              const bbT = target.boundingBox({{ includeLabels: true }});
              const p = target.position();
              const overlapY = Math.max(0, Math.min(bbA.y2, bbB.y2) - Math.max(bbA.y1, bbB.y1));
              const overlapX = Math.max(0, Math.min(bbA.x2, bbB.x2) - Math.max(bbA.x1, bbB.x1));
              const dy = Math.max(margin, overlapY + margin);
              const dx = overlapX > 0 ? overlapX * 0.35 : 0;
              target.position({{ x: p.x + dx, y: p.y + dy }});
              moved = true;
            }}
          }}
        }});
        if (!moved) break;
        pass++;
      }}
      cy.resize();
      cy.fit(cy.elements(), 100);
    }}

    function runAdaptiveLayout(cy, baseLayout) {{
      let attempt = 0;
      let lw = 280;
      let spacing = 120;
      while (attempt < 3) {{
        const newLayout = Object.assign({{}}, baseLayout);
        newLayout.levelWidth = function() {{ return lw; }};
        newLayout.minNodeSpacing = spacing;
        cy.layout(newLayout).run();
        cy.resize();
        cy.fit(cy.elements(), 100);
        if (!anyOverlap(cy)) break;
        lw += 70;
        spacing += 30;
        attempt++;
      }}
      // Final safeguard: if overlaps persist, nudge nodes apart
      if (anyOverlap(cy)) {{ resolveOverlaps(cy, 16, 12); }}
    }}

    function render() {{
      const [elements, layout, stylesheet] = buildElements(DATA, LEVELS, filters, DESC_COL);
      if (!cy) {{
        cy = cytoscape({{
          container: document.getElementById('cy'),
          elements: elements,
          style: stylesheet,
          layout: layout,
          wheelSensitivity: 0.2,
        }});
        // Adaptive layout: increase spacing if any label overlaps are detected
        runAdaptiveLayout(cy, layout);
        cy.on('tap', 'node', (evt) => {{
          const node = evt.target;
          const kind = node.data('kind');
          const value = node.data('value');
          if ((kind === 'level0' || kind === 'next') && value !== undefined) {{
            if (filters.length < LEVELS.length) {{
              filters.push(String(value));
              render();
            }}
          }}
        }});
      }} else {{
        cy.elements().remove();
        cy.add(elements);
        runAdaptiveLayout(cy, layout);
      }}

      // Breadcrumb
      const trail = filters.map((f, i) => `${{LEVELS[i]}}: ${{f}}`);
      document.getElementById('breadcrumb').innerHTML = trail.length ? `<strong>Path:</strong> ${{trail.join(' › ')}}` : '';

      // Description list
      if (DESC_COL && filters.length === LEVELS.length) {{
        const dfFiltered = filterRows(DATA, filters);
        const descSet = new Set();
        dfFiltered.forEach(r => {{ if (r[DESC_COL]) descSet.add(String(r[DESC_COL])); }});
        const list = Array.from(descSet).slice(0, 200).map(d => `<li>${{d}}</li>`).join('');
        document.getElementById('descPanel').innerHTML = `<strong>Descriptions:</strong><ul>${{list}}</ul>`;
      }} else {{
        document.getElementById('descPanel').innerHTML = '';
      }}
    }}

    function reset() {{ filters = []; render(); }}
    function back() {{ if (filters.length > 0) {{ filters.pop(); render(); }} }}

    window.addEventListener('DOMContentLoaded', () => {{
      document.getElementById('title').textContent = {json.dumps(title)};
      document.getElementById('backBtn').addEventListener('click', back);
      document.getElementById('resetBtn').addEventListener('click', reset);
      // Keep graph fitted on viewport resize
      window.addEventListener('resize', () => {{ if (cy) {{ cy.resize(); runAdaptiveLayout(cy, {{ name: 'concentric' }}); }} }});
      render();
    }});
    """

    # HTML template
    head_scripts = ""
    if cytoscape_script_inline:
        head_scripts = f"<script>\n{cytoscape_script_inline}\n</script>"
    else:
        head_scripts = f"<script src=\"https://unpkg.com/cytoscape@{cy_version}/dist/cytoscape.min.js\"></script>"

    html = f"""
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>{title}</title>
      <style>{css}</style>
      {head_scripts}
    </head>
    <body>
      <div class="app">
        <div id="controls">
          <h3 id="title">{title}</h3>
          <div class="row">
            <button class="btn" id="backBtn">Back</button>
            <button class="btn" id="resetBtn">Reset</button>
          </div>
          <div class="meta">
            <div><span class="label">Source:</span> {source_name or ''}</div>
            <div><span class="label">Columns:</span> {', '.join(columns_display or levels)}</div>
          </div>
          <div id="breadcrumb"></div>
          <div id="descPanel"></div>
        </div>
        <div id="cy"></div>
      </div>
      <script>{js}</script>
    </body>
    </html>
    """
    return html


def main():
    parser = argparse.ArgumentParser(description="Generate a self-contained HTML network explorer from CSV/Excel")
    parser.add_argument('--input', required=False, default=DEFAULT_INPUT, help='Path to input CSV/XLSX/XLS file')
    parser.add_argument('--columns', nargs='+', required=False, default=DEFAULT_COLUMNS, help='Ordered columns to explore')
    parser.add_argument('--desc', required=False, default=DEFAULT_DESC, help='Optional description column name')
    parser.add_argument('--title', required=False, default=DEFAULT_TITLE, help='Page title')
    parser.add_argument('--output', required=False, default=DEFAULT_OUTPUT, help='Output HTML file path')
    parser.add_argument('--embed-lib', action='store_true' if not DEFAULT_EMBED_LIB else 'store_false', help=('Embed Cytoscape inline' if not DEFAULT_EMBED_LIB else 'Disable inline embed'))
    args = parser.parse_args()

    df = read_table(args.input)
    validate_columns(df, args.columns, args.desc)
    rows = rows_for_export(df, args.columns, args.desc)
    rows_json = json.dumps(rows)

    source_name = os.path.basename(args.input)
    html = build_html(rows_json, args.columns, args.desc, args.title,
                      embed_lib=args.embed_lib if isinstance(args.embed_lib, bool) else DEFAULT_EMBED_LIB,
                      source_name=source_name, columns_display=args.columns)
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Wrote {args.output} (rows={len(rows)}, columns={args.columns}, desc={args.desc})")


if __name__ == '__main__':
    main()
