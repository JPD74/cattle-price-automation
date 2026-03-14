#!/usr/bin/env python3
"""
write_dashboard.py - Generate the V1 Livestock Intelligence Dashboard HTML.
Called by the Deploy V1 Dashboard GitHub Actions workflow.
Reads dashboard.html, injects V1 Classes tab, writes it back.
"""

import os

def generate_dashboard():
    base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
    with open(base_path, "r") as f:
        html = f.read()

    # Idempotency: skip if already injected
    if "tab-classes" in html:
        print("V1 Classes tab already present - skipping")
        return

    # === 1. Add V1 Classes tab button after Cross-Country ===
    old_tab = """Cross-Country</div>"""
    new_tab = """Cross-Country</div>
        <div class="tab" onclick="switchTab('classes')">V1 Classes</div>"""
    if old_tab in html:
        html = html.replace(old_tab, new_tab, 1)
    else:
        print("WARNING: Could not find Cross-Country tab div to inject after")

    # === 2. Add V1 Classes tab content div before first <script> ===
    v1_tab_html = """<div id="tab-classes" class="tab-content">
  <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:16px">
    <label style="color:#94a3b8;font-size:13px">Country:</label>
    <select id="cls-country" onchange="loadV1Classes()">
      <option value="">All Countries</option>
      <option value="AU">Australia</option><option value="BR">Brazil</option>
      <option value="AR">Argentina</option><option value="UY">Uruguay</option>
      <option value="PY">Paraguay</option><option value="US">USA</option>
      <option value="NZ">New Zealand</option>
    </select>
  </div>
  <div class="card">
    <h3>V1 Canonical Classes &mdash; Lifecycle Order</h3>
    <div id="cls-table"><div class="loading">Loading...</div></div>
  </div>
</div>
"""
    # Insert before the Chart.js <script> or inline <script>
    script_marker = "<script>"
    # Find second <script> (first is chart.js CDN, second is inline)
    first_idx = html.find(script_marker)
    second_idx = html.find(script_marker, first_idx + 1)
    if second_idx > 0:
        html = html[:second_idx] + v1_tab_html + html[second_idx:]
    else:
        # Only one script tag - insert before it
        html = html[:first_idx] + v1_tab_html + html[first_idx:]

    # === 3. Add switchTab handler for 'classes' ===
    old_switch = "if (name==='spreads') loadSpreads();"
    new_switch = old_switch + "\n      if (name==='classes') loadV1Classes();"
    html = html.replace(old_switch, new_switch, 1)

    # === 4. Add loadV1Classes JS function before "// Initialize" ===
    v1_js = """
async function loadV1Classes() {
  const country = document.getElementById('cls-country').value;
  let url = API + '/api/intelligence/v1-classes';
  if (country) url += '?country=' + country;
  try {
    const data = await fetch(url).then(r => r.json());
    if (!data.length) {
      document.getElementById('cls-table').innerHTML = '<div class="loading">No V1 classes found</div>';
      return;
    }
    // Group by country for display
    let tbl = '<table><tr><th>Order</th><th>Class ID</th><th>Canonical Name</th><th>Species</th><th>Stage</th><th>Sex</th><th>Country</th><th>Local Name</th><th>Price Basis</th><th>Conv Factor</th><th>AE</th><th>Source</th></tr>';
    data.forEach(c => {
      tbl += '<tr>'
        + '<td>' + (c.lifecycle_order || '-') + '</td>'
        + '<td>' + (c.class_id || '-') + '</td>'
        + '<td><strong>' + c.canonical_name + '</strong></td>'
        + '<td>' + (c.species || '-') + '</td>'
        + '<td>' + (c.stage || '-') + '</td>'
        + '<td>' + (c.sex || '-') + '</td>'
        + '<td>' + c.country + '</td>'
        + '<td>' + c.local_name + '</td>'
        + '<td>' + (c.price_basis || '-') + '</td>'
        + '<td>' + (c.conversion_factor || '-') + '</td>'
        + '<td>' + (c.ae_equivalent || '-') + '</td>'
        + '<td>' + (c.data_source || '-') + '</td>'
        + '</tr>';
    });
    tbl += '</table>';
    document.getElementById('cls-table').innerHTML = tbl;
  } catch(e) {
    console.error('V1 Classes error:', e);
    document.getElementById('cls-table').innerHTML = '<div class="loading">Error loading V1 classes</div>';
  }
}

"""
    old_init = "// Initialize"
    html = html.replace(old_init, v1_js + old_init, 1)

    # Write out
    with open(base_path, "w") as f:
        f.write(html)
    print("dashboard.html updated with V1 Classes tab successfully")


if __name__ == "__main__":
    generate_dashboard()
