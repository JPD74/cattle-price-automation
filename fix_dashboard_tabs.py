"""Fix dashboard: Inject bands panel HTML + horizontal-band chart JS."""
import re

with open('dashboard.html', 'r') as f:
    html = f.read()

changes = 0

# FIX 1: Inject tab-bands div if missing
if 'id="tab-bands"' not in html:
    bands_html = '''
<div id="tab-bands" class="tab-content">
<p>Country: <select id="bands-country" onchange="loadBands()">
<option value="AU">Australia - EYCI</option>
<option value="BR">Brazil - Boi Gordo</option>
<option value="UY">Uruguay - Novillo</option>
<option value="PY">Paraguay - Novillo</option>
<option value="NZ">New Zealand - Prime Steer</option>
<option value="US">USA - Fed Cattle</option>
<option value="AR">Argentina - Novillo</option>
</select></p>
<div class="card">
<h3 id="bands-title">Historical Price Bands (USD/kg)</h3>
<canvas id="bandsChart"></canvas>
<div id="bands-stats">Select a country to view bands</div>
</div>
</div>
'''
    # Insert after tab-pulse div
    marker = re.search(r'<div id="tab-signals"', html)
    if marker:
        html = html[:marker.start()] + bands_html + html[marker.start():]
        changes += 1
        print('FIX 1: Injected tab-bands panel before tab-signals')
    else:
        # Try after tab-pulse closing
        pulse_end = html.find('</div><!-- tab-pulse -->')
        if pulse_end == -1:
            # Find first tab-content div end after tab-pulse
            pulse_start = html.find('id="tab-pulse"')
            if pulse_start > -1:
                # Insert after the pulse div by finding pattern
                sig = html.find('id="tab-signals"')
                if sig > -1:
                    html = html[:sig-5] + bands_html + html[sig-5:]
                    changes += 1
                    print('FIX 1: Injected tab-bands panel')
else:
    print('FIX 1: tab-bands already exists')

# FIX 2: Fix bands tab button onclick
old_btn = "switchTab('signals')\" data-i18n=\"tab_bands\""
new_btn = "switchTab('bands')\" data-i18n=\"tab_bands\""
if old_btn in html:
    html = html.replace(old_btn, new_btn, 1)
    changes += 1
    print('FIX 2: Bands tab onclick fixed')

# FIX 3: Replace loadBands() function
old_fn = re.search(r'async function loadBands\(\).*?\n\s*\}\s*\n', html, re.DOTALL)
if old_fn:
    new_fn = """async function loadBands() {
  try {
    const country = document.getElementById('bands-country').value;
    const names = {AU:'Australia - EYCI (Young Cattle)',BR:'Brazil - Boi Gordo',UY:'Uruguay - Novillo Gordo',PY:'Paraguay - Novillo Gordo',NZ:'New Zealand - Prime Steer',US:'USA - Fed Cattle',AR:'Argentina - Novillo'};
    const resp = await fetch('/api/cattle/prices?country='+country);
    const data = await resp.json();
    if (!data.length) { document.getElementById('bands-stats').innerHTML='<p>No data</p>'; return; }
    const prices = data.map(p => p.price_per_kg_usd).filter(p => p > 0);
    const byYear = {};
    data.forEach(p => { if(p.price_per_kg_usd>0){ const yr=p.date.substring(0,4); if(!byYear[yr])byYear[yr]=[]; byYear[yr].push(p.price_per_kg_usd); }});
    const years = Object.keys(byYear).sort();
    const sorted = [...prices].sort((a,b)=>a-b);
    const pct = (p) => sorted[Math.min(Math.floor(sorted.length*p), sorted.length-1)];
    const p0=sorted[0], p10=pct(0.10), p25=pct(0.25), p50=pct(0.50), p75=pct(0.75), p90=pct(0.90), p100=sorted[sorted.length-1];
    const yearlyAvg = years.map(y => byYear[y].reduce((a,b)=>a+b,0)/byYear[y].length);
    const el=document.getElementById('bands-title'); if(el) el.innerHTML='<strong>'+country+'</strong> '+(names[country]||country)+' (USD/kg)';
    if (charts.bands) charts.bands.destroy();
    const bp = {id:'bandBg', beforeDraw(chart){const{ctx,chartArea:{left,right,top,bottom},scales:{y}}=chart;[{mn:p90,mx:p100*1.05,c:'rgba(96,165,250,0.45)'},{mn:p75,mx:p90,c:'rgba(74,222,128,0.40)'},{mn:p50,mx:p75,c:'rgba(163,190,80,0.35)'},{mn:p25,mx:p50,c:'rgba(140,160,60,0.35)'},{mn:p10,mx:p25,c:'rgba(180,120,40,0.40)'},{mn:p0*0.95,mx:p10,c:'rgba(239,68,68,0.30)'}].forEach(b=>{const yt=y.getPixelForValue(b.mx),yb=y.getPixelForValue(b.mn);ctx.fillStyle=b.c;ctx.fillRect(left,Math.max(yt,top),right-left,Math.min(yb,bottom)-Math.max(yt,top));});}};
    charts.bands = new Chart(document.getElementById('bandsChart'),{type:'line',data:{labels:years,datasets:[{label:'Actual Price',data:yearlyAvg,borderColor:'rgba(255,255,255,0.9)',backgroundColor:'rgba(255,255,255,0.1)',borderWidth:2.5,pointBackgroundColor:'rgba(255,255,255,0.8)',pointRadius:4,fill:false,tension:0.1}]},options:{responsive:true,plugins:{legend:{display:false},tooltip:{callbacks:{label:ctx=>'$'+ctx.parsed.y.toFixed(2)+'/kg'}}},scales:{y:{min:p0*0.9,max:p100*1.1,ticks:{color:'#94a3b8',callback:v=>'$'+v.toFixed(2)},grid:{color:'rgba(255,255,255,0.05)'}},x:{ticks:{color:'#94a3b8'},grid:{display:false}}}},plugins:[bp]});
    document.getElementById('bands-stats').innerHTML='<div style="display:flex;flex-wrap:wrap;gap:12px;margin-top:12px"><span style="display:flex;align-items:center;gap:4px"><span style="width:16px;height:16px;background:rgba(96,165,250,0.6);border-radius:2px"></span><small style="color:#94a3b8">Extremely High (90-100th)</small></span><span style="display:flex;align-items:center;gap:4px"><span style="width:16px;height:16px;background:rgba(74,222,128,0.55);border-radius:2px"></span><small style="color:#94a3b8">Well Above Avg (75-90th)</small></span><span style="display:flex;align-items:center;gap:4px"><span style="width:16px;height:16px;background:rgba(163,190,80,0.5);border-radius:2px"></span><small style="color:#94a3b8">Average (50-75th)</small></span><span style="display:flex;align-items:center;gap:4px"><span style="width:16px;height:16px;background:rgba(140,160,60,0.5);border-radius:2px"></span><small style="color:#94a3b8">Average (25-50th)</small></span><span style="display:flex;align-items:center;gap:4px"><span style="width:16px;height:16px;background:rgba(180,120,40,0.55);border-radius:2px"></span><small style="color:#94a3b8">Well Below Avg (10-25th)</small></span><span style="display:flex;align-items:center;gap:4px"><span style="width:16px;height:16px;background:rgba(239,68,68,0.45);border-radius:2px"></span><small style="color:#94a3b8">Extremely Low (0-10th)</small></span><span style="display:flex;align-items:center;gap:4px"><span style="width:16px;height:3px;background:rgba(255,255,255,0.9);border-radius:1px"></span><small style="color:#94a3b8">Actual Price</small></span></div>';
  } catch(e) { console.error('Bands error:',e); }
}
"""
    html = html[:old_fn.start()] + new_fn + html[old_fn.end():]
    changes += 1
    print('FIX 3: loadBands() replaced with horizontal-band chart')
else:
    print('FIX 3: loadBands() not found')

if changes > 0:
    with open('dashboard.html', 'w') as f:
        f.write(html)
    print(f'\nApplied {changes} fix(es)')
else:
    print('\nNo changes needed')
