"""Fix dashboard: Replace Historical Bands chart with horizontal-band format."""
import re

with open('dashboard.html', 'r') as f:
    html = f.read()

changes = 0

# FIX 1: Historical Bands tab button onclick
old = "switchTab('signals')\" data-i18n=\"tab_bands\""
new = "switchTab('bands')\" data-i18n=\"tab_bands\""
if old in html:
    html = html.replace(old, new, 1)
    changes += 1
    print('FIX 1: Historical Bands tab onclick fixed')

# FIX 2: Signal endpoint
if '/api/intelligence/signals' in html:
    html = html.replace('/api/intelligence/signals', '/api/intelligence/signal')
    changes += 1
    print('FIX 2: Signal endpoint fixed')

# FIX 3: Replace the entire loadBands() function with horizontal-band chart
# Find and replace the loadBands function
old_loadbands_pattern = r'async function loadBands\(\).*?\n\s*\}\s*\n'
match = re.search(old_loadbands_pattern, html, re.DOTALL)
if match:
    new_loadbands = '''async function loadBands() {
  try {
    const country = document.getElementById('bands-country').value;
    const names = {AU:'Australia - EYCI (Young Cattle)',BR:'Brazil - Boi Gordo',UY:'Uruguay - Novillo Gordo',PY:'Paraguay - Novillo Gordo',NZ:'New Zealand - Prime Steer',US:'USA - Fed Cattle',AR:'Argentina - Novillo'};
    const resp = await fetch('/api/cattle/prices?country='+country);
    const data = await resp.json();
    if (!data.length) { document.getElementById('bands-stats').innerHTML='<p>No data</p>'; return; }
    // Get all USD prices and group by year
    const prices = data.map(p => p.price_per_kg_usd).filter(p => p > 0);
    const byYear = {};
    data.forEach(p => { if(p.price_per_kg_usd>0){ const yr=p.date.substring(0,4); if(!byYear[yr])byYear[yr]=[]; byYear[yr].push(p.price_per_kg_usd); }});
    const years = Object.keys(byYear).sort();
    // Calculate percentiles from ALL historical data
    const sorted = [...prices].sort((a,b)=>a-b);
    const pct = (p) => sorted[Math.min(Math.floor(sorted.length*p), sorted.length-1)];
    const p0 = sorted[0];
    const p10 = pct(0.10);
    const p25 = pct(0.25);
    const p50 = pct(0.50);
    const p75 = pct(0.75);
    const p90 = pct(0.90);
    const p100 = sorted[sorted.length-1];
    // Yearly average actual prices
    const yearlyAvg = years.map(y => byYear[y].reduce((a,b)=>a+b,0)/byYear[y].length);
    // Chart title
    document.getElementById('bands-title').innerHTML = '<strong>' + country + '</strong> ' + (names[country]||country) + ' (USD/kg)';
    if (charts.bands) charts.bands.destroy();
    // Horizontal band annotation plugin
    const bandPlugin = {
      id: 'bandBackground',
      beforeDraw(chart) {
        const {ctx, chartArea:{left,right,top,bottom}, scales:{y}} = chart;
        const bands = [
          {min:p90, max:p100*1.05, color:'rgba(96,165,250,0.45)', label:'Extremely High (90-100th)'},
          {min:p75, max:p90, color:'rgba(74,222,128,0.40)', label:'Well Above Avg (75-90th)'},
          {min:p50, max:p75, color:'rgba(163,190,80,0.35)', label:'Average (50-75th)'},
          {min:p25, max:p50, color:'rgba(140,160,60,0.35)', label:'Average (25-50th)'},
          {min:p10, max:p25, color:'rgba(180,120,40,0.40)', label:'Well Below Avg (10-25th)'},
          {min:p0*0.95, max:p10, color:'rgba(239,68,68,0.30)', label:'Extremely Low (0-10th)'}
        ];
        bands.forEach(b => {
          const yTop = y.getPixelForValue(b.max);
          const yBot = y.getPixelForValue(b.min);
          ctx.fillStyle = b.color;
          ctx.fillRect(left, Math.max(yTop,top), right-left, Math.min(yBot,bottom)-Math.max(yTop,top));
        });
      }
    };
    charts.bands = new Chart(document.getElementById('bandsChart'), {
      type: 'line',
      data: {
        labels: years,
        datasets: [{
          label: 'Actual Price',
          data: yearlyAvg,
          borderColor: 'rgba(255,255,255,0.9)',
          backgroundColor: 'rgba(255,255,255,0.1)',
          borderWidth: 2.5,
          pointBackgroundColor: 'rgba(255,255,255,0.8)',
          pointRadius: 4,
          fill: false,
          tension: 0.1
        }]
      },
      options: {
        responsive: true,
        plugins: {
          legend: {display: false},
          tooltip: {callbacks: {label: ctx => '$'+ctx.parsed.y.toFixed(2)+'/kg'}}
        },
        scales: {
          y: {
            min: p0*0.9,
            max: p100*1.1,
            ticks: {color:'#94a3b8', callback: v=>'$'+v.toFixed(2)},
            grid: {color:'rgba(255,255,255,0.05)'}
          },
          x: {ticks:{color:'#94a3b8'}, grid:{display:false}}
        }
      },
      plugins: [bandPlugin]
    });
    // Custom legend for bands
    document.getElementById('bands-stats').innerHTML = '<div style="display:flex;flex-wrap:wrap;gap:12px;margin-top:12px;">'+
      '<span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:16px;background:rgba(96,165,250,0.6);border-radius:2px;"></span><small style="color:#94a3b8;">Extremely High (90-100th)</small></span>'+
      '<span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:16px;background:rgba(74,222,128,0.55);border-radius:2px;"></span><small style="color:#94a3b8;">Well Above Avg (75-90th)</small></span>'+
      '<span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:16px;background:rgba(163,190,80,0.5);border-radius:2px;"></span><small style="color:#94a3b8;">Average (50-75th)</small></span>'+
      '<span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:16px;background:rgba(140,160,60,0.5);border-radius:2px;"></span><small style="color:#94a3b8;">Average (25-50th)</small></span>'+
      '<span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:16px;background:rgba(180,120,40,0.55);border-radius:2px;"></span><small style="color:#94a3b8;">Well Below Avg (10-25th)</small></span>'+
      '<span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:16px;background:rgba(239,68,68,0.45);border-radius:2px;"></span><small style="color:#94a3b8;">Extremely Low (0-10th)</small></span>'+
      '<span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:3px;background:rgba(255,255,255,0.9);border-radius:1px;"></span><small style="color:#94a3b8;">Actual Price</small></span>'+
      '</div>';
  } catch(e) { console.error('Bands error:',e); }
}
'''
    html = html[:match.start()] + new_loadbands + html[match.end():]
    changes += 1
    print('FIX 3: loadBands() replaced with horizontal-band chart')
else:
    print('FIX 3: loadBands() pattern not found')

# FIX 4: Replace the bands panel HTML to include title element
old_bands_title = '<h3>Historical Price Bands (USD/kg)</h3>'
new_bands_title = '<h3 id="bands-title">Historical Price Bands (USD/kg)</h3>'
if old_bands_title in html:
    html = html.replace(old_bands_title, new_bands_title, 1)
    changes += 1
    print('FIX 4: Added bands-title id to heading')

if changes > 0:
    with open('dashboard.html', 'w') as f:
        f.write(html)
    print(f'\nApplied {changes} fix(es) to dashboard.html')
else:
    print('\nNo changes needed')
