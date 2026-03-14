#!/usr/bin/env python3
"""
inject_i18n.py - Injects EN/PT/ES language toggle into dashboard.html
Called by Deploy V1 Dashboard GitHub Actions workflow.
"""
import os

def inject_i18n():
    base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
    with open(base_path, "r") as f:
        html = f.read()

    # Idempotency guard
    if "lang-toggle" in html:
        print("i18n already injected - skipping")
        return

    # === 1. Language toggle UI in header (after <h1>) ===
    old_header = '<h1>Livestock Intelligence Dashboard</h1>'
    new_header = '''<h1 data-i18n="title">Livestock Intelligence Dashboard</h1>
<div id="lang-toggle" style="position:absolute;top:20px;right:32px;display:flex;gap:6px;z-index:100">
  <button onclick="setLang('en')" id="btn-en" style="padding:4px 12px;border-radius:4px;border:1px solid #3b82f6;background:#3b82f6;color:white;cursor:pointer;font-size:12px;font-weight:600">EN</button>
  <button onclick="setLang('pt')" id="btn-pt" style="padding:4px 12px;border-radius:4px;border:1px solid #334155;background:transparent;color:#94a3b8;cursor:pointer;font-size:12px;font-weight:600">PT</button>
  <button onclick="setLang('es')" id="btn-es" style="padding:4px 12px;border-radius:4px;border:1px solid #334155;background:transparent;color:#94a3b8;cursor:pointer;font-size:12px;font-weight:600">ES</button>
</div>'''
    if old_header in html:
        html = html.replace(old_header, new_header, 1)
    else:
        print("WARNING: Could not find h1 to inject language toggle")

    # Make header relative-positioned for absolute toggle
    html = html.replace(
        '.header { background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); padding: 24px 32px; border-bottom: 1px solid #334155; }',
        '.header { background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); padding: 24px 32px; border-bottom: 1px solid #334155; position:relative; }',
        1
    )

    # === 2. Add data-i18n attributes to tab buttons ===
    tab_map = [
        ('onclick="switchTab(\'pulse\')"', 'onclick="switchTab(\'pulse\')" data-i18n="tab_pulse"'),
        ('onclick="switchTab(\'signals\')"', 'onclick="switchTab(\'signals\')" data-i18n="tab_signals"'),
        ('onclick="switchTab(\'herd\')"', 'onclick="switchTab(\'herd\')" data-i18n="tab_herd"'),
        ('onclick="switchTab(\'simulator\')"', 'onclick="switchTab(\'simulator\')" data-i18n="tab_simulator"'),
        ('onclick="switchTab(\'prices\')"', 'onclick="switchTab(\'prices\')" data-i18n="tab_prices"'),
        ('onclick="switchTab(\'spreads\')"', 'onclick="switchTab(\'spreads\')" data-i18n="tab_spreads"'),
        ('onclick="switchTab(\'classes\')"', 'onclick="switchTab(\'classes\')" data-i18n="tab_classes"'),
    ]
    for old, new in tab_map:
        html = html.replace(old, new, 1)

    # === 3. Inject i18n JS before // Initialize ===
    i18n_js = '''
const TRANSLATIONS = {
  en: {
    title: "Livestock Intelligence Dashboard",
    subtitle: "Real-time cattle price intelligence across 7 countries | Stage-trade margins | Market signals",
    tab_pulse: "Market Pulse", tab_signals: "Trade Signals", tab_herd: "Herd Portfolio",
    tab_simulator: "Scenario Simulator", tab_prices: "Price Data",
    tab_spreads: "Cross-Country", tab_classes: "V1 Classes",
    country_label: "Country:", trade_label: "Trade", head_label: "Head Count",
    weight_label: "Entry Weight (kg)", run_btn: "Run Simulation",
    all_countries: "All Countries", loading: "Loading...",
    no_data: "No data for this trade",
    h_benchmark: "Benchmark Prices by Country (USD/kg)",
    h_latest: "Latest Prices by Country",
    h_signals: "NET MARGIN BY STAGE TRADE",
    h_simulator: "Stage Trade Scenario Simulator",
    h_sim_sub: "Adjust parameters to model trade outcomes",
    h_v1: "V1 Canonical Classes \u2014 Lifecycle Order",
    h_spreads: "Cross-Country Price Spread (USD/kg)",
    h_spread_detail: "Spread Detail",
    col_order: "Order", col_classid: "Class ID", col_canonical: "Canonical Name",
    col_species: "Species", col_stage: "Stage", col_sex: "Sex",
    col_country: "Country", col_localname: "Local Name", col_pricebasis: "Price Basis",
    col_conv: "Conv Factor", col_ae: "AE", col_source: "Source",
    total_invest: "Total Investment", total_rev: "Total Revenue",
    gross_margin: "Gross Margin", net_margin: "Net Margin (est)",
    trade_params: "Trade Parameters"
  },
  pt: {
    title: "Painel de Intelig\u00eancia Pecu\u00e1ria",
    subtitle: "Intelig\u00eancia de pre\u00e7os de gado em tempo real em 7 pa\u00edses | Margens por fase | Sinais de mercado",
    tab_pulse: "Pulso do Mercado", tab_signals: "Sinais de Neg\u00f3cio", tab_herd: "Portf\u00f3lio do Rebanho",
    tab_simulator: "Simulador de Cen\u00e1rios", tab_prices: "Pre\u00e7os",
    tab_spreads: "Compara\u00e7\u00e3o Internacional", tab_classes: "Classes V1",
    country_label: "Pa\u00eds:", trade_label: "Neg\u00f3cio", head_label: "N\u00famero de Cabe\u00e7as",
    weight_label: "Peso de Entrada (kg)", run_btn: "Executar Simula\u00e7\u00e3o",
    all_countries: "Todos os Pa\u00edses", loading: "Carregando...",
    no_data: "Sem dados para esta opera\u00e7\u00e3o",
    h_benchmark: "Pre\u00e7os de Refer\u00eancia por Pa\u00eds (USD/kg)",
    h_latest: "\u00daltimos Pre\u00e7os por Pa\u00eds",
    h_signals: "MARGEM L\u00cdQUIDA POR FASE DE COM\u00c9RCIO",
    h_simulator: "Simulador de Fase de Com\u00e9rcio",
    h_sim_sub: "Ajuste os par\u00e2metros para modelar resultados",
    h_v1: "Classes Can\u00f4nicas V1 \u2014 Ordem do Ciclo de Vida",
    h_spreads: "Spread de Pre\u00e7os Internacional (USD/kg)",
    h_spread_detail: "Detalhe do Spread",
    col_order: "Ordem", col_classid: "ID da Classe", col_canonical: "Nome Can\u00f4nico",
    col_species: "Esp\u00e9cie", col_stage: "Fase", col_sex: "Sexo",
    col_country: "Pa\u00eds", col_localname: "Nome Local", col_pricebasis: "Base de Pre\u00e7o",
    col_conv: "Fator Conv.", col_ae: "UA", col_source: "Fonte",
    total_invest: "Investimento Total", total_rev: "Receita Total",
    gross_margin: "Margem Bruta", net_margin: "Margem L\u00edquida (est.)",
    trade_params: "Par\u00e2metros da Opera\u00e7\u00e3o"
  },
  es: {
    title: "Panel de Inteligencia Ganadera",
    subtitle: "Inteligencia de precios de ganado en tiempo real en 7 pa\u00edses | M\u00e1rgenes por etapa | Se\u00f1ales de mercado",
    tab_pulse: "Pulso del Mercado", tab_signals: "Se\u00f1ales de Negocio", tab_herd: "Portafolio del Rodeo",
    tab_simulator: "Simulador de Escenarios", tab_prices: "Precios",
    tab_spreads: "Comparaci\u00f3n Internacional", tab_classes: "Clases V1",
    country_label: "Pa\u00eds:", trade_label: "Operaci\u00f3n", head_label: "N\u00famero de Cabezas",
    weight_label: "Peso de Entrada (kg)", run_btn: "Ejecutar Simulaci\u00f3n",
    all_countries: "Todos los Pa\u00edses", loading: "Cargando...",
    no_data: "Sin datos para esta operaci\u00f3n",
    h_benchmark: "Precios de Referencia por Pa\u00eds (USD/kg)",
    h_latest: "\u00daltimos Precios por Pa\u00eds",
    h_signals: "MARGEN NETO POR ETAPA DE COMERCIO",
    h_simulator: "Simulador de Etapa de Comercio",
    h_sim_sub: "Ajuste los par\u00e1metros para modelar resultados",
    h_v1: "Clases Can\u00f3nicas V1 \u2014 Orden del Ciclo de Vida",
    h_spreads: "Spread de Precios Internacional (USD/kg)",
    h_spread_detail: "Detalle del Spread",
    col_order: "Orden", col_classid: "ID de Clase", col_canonical: "Nombre Can\u00f3nico",
    col_species: "Especie", col_stage: "Etapa", col_sex: "Sexo",
    col_country: "Pa\u00eds", col_localname: "Nombre Local", col_pricebasis: "Base de Precio",
    col_conv: "Factor Conv.", col_ae: "UA", col_source: "Fuente",
    total_invest: "Inversi\u00f3n Total", total_rev: "Ingreso Total",
    gross_margin: "Margen Bruto", net_margin: "Margen Neto (est.)",
    trade_params: "Par\u00e1metros de la Operaci\u00f3n"
  }
};

let currentLang = localStorage.getItem('lang') || 'en';

function setLang(lang) {
  currentLang = lang;
  localStorage.setItem('lang', lang);
  applyTranslations();
  // Update toggle button styles
  ['en','pt','es'].forEach(l => {
    const btn = document.getElementById('btn-'+l);
    if (!btn) return;
    if (l === lang) {
      btn.style.background = '#3b82f6';
      btn.style.borderColor = '#3b82f6';
      btn.style.color = 'white';
    } else {
      btn.style.background = 'transparent';
      btn.style.borderColor = '#334155';
      btn.style.color = '#94a3b8';
    }
  });
}

function t(key) {
  const tr = TRANSLATIONS[currentLang] || TRANSLATIONS['en'];
  return tr[key] || TRANSLATIONS['en'][key] || key;
}

function applyTranslations() {
  const tr = TRANSLATIONS[currentLang] || TRANSLATIONS['en'];
  // Translate all data-i18n elements
  document.querySelectorAll('[data-i18n]').forEach(el => {
    const key = el.getAttribute('data-i18n');
    if (tr[key]) el.textContent = tr[key];
  });
  // Subtitle
  const sub = document.querySelector('.header p');
  if (sub) sub.textContent = tr.subtitle || sub.textContent;
  // Run simulation button
  const runBtn = document.querySelector('.btn[onclick="runSimulation()"]');
  if (runBtn) runBtn.textContent = tr.run_btn || runBtn.textContent;
  // Simulator subtitle
  const simSub = document.querySelector('#tab-simulator .card ~ * p, #tab-simulator p');
  if (simSub) simSub.textContent = tr.h_sim_sub || simSub.textContent;
}

'''
    old_init = "// Initialize"
    html = html.replace(old_init, i18n_js + old_init, 1)

    # === 4. Apply lang on page load after loadPulse() ===
    html = html.replace(
        "loadPulse();",
        "loadPulse();\n  setLang(currentLang);",
        1
    )

    with open(base_path, "w") as f:
        f.write(html)
    print("i18n language toggle injected successfully")

if __name__ == "__main__":
    inject_i18n()
