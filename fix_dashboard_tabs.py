"""Fix dashboard tab mappings and enhance Trade Signals display."""
import re

with open('dashboard.html', 'r') as f:
    html = f.read()

changes = 0

# FIX 1: Historical Bands tab button points to 'signals' instead of 'bands'
# The inline fix set onclick="switchTab('signals')" for the bands tab button
old = "switchTab('signals')\" data-i18n=\"tab_bands\""
new = "switchTab('bands')\" data-i18n=\"tab_bands\""
if old in html:
    html = html.replace(old, new, 1)
    changes += 1
    print('FIX 1: Historical Bands tab now switches to bands panel')
else:
    print('FIX 1: Pattern not found (may already be fixed)')

# FIX 2: Ensure Trade Signals tab loads data correctly
# Check if loadSignals fetches from correct endpoint
if '/api/intelligence/signals' in html:
    html = html.replace('/api/intelligence/signals', '/api/intelligence/signal')
    changes += 1
    print('FIX 2: Fixed signals API endpoint (signals -> signal)')
else:
    print('FIX 2: Signal endpoint already correct or not found')

if changes > 0:
    with open('dashboard.html', 'w') as f:
        f.write(html)
    print(f'\nApplied {changes} fix(es) to dashboard.html')
else:
    print('\nNo changes needed')
