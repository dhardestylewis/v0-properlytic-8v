"""Build protest-results.json from exp02_isotonic scores for the results page."""
import csv, json, os

scores = {}
src = os.path.join(os.path.dirname(__file__), '..', '..', 'Analysis', 'Results',
                   'Experiments', 'exp02_isotonic', 'per_parcel_scores.csv')

with open(src) as f:
    for row in csv.DictReader(f):
        year = int(row['year'])
        prob = float(row['ens_calibrated'])
        actual = 1 if row['actual'] == '1.0' else 0
        if year not in scores:
            scores[year] = {'sum_prob': 0, 'sum_actual': 0, 'n': 0}
        scores[year]['sum_prob'] += prob
        scores[year]['sum_actual'] += actual
        scores[year]['n'] += 1

results = []
for year in sorted(scores.keys()):
    s = scores[year]
    r = {
        'year': year,
        'avg_prob': round(s['sum_prob'] / s['n'], 4),
        'actual_rate': round(s['sum_actual'] / s['n'], 4),
        'n': s['n'],
    }
    results.append(r)
    print(f"{year}: prob={r['avg_prob']:.4f}  rate={r['actual_rate']:.4f}  n={r['n']}")

out = os.path.join(os.path.dirname(__file__), '..', 'public', 'protest-results.json')
with open(out, 'w') as f:
    json.dump(results, f, indent=2)
print(f"\nSaved {len(results)} year results to {out}")
