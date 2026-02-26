"""Print MVT layer names and feature properties."""
import urllib.request
import mapbox_vector_tile as mvt

d = urllib.request.urlopen('http://localhost:3000/api/forecast-tiles/10/233/421?year=2024', timeout=30).read()
t = mvt.decode(d)
for name in t:
    feats = t[name]['features']
    keys = list(feats[0]['properties'].keys()) if feats else []
    print(f"Layer '{name}': {len(feats)} features, property_keys={keys}")
    if feats:
        print(f"  Sample props: {feats[0]['properties']}")
        print(f"  Feature id field: {feats[0].get('id', 'MISSING')}")
