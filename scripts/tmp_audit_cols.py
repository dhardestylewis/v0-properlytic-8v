import polars as pl

lf = pl.scan_parquet(r"G:\My Drive\HCAD_Archive_Aggregates\hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet")
schema = lf.collect_schema()
cols = schema.names()

cat_cols = [c for c in cols if c not in ["acct","yr","tot_appr_val"] and "str" in str(schema.get(c)).lower()]
num_cols = [c for c in cols if c not in ["acct","yr","tot_appr_val"] and c not in cat_cols]

out = []
out.append(f"Total: {len(cols)} cols | Numeric: {len(num_cols)} | Cat: {len(cat_cols)}")
out.append("")
out.append("NUMERIC (>> = used by model, first 30):")
for j, c in enumerate(num_cols):
    m = ">>" if j < 30 else "  "
    t = str(schema.get(c))
    out.append(f"  {m} {j:2d} {c} [{t}]")
out.append("")
out.append("CATEGORICAL (>> = used, first 10):")
for j, c in enumerate(cat_cols):
    m = ">>" if j < 10 else "  "
    out.append(f"  {m} {j:2d} {c}")

text = "\n".join(out)
print(text)
with open("scripts/panel_audit.txt", "w") as f:
    f.write(text)
