"""
Test: can overlay-by-group succeed for UF 25 (PB) where full overlay fails?
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib', 'compat_censos'))

import geopandas as gpd
import pandas as pd
from shapely.wkt import loads, dumps
from utils import makeCensusLayer, find_utm_proj, getNeighbors

uf = '25'
out_dir = os.path.join(os.path.dirname(__file__), '..', 'data-raw', 'census_tract', 'compat')

print(f"[test] Loading 2010 tracts for UF {uf}...")
m2 = makeCensusLayer(os.path.join(out_dir, f'setores_2010_{uf}.gpkg'),
                     id_column='ID', len_higher_hierarchy=11, geosys='SIRGAS2000')
print(f"  2010: {len(m2)} tracts, CRS: {m2.crs}")

print(f"[test] Loading 2000 tracts for UF {uf}...")
gdf_2000 = gpd.read_file(os.path.join(out_dir, f'setores_2000_{uf}.gpkg'))
gdf_2000['ID'] = gdf_2000['ID'].astype(str)
gdf_2000['GROUP'] = gdf_2000['ID'].apply(lambda x: ''.join(c for c in x.split('-')[0] if c.isdigit())[:11])
gdf_2000['geometry'] = gdf_2000['geometry'].make_valid()
gdf_2000 = gdf_2000.to_crs(m2.crs)
gdf_2000['AREA'] = gdf_2000.geometry.area
gdf_2000['NEIGHBOR'] = gdf_2000.apply(lambda x: getNeighbors(x, gdf_2000), axis=1)
print(f"  2000: {len(gdf_2000)} polygons")

# Prepare for overlay (same as _prepareGridForUnion)
def prepare(gdf, prefix):
    utmcrs = gdf.crs
    gdf = gdf.rename(columns={k: f'{prefix}.{k}' for k in gdf.columns})
    gdf = gdf.set_geometry(f'{prefix}.geometry', crs=utmcrs)
    gdf[f'{prefix}.geometry'] = [loads(dumps(g, rounding_precision=3)) for g in gdf[f'{prefix}.geometry']]
    return gdf

gA = prepare(gdf_2000, 'A')
gB = prepare(m2, 'B')

# Test 1: Full overlay (should fail)
print("\n[test] Attempting FULL overlay...")
try:
    full = gpd.overlay(gA, gB, how='union').dropna(subset=['A.ID', 'B.ID'])
    print(f"  Full overlay SUCCEEDED: {len(full)} rows")
except Exception as e:
    print(f"  Full overlay FAILED: {type(e).__name__}: {str(e)[:100]}")

# Test 2: Overlay by GROUP
print("\n[test] Attempting overlay BY GROUP...")
results = []
groups = sorted(set(gA['A.GROUP'].unique()) | set(gB['B.GROUP'].unique()))
n_ok = 0
n_fail = 0
n_empty = 0
for grp in groups:
    gA_grp = gA[gA['A.GROUP'] == grp]
    gB_grp = gB[gB['B.GROUP'] == grp]
    if len(gA_grp) == 0 or len(gB_grp) == 0:
        n_empty += 1
        continue
    try:
        result = gpd.overlay(gA_grp, gB_grp, how='union').dropna(subset=['A.ID', 'B.ID'])
        results.append(result)
        n_ok += 1
    except Exception as e:
        n_fail += 1
        print(f"  GROUP {grp}: FAILED - {str(e)[:80]}")

if results:
    by_group = pd.concat(results, ignore_index=True)
else:
    by_group = pd.DataFrame()

print(f"\n[test] Results:")
print(f"  Groups total: {len(groups)}")
print(f"  Groups OK: {n_ok}")
print(f"  Groups failed: {n_fail}")
print(f"  Groups empty: {n_empty}")
print(f"  Total overlay rows: {len(by_group)}")

if n_fail == 0:
    print("\nPASS: all groups succeeded")
else:
    print(f"\nPARTIAL: {n_fail} groups failed out of {len(groups)}")
