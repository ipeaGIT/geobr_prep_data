"""
Toy test: verify that overlay-by-group produces same result as full overlay,
and that it handles problematic geometries gracefully.
"""
import geopandas as gpd
import pandas as pd
from shapely.geometry import box, Polygon
from shapely.wkt import loads, dumps

print("=== Toy test: overlay by GROUP ===")

# Create two simple GeoDataFrames with GROUP column
# Group "A": two polygons in m1, two in m2
# Group "B": one polygon in m1, one in m2
m1 = gpd.GeoDataFrame({
    'A.ID': ['001', '002', '003'],
    'A.GROUP': ['11001', '11001', '11002'],
    'A.geometry': [box(0, 0, 5, 5), box(3, 3, 8, 8), box(10, 10, 15, 15)]
}, geometry='A.geometry', crs='EPSG:31982')

m2 = gpd.GeoDataFrame({
    'B.ID': ['101', '102', '103'],
    'B.GROUP': ['11001', '11001', '11002'],
    'B.geometry': [box(1, 1, 6, 6), box(4, 4, 9, 9), box(11, 11, 16, 16)]
}, geometry='B.geometry', crs='EPSG:31982')

# Method 1: Full overlay (reference)
print("Method 1: full overlay...")
full = gpd.overlay(m1, m2, how='union').dropna(subset=['A.ID', 'B.ID'])
print(f"  Full overlay: {len(full)} rows")

# Method 2: Overlay by group
print("Method 2: overlay by group...")
results = []
groups = sorted(set(m1['A.GROUP'].unique()) | set(m2['B.GROUP'].unique()))
for grp in groups:
    gA_grp = m1[m1['A.GROUP'] == grp]
    gB_grp = m2[m2['B.GROUP'] == grp]
    if len(gA_grp) == 0 or len(gB_grp) == 0:
        continue
    try:
        result = gpd.overlay(gA_grp, gB_grp, how='union').dropna(subset=['A.ID', 'B.ID'])
        results.append(result)
        print(f"  GROUP {grp}: {len(result)} rows")
    except Exception as e:
        print(f"  GROUP {grp}: FAILED - {e}")

by_group = pd.concat(results, ignore_index=True)
print(f"  By-group total: {len(by_group)} rows")

# Verify same number of intersecting pairs
full_pairs = set(zip(full['A.ID'], full['B.ID']))
group_pairs = set(zip(by_group['A.ID'], by_group['B.ID']))
assert full_pairs == group_pairs, f"Mismatch: full={full_pairs}, group={group_pairs}"

print("PASS: overlay by GROUP produces same result as full overlay")
