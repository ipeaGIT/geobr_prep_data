"""
Test: fix the single failing GROUP 25137030500 with buffer(0) + make_valid
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib', 'compat_censos'))

import geopandas as gpd
from shapely.wkt import loads, dumps
from shapely import make_valid
from utils import makeCensusLayer, find_utm_proj, getNeighbors

uf = '25'
out_dir = os.path.join(os.path.dirname(__file__), '..', 'data-raw', 'census_tract', 'compat')

print(f"Loading data...")
m2 = makeCensusLayer(os.path.join(out_dir, f'setores_2010_{uf}.gpkg'),
                     id_column='ID', len_higher_hierarchy=11, geosys='SIRGAS2000')
gdf_2000 = gpd.read_file(os.path.join(out_dir, f'setores_2000_{uf}.gpkg'))
gdf_2000['ID'] = gdf_2000['ID'].astype(str)
gdf_2000['GROUP'] = gdf_2000['ID'].apply(lambda x: ''.join(c for c in x.split('-')[0] if c.isdigit())[:11])
gdf_2000['geometry'] = gdf_2000['geometry'].make_valid()
gdf_2000 = gdf_2000.to_crs(m2.crs)

def prepare(gdf, prefix):
    utmcrs = gdf.crs
    gdf = gdf.rename(columns={k: f'{prefix}.{k}' for k in gdf.columns})
    gdf = gdf.set_geometry(f'{prefix}.geometry', crs=utmcrs)
    gdf[f'{prefix}.geometry'] = [loads(dumps(g, rounding_precision=3)) for g in gdf[f'{prefix}.geometry']]
    return gdf

gA = prepare(gdf_2000, 'A')
gB = prepare(m2, 'B')

grp = '25137030500'
gA_grp = gA[gA['A.GROUP'] == grp]
gB_grp = gB[gB['B.GROUP'] == grp]
print(f"GROUP {grp}: {len(gA_grp)} in m1, {len(gB_grp)} in m2")

# Attempt 1: direct overlay (should fail)
print("\nAttempt 1: direct overlay...")
try:
    r = gpd.overlay(gA_grp, gB_grp, how='union')
    print(f"  OK: {len(r)} rows")
except Exception as e:
    print(f"  FAILED: {str(e)[:80]}")

# Attempt 2: buffer(0) + make_valid
print("\nAttempt 2: buffer(0) + make_valid...")
try:
    gA_fix = gA_grp.copy()
    gB_fix = gB_grp.copy()
    gA_fix['A.geometry'] = gA_fix['A.geometry'].buffer(0).make_valid()
    gB_fix['B.geometry'] = gB_fix['B.geometry'].buffer(0).make_valid()
    r = gpd.overlay(gA_fix, gB_fix, how='union')
    print(f"  OK: {len(r)} rows")
except Exception as e:
    print(f"  FAILED: {str(e)[:80]}")

# Attempt 3: lower rounding precision (2 instead of 3)
print("\nAttempt 3: rounding_precision=2...")
try:
    gA_rp2 = gA_grp.copy()
    gB_rp2 = gB_grp.copy()
    gA_rp2['A.geometry'] = [loads(dumps(g, rounding_precision=2)) for g in gA_rp2['A.geometry']]
    gB_rp2['B.geometry'] = [loads(dumps(g, rounding_precision=2)) for g in gB_rp2['B.geometry']]
    gA_rp2['A.geometry'] = gA_rp2['A.geometry'].make_valid()
    gB_rp2['B.geometry'] = gB_rp2['B.geometry'].make_valid()
    r = gpd.overlay(gA_rp2, gB_rp2, how='union')
    print(f"  OK: {len(r)} rows")
except Exception as e:
    print(f"  FAILED: {str(e)[:80]}")

# Attempt 4: use intersection instead of union
print("\nAttempt 4: overlay how='intersection'...")
try:
    r = gpd.overlay(gA_grp, gB_grp, how='intersection')
    print(f"  OK: {len(r)} rows")
except Exception as e:
    print(f"  FAILED: {str(e)[:80]}")

# Attempt 5: grid_size parameter in shapely
print("\nAttempt 5: individual pairwise intersection...")
try:
    from shapely import intersection
    count = 0
    for _, a_row in gA_grp.iterrows():
        for _, b_row in gB_grp.iterrows():
            try:
                inter = a_row['A.geometry'].intersection(b_row['B.geometry'])
                if not inter.is_empty and inter.area > 0:
                    count += 1
            except:
                pass
    print(f"  OK: {count} intersecting pairs found")
except Exception as e:
    print(f"  FAILED: {str(e)[:80]}")
