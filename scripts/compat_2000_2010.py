#!/usr/bin/env python3
"""
Run CompatMalhas for census_tract 2000 vs 2010 by UF.

Usage: python compat_2000_2010.py <UF_CODE>
Example: python compat_2000_2010.py 11   # Rondônia

Input: GeoPackages in data-raw/census_tract/compat/
  setores_2000_{uf}.gpkg  (rural 2000, 1 polygon per range, ID = geocodigo)
  setores_2010_{uf}.gpkg  (individual tracts 2010, ID = code_tract)

Output: in data-raw/census_tract/compat/
  compat_{uf}_C2000_{uf}.csv   (ID_2000 → CD_MCA)
  compat_{uf}_C2010_{uf}.csv   (ID_2010 → CD_MCA)
  compat_{uf}_MCA.gpkg         (AMC geometries + graph)
  compat_{uf}_report.json      (metrics)
"""
import sys
import os

# Add vendored compat_censos to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib', 'compat_censos'))

from utils import comparability_graph, makeCensusLayer, find_utm_proj, getNeighbors
import geopandas as gpd

uf = sys.argv[1]
out_dir = os.path.join('data-raw', 'census_tract', 'compat')
os.makedirs(out_dir, exist_ok=True)

m1_path = os.path.join(out_dir, f'setores_2000_{uf}.gpkg')
m2_path = os.path.join(out_dir, f'setores_2010_{uf}.gpkg')

print(f"[compat {uf}] Loading 2010 tracts via makeCensusLayer...")
m2 = makeCensusLayer(m2_path, id_column='ID', len_higher_hierarchy=11,
                     geosys='SIRGAS2000')
print(f"[compat {uf}] 2010: {len(m2)} tracts loaded")

# For 2000: bypass makeCensusLayer because IDs contain range notation
# (e.g., "120033605000001-0003") which makeCensusLayer would garble
# by stripping non-digit characters.
print(f"[compat {uf}] Loading 2000 tracts (manual prep, preserving range IDs)...")
gdf_2000 = gpd.read_file(m1_path)
gdf_2000['ID'] = gdf_2000['ID'].astype(str)

# GROUP = first 11 digits (for ranges, use first 11 digits of the prefix)
def extract_group(id_str):
    # Strip range suffix for group extraction
    prefix = id_str.split('-')[0]
    digits = ''.join(c for c in prefix if c.isdigit())
    return digits[:11] if len(digits) >= 11 else digits

gdf_2000['GROUP'] = gdf_2000['ID'].apply(extract_group)
gdf_2000['geometry'] = gdf_2000['geometry'].make_valid()

# Reproject to UTM (same as m2)
gdf_2000 = gdf_2000.to_crs(m2.crs)

# Calculate AREA and NEIGHBOR (required by comparability_graph)
gdf_2000['AREA'] = gdf_2000.geometry.area
gdf_2000['NEIGHBOR'] = gdf_2000.apply(getNeighbors, geogrid=gdf_2000, axis=1)
print(f"[compat {uf}] 2000: {len(gdf_2000)} polygons loaded (with range notation)")

# Build compatibility graph
# ap_ratio=0.2: more tolerant for 1:500k slivers
print(f"[compat {uf}] Building compatibility graph...")
G = comparability_graph(gdf_2000, m2, ap_ratio=0.2)

# Step 1: maintenance (same ID in both censuses — works for non-range IDs)
print(f"[compat {uf}] Step 1: findMaintenances...")
G.findMaintenances()

# Step 2: splitting (L_min=0.7, tolerant for low-resolution 2000 polygons)
print(f"[compat {uf}] Step 2: findSplitings (threshold=0.7)...")
G.findSplitings(threshold=0.7)

# Step 3: forced overlay (larger buffers for 1:500k imprecision)
for b in [-50, -20]:
    print(f"[compat {uf}] Step 3: forceOverlay (buffer={b}m)...")
    G.forceOverlay(buffer=b)
print(f"[compat {uf}] Step 3: forceOverlay (buffer=0m, use_all=True)...")
G.forceOverlay(buffer=0, use_all=True)

# Export results
print(f"[compat {uf}] Exporting results...")
G.exportCompatFiles(f'compat_{uf}', f'C2000_{uf}', f'C2010_{uf}',
                    mca_base='B', dir=out_dir)
G.reportCompat(file=os.path.join(out_dir, f'compat_{uf}_report'))

print(f"[compat {uf}] Done!")
