#!/usr/bin/env python3
"""
Export the canonical unit-information.jsonl to a GIS-friendly, flat view for
visual QC (spotting streaks, banding, or holes in any data column).

The JSONL is nested (crops as a list), which GIS software cannot theme
directly. This produces one FLAT table per crop — each attribute in its own
column, one point per cell — plus a `cells` summary layer. Because the export
is generated from the validated JSONL, its columns can never desync; a crop
that is absent at a cell simply produces no point in that crop's layer, so
coverage gaps show up as visible holes.

Output (into --outdir, default res/input/qc):
  - GeoPackage `unit-information_qc.gpkg` with layers crops_MZ, crops_SB, …,
    and `cells`  — when geopandas is installed (single file, typed columns).
  - otherwise CSV per layer (crops_MZ.csv, …, cells.csv) with x,y columns,
    loadable in QGIS via "Add Delimited Text Layer".

Each crop layer carries the crop attributes, the cell's soil fields (for soil
QC too), and two derived helpers: nSurplus (act - rec) and irrigated (0/1).

Usage:
  python3 prep/export_gis.py
  python3 prep/export_gis.py --jsonl res/input/unit-information.jsonl --outdir res/input/qc
"""

import argparse
import collections
import json
import os
import sys

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE_DIR)
INPUT_DIR = os.path.join(REPO_ROOT, "res", "input")

DEFAULT_JSONL = os.path.join(INPUT_DIR, "unit-information.jsonl")
DEFAULT_OUTDIR = os.path.join(INPUT_DIR, "qc")

# Numeric columns whose spatial range is worth a quick terminal sanity check.
RANGE_COLS = ["plantingDate", "nFertRateAct", "nFertRateRec", "nSurplus",
              "plantingDensity", "soilRootingDepth"]


def load_cells(jsonl_path):
    cells = []
    with open(jsonl_path) as f:
        for line in f:
            line = line.strip()
            if line:
                cells.append(json.loads(line))
    return cells


def build_tables(cells):
    """Return (per_crop: code -> list[row], cell_rows: list[row])."""
    per_crop = collections.defaultdict(list)
    cell_rows = []
    for c in cells:
        soil = {
            "cell5m": c["cell5m"],
            "x": c["x"],
            "y": c["y"],
            "unitId": c["unitId"],
            "soilProfileId": c["soilProfileId"],
            "soilRootingDepth": c["soilRootingDepth"],
        }
        codes = [cr["code"] for cr in c["crops"]]
        cell_rows.append({**soil, "crops": ",".join(codes), "nCrops": len(codes)})

        for cr in c["crops"]:
            per_crop[cr["code"]].append({
                **soil,
                "code": cr["code"],
                "plantingDate": cr["plantingDate"],
                "area": cr["area"],
                "nFertRateAct": cr["nFertRateAct"],
                "nFertRateRec": cr["nFertRateRec"],
                "waterSupply": cr["waterSupply"],
                "plantingDensity": cr["plantingDensity"],
                # derived QC helpers
                "nSurplus": round(cr["nFertRateAct"] - cr["nFertRateRec"], 3),
                "irrigated": 1 if cr["waterSupply"] == "I" else 0,
            })
    return per_crop, cell_rows


def write_geopackage(per_crop, cell_rows, outdir):
    import geopandas as gpd
    from shapely.geometry import Point

    def to_gdf(rows):
        df = pd.DataFrame(rows)
        geom = [Point(xy) for xy in zip(df["x"], df["y"])]
        return gpd.GeoDataFrame(df, geometry=geom, crs="EPSG:4326")

    gpkg = os.path.join(outdir, "unit-information_qc.gpkg")
    if os.path.exists(gpkg):
        os.remove(gpkg)  # rewrite cleanly rather than append to stale layers
    for code, rows in sorted(per_crop.items()):
        to_gdf(rows).to_file(gpkg, layer=f"crops_{code}", driver="GPKG")
    to_gdf(cell_rows).to_file(gpkg, layer="cells", driver="GPKG")
    return [gpkg]


def write_csv(per_crop, cell_rows, outdir):
    written = []
    for code, rows in sorted(per_crop.items()):
        path = os.path.join(outdir, f"crops_{code}.csv")
        pd.DataFrame(rows).to_csv(path, index=False)
        written.append(path)
    path = os.path.join(outdir, "cells.csv")
    pd.DataFrame(cell_rows).to_csv(path, index=False)
    written.append(path)
    return written


def print_ranges(per_crop):
    for code, rows in sorted(per_crop.items()):
        df = pd.DataFrame(rows)
        bits = []
        for col in RANGE_COLS:
            if col in df:
                bits.append(f"{col} {df[col].min():g}..{df[col].max():g}")
        print(f"  {code} ({len(rows)} pts): " + ", ".join(bits))


def main():
    ap = argparse.ArgumentParser(description="Export unit-information.jsonl to a flat GIS QC view")
    ap.add_argument("--jsonl", default=DEFAULT_JSONL, help="input JSONL")
    ap.add_argument("--outdir", default=DEFAULT_OUTDIR, help="output directory")
    ap.add_argument("--csv", action="store_true", help="force CSV output even if geopandas is available")
    args = ap.parse_args()

    if not os.path.isfile(args.jsonl):
        sys.exit(f"Input not found: {args.jsonl} (run prep/csv_to_jsonl.py first)")
    os.makedirs(args.outdir, exist_ok=True)

    cells = load_cells(args.jsonl)
    per_crop, cell_rows = build_tables(cells)

    have_geopandas = False
    if not args.csv:
        try:
            import geopandas  # noqa: F401
            have_geopandas = True
        except ImportError:
            pass

    if have_geopandas:
        written = write_geopackage(per_crop, cell_rows, args.outdir)
        print(f"> Wrote GeoPackage: {written[0]}")
        print(f"  layers: {', '.join('crops_' + c for c in sorted(per_crop))}, cells")
    else:
        if not args.csv:
            print("> geopandas not installed — writing CSV layers "
                  "(load in QGIS via 'Add Delimited Text Layer', X=x, Y=y). "
                  "`pip install geopandas` for a single GeoPackage.")
        written = write_csv(per_crop, cell_rows, args.outdir)
        for p in written:
            print(f"> Wrote {p}")

    print(f"> {len(cell_rows)} cells, {sum(len(r) for r in per_crop.values())} crop-points. Value ranges:")
    print_ranges(per_crop)


if __name__ == "__main__":
    main()
