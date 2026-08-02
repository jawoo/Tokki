"""Stamp an explicit `cultivar` {code,name} onto every zone crop in a
unit-information JSONL (prep/cultivar_select.py): maize by growing-season GDD
(from the lookup table), soybean by latitude. Idempotent — re-running overwrites
cleanly.

Usage:
  python3 prep/stamp_cultivars.py res/input/unit-information.jsonl
  python3 prep/stamp_cultivars.py <file.jsonl> --cul-dir res/.csm --gdd res/input/cell-gdd.csv
"""

import argparse
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cultivar_select import select_cultivar


def load_gdd(path):
    """cell5m -> climatological growing-season GDD; empty dict if unavailable."""
    if not path or not os.path.exists(path):
        return {}
    gdd = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                gdd[int(row["cell5m"])] = float(row["gdd"])
            except (ValueError, KeyError):
                continue
    return gdd


def stamp(path, cul_dir, gdd_path):
    gdd_by_cell = load_gdd(gdd_path)
    records = []
    n_stamped = 0
    n_gdd = 0
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            lat = rec["y"]
            gdd = gdd_by_cell.get(rec.get("cell5m"))
            for crop in rec.get("crops", []):
                cv = select_cultivar(crop["code"], lat, cul_dir, gdd=gdd)
                if cv is not None:
                    crop["cultivar"] = cv
                    n_stamped += 1
                    if crop["code"] == "MZ" and gdd is not None:
                        n_gdd += 1
            records.append(rec)
    if n_gdd:
        print(f"> maize assigned by GDD for {n_gdd} crop-cell(s); "
              f"{'others' if n_gdd < n_stamped else 'none'} by latitude fallback")

    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")
    os.replace(tmp, path)
    return len(records), n_stamped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("jsonl", help="unit-information JSONL to stamp in place")
    ap.add_argument("--cul-dir", default="res/.csm",
                    help="directory holding the crop CUL files (default res/.csm)")
    ap.add_argument("--gdd", default="res/input/cell-gdd.csv",
                    help="cell5m,gdd lookup for maize assignment (default res/input/cell-gdd.csv; "
                         "maize falls back to latitude if a cell is missing)")
    args = ap.parse_args()
    cells, stamped = stamp(args.jsonl, args.cul_dir, args.gdd)
    print(f"> stamped {stamped} crop-cultivar(s) across {cells} cell(s) in {args.jsonl}")


if __name__ == "__main__":
    main()
