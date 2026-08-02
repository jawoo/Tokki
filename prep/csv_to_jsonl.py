#!/usr/bin/env python3
"""
Convert the wide unit-information CSV into the canonical model inputs:

  1. unit-information.jsonl  — one JSON object per grid cell, crops as a nested
                               list of self-contained named records (no parallel
                               comma-packed columns, so a missing crop-specific
                               value can no longer silently desync other columns).
  2. US.SOL                  — all DSSAT soil profiles concatenated, keyed by
                               soilProfileId, pulled out of the table so the
                               input stays small and free of multi-line fields.

A soilRootingDepth of 0 (no restrictive layer recorded) is filled to
NO_RESTRICTION_DEPTH cm, matching the prep pipeline's NULL-fill rule.

Every record is checked against unit-information.schema.json (essential checks
are also done inline so the script is useful without the `jsonschema` package).
A crop with a blank/invalid required field is skipped with a message naming the
CELL5M; a cell left with no valid crops is skipped entirely.

Usage:
  python3 prep/csv_to_jsonl.py                    # uses the defaults below
  python3 prep/csv_to_jsonl.py --input path.csv --outdir res/input
"""

import argparse
import csv
import json
import os
import re
import sys
import warnings

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cultivar_select import select_cultivar

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE_DIR)
INPUT_DIR = os.path.join(REPO_ROOT, "res", "input")
CUL_DIR = os.path.join(REPO_ROOT, "res", ".csm")
GDD_PATH = os.path.join(INPUT_DIR, "cell-gdd.csv")

DEFAULT_INPUT = os.path.join(INPUT_DIR, "unit-information_usa-mzsb-47707.csv")
DEFAULT_OUTDIR = INPUT_DIR
SCHEMA_PATH = os.path.join(INPUT_DIR, "unit-information.schema.json")


def _load_gdd(path):
    """cell5m -> climatological growing-season GDD for maize assignment; empty
    dict (latitude fallback) if the table is absent."""
    table = {}
    if os.path.exists(path):
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                try:
                    table[int(r["cell5m"])] = float(r["gdd"])
                except (ValueError, KeyError):
                    continue
    return table


GDD_BY_CELL = _load_gdd(GDD_PATH)

# Per-crop columns (comma-packed in the wide CSV) → JSON crop keys.
CROP_COLUMNS = {
    "PlantingDates": "plantingDate",
    "Areas": "area",
    "NFertRateAct": "nFertRateAct",
    "NFertRateRec": "nFertRateRec",
    "WaterSupply": "waterSupply",
    "PlantingDensity": "plantingDensity",
}
# Larger CSVs carry very wide multi-line soil fields.
csv.field_size_limit(10_000_000)

# A rooting depth of 0 means no restrictive layer was recorded; treat it as the
# documented "no restriction" fill value (cm) rather than a real 0 cm depth,
# which the model would otherwise floor to 40 cm and badly under-water the cell.
NO_RESTRICTION_DEPTH = 200


def to_int(s):
    s = s.strip()
    return int(s) if re.fullmatch(r"-?\d+", s) else int(round(float(s)))


def to_float(s):
    return float(s.strip())


def build_crop(code, values):
    """Assemble one typed crop dict from its per-column string values.

    Returns (crop_dict, error_message). Exactly one is non-None.
    """
    code = code.strip()
    try:
        crop = {
            "code": code,
            "plantingDate": to_int(values["PlantingDates"]),
            "area": to_float(values["Areas"]),
            "nFertRateAct": to_float(values["NFertRateAct"]),
            "nFertRateRec": to_float(values["NFertRateRec"]),
            "waterSupply": values["WaterSupply"].strip(),
            "plantingDensity": to_float(values["PlantingDensity"]),
        }
    except (ValueError, KeyError) as ex:
        blanks = [c for c, v in values.items() if not v.strip()]
        detail = f"blank {blanks}" if blanks else f"{ex}"
        return None, f"crop '{code}': cannot parse ({detail}) from {values}"
    return crop, None


def convert(input_csv, outdir):
    jsonl_path = os.path.join(outdir, "unit-information.jsonl")
    sol_path = os.path.join(outdir, "US.SOL")

    soils = {}            # soilProfileId -> profile text
    soil_conflicts = 0
    records = []
    skipped_crops = 0
    skipped_cells = 0
    filled_rooting = 0

    with open(input_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cell = row["CELL5M"].strip()
            crops_raw = [c.strip() for c in row["Crops"].split(",")]
            # Keep trailing empties (split with no cap) so counts stay aligned.
            packed = {col: row[col].split(",") for col in CROP_COLUMNS}

            # Every per-crop column must carry one value per crop.
            nc = len(crops_raw)
            mismatched = {col: len(v) for col, v in packed.items() if len(v) != nc}
            if mismatched:
                print(f"! CELL5M {cell}: per-crop column count mismatch "
                      f"(Crops={nc}, {mismatched}) — skipping cell", file=sys.stderr)
                skipped_cells += 1
                continue

            crops = []
            for i, code in enumerate(crops_raw):
                values = {col: packed[col][i] for col in CROP_COLUMNS}
                crop, err = build_crop(code, values)
                if err:
                    print(f"! CELL5M {cell}: {err} — skipping crop", file=sys.stderr)
                    skipped_crops += 1
                    continue
                crops.append(crop)

            if not crops:
                print(f"! CELL5M {cell}: no valid crops — skipping cell", file=sys.stderr)
                skipped_cells += 1
                continue

            # Collect the soil profile, de-duplicated by id.
            soil_id = row["SoilProfileID"].strip()
            profile = row["SoilProfile"].strip("\n")
            if soil_id in soils and soils[soil_id] != profile:
                print(f"! SoilProfileID {soil_id}: conflicting profile text — keeping first",
                      file=sys.stderr)
                soil_conflicts += 1
            else:
                soils[soil_id] = profile

            rooting_depth = to_int(row["SoilRootingDepth"])
            if rooting_depth == 0:
                rooting_depth = NO_RESTRICTION_DEPTH
                filled_rooting += 1

            lat = to_float(row["Y"])
            # Stamp each zone crop with its cultivar so the file fully documents
            # what the model runs (non-zone crops get None and stay on the
            # run-time fallback): maize by growing-season GDD (if available for
            # the cell), soybean by latitude. See prep/cultivar_select.py.
            cell_gdd = GDD_BY_CELL.get(to_int(cell))
            for crop in crops:
                cv = select_cultivar(crop["code"], lat, CUL_DIR, gdd=cell_gdd)
                if cv is not None:
                    crop["cultivar"] = cv

            records.append({
                "unitId": to_int(row["UnitID"]),
                "cell5m": to_int(cell),
                "x": to_float(row["X"]),
                "y": lat,
                "soilProfileId": soil_id,
                "soilRootingDepth": rooting_depth,
                "crops": crops,
            })

    with open(jsonl_path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")

    with open(sol_path, "w") as f:
        for soil_id in sorted(soils):
            f.write(soils[soil_id].rstrip("\n"))
            f.write("\n\n")

    return {
        "jsonl": jsonl_path,
        "sol": sol_path,
        "cells": len(records),
        "crops": sum(len(r["crops"]) for r in records),
        "soils": len(soils),
        "skipped_crops": skipped_crops,
        "skipped_cells": skipped_cells,
        "soil_conflicts": soil_conflicts,
        "filled_rooting": filled_rooting,
        "records": records,
    }


def validate(records, schema_path):
    """Validate every record against the JSON Schema, if jsonschema is available."""
    try:
        import jsonschema
    except ImportError:
        print("> jsonschema not installed — skipping full schema validation "
              "(inline integrity checks already applied). `pip install jsonschema` to enable.")
        return 0

    with open(schema_path) as f:
        schema = json.load(f)

    # Prefer the draft named by $schema; fall back to the newest draft the
    # installed jsonschema supports (keywords used here are Draft7-compatible).
    # Older jsonschema warns when it doesn't know the 2020-12 metaschema URL and
    # falls back to its latest draft — that fallback is exactly what we want here.
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*metaschema specified by \\$schema.*")
            validator = jsonschema.validators.validator_for(schema)(schema)
    except (AttributeError, KeyError):
        for name in ("Draft202012Validator", "Draft201909Validator", "Draft7Validator"):
            cls = getattr(jsonschema, name, None)
            if cls is not None:
                validator = cls(schema)
                break
        else:
            raise

    errors = 0
    for rec in records:
        for err in validator.iter_errors(rec):
            errors += 1
            if errors <= 20:
                loc = "/".join(str(p) for p in err.absolute_path) or "(root)"
                print(f"! schema: cell5m={rec.get('cell5m')} at {loc}: {err.message}",
                      file=sys.stderr)
    if errors:
        print(f"> Schema validation FAILED: {errors} error(s).")
    else:
        print(f"> Schema validation passed for all {len(records)} records.")
    return errors


def main():
    ap = argparse.ArgumentParser(description="Convert wide unit-information CSV to JSONL + US.SOL")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="wide unit-information CSV")
    ap.add_argument("--outdir", default=DEFAULT_OUTDIR, help="output directory")
    ap.add_argument("--schema", default=SCHEMA_PATH, help="JSON Schema for validation")
    ap.add_argument("--no-validate", action="store_true", help="skip JSON Schema validation")
    args = ap.parse_args()

    if not os.path.isfile(args.input):
        sys.exit(f"Input not found: {args.input}")

    r = convert(args.input, args.outdir)
    print(f"> Wrote {r['jsonl']}  ({r['cells']} cells, {r['crops']} crop-units)")
    print(f"> Wrote {r['sol']}  ({r['soils']} soil profiles)")
    if r["filled_rooting"]:
        print(f"> Filled soilRootingDepth 0 -> {NO_RESTRICTION_DEPTH} cm for "
              f"{r['filled_rooting']} cell(s) (no restrictive layer recorded).")
    if r["skipped_crops"] or r["skipped_cells"]:
        print(f"> Skipped {r['skipped_crops']} crop(s) and {r['skipped_cells']} cell(s) "
              f"with missing/invalid data (see messages above).")
    if r["soil_conflicts"]:
        print(f"> {r['soil_conflicts']} soil id(s) had conflicting profile text.")

    errors = 0
    if not args.no_validate:
        errors = validate(r["records"], args.schema)

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
