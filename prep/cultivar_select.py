"""Maturity cultivar selection for the input generator.

Assignment drivers are chosen per crop's physiology:
  * Maize relative maturity is thermal-time driven, so maize is assigned by the
    cell's climatological growing-season GDD (base 10 C, 30 C cap, planting ->
    first fall frost, 1995-2025 mean) when a GDD value is supplied. GDD captures
    the longitude/elevation/continentality gradients that latitude alone misses
    (e.g. Michigan and Wisconsin run shorter maturities than their latitude
    implies). Falls back to latitude when no GDD is available.
  * Soybean maturity group is photoperiod (day-length) driven, i.e. essentially
    latitudinal, so soybean stays on the latitude rule.

The latitude fallback here is the Python twin of Utility.maturityTarget /
selectMaturityCultivar in the Java model — kept in lockstep so an *unstamped*
record reproduces exactly what the model would pick at run time. The GDD path is
Python-only (the model has no weather at cultivar-selection time); stamped files
carry the result explicitly, so the model never needs to recompute it.

Only zone crops (maize, soybean) get an explicit cultivar; every other crop
returns None and stays on the run-time fallback (the flagged cross-product).
"""

import os

ZONE_CROPS = ("MZ", "SB")

# Maize GDD -> relative-maturity class boundaries (growing-season GDD as computed
# by prep GDD tooling; see res/input/cell-gdd.csv). LONG and MEDIUM currently
# share calibrated coefficients, so the LONG/MEDIUM split only refines the map;
# the SHORT boundary is the yield-relevant one (SHORT uses distinct coefficients).
MAIZE_GDD_LONG = 1600.0     # >= this -> LONG (full-season belt and south)
MAIZE_GDD_MEDIUM = 1250.0   # >= this -> MEDIUM (northern transition); else SHORT

_cache = {}   # cropCode -> {maturity index: (code, name)}


def _model_name_version(code):
    if code in ("BA", "MZ", "SG", "WH"):
        return "CER048"
    if code in ("SB", "FB", "CH"):
        return "GRO048"
    if code == "TF":
        return "APS048"
    return "048"


def _maturity_index(crop_code, name):
    """Maturity index for a generic maturity cultivar name, or None. Mirrors
    Utility.maturityIndex exactly (including substring bounds at the caller)."""
    if crop_code == "SB":
        if not name.startswith("M GROUP"):
            return None
        tok = name[7:].strip()
        if tok == "000":
            return -2.0
        if tok == "00":
            return -1.0
        try:
            return float(int(tok))
        except ValueError:
            return None            # named lines e.g. Savoy, Vinton
    if crop_code == "MZ":
        return {"V.SHORT SEASON": 0.0, "SHORT SEASON": 1.0,
                "MEDIUM SEASON": 2.0, "LONG SEASON": 3.0}.get(name)
    return None


def _maturity_cultivars(crop_code, cul_dir):
    if crop_code in _cache:
        return _cache[crop_code]
    result = {}
    path = os.path.join(cul_dir, crop_code + _model_name_version(crop_code) + ".CUL")
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if len(line) <= 70:                       # same guard as the Java reader
                continue
            code = line[0:6].strip()
            name = line[7:24].strip()
            idx = _maturity_index(crop_code, name)
            if idx is not None and idx not in result:  # first (canonical) per index
                result[idx] = (code, name)
    _cache[crop_code] = result
    return result


def _maturity_target(crop_code, abs_lat):
    if crop_code == "SB":
        # Soybean MG tracks latitude (photoperiod). Anchored to published MG-zone
        # band centres (MG0 ~46N ... MG8 ~30N; Mourtzinis & Conley 2017 and the
        # classic delineation): MG = (46 - lat)/2. Matches the core belt exactly
        # and corrects the deep south (longer MG) and far north (shorter MG).
        mg = (46.0 - abs_lat) / 2.0
        return max(0.0, min(8.0, mg))
    if abs_lat >= 47.0:
        return 1.0
    if abs_lat >= 44.0:
        return 2.0
    return 3.0


def _maize_target_gdd(gdd):
    """Maize season rank (LONG=3, MEDIUM=2, SHORT=1) from growing-season GDD."""
    if gdd >= MAIZE_GDD_LONG:
        return 3.0
    if gdd >= MAIZE_GDD_MEDIUM:
        return 2.0
    return 1.0


def select_cultivar(crop_code, latitude, cul_dir, gdd=None):
    """Return {'code':..., 'name':...} for a zone crop, or None to leave the
    record on the run-time fallback (non-zone crops / empty CUL).

    Maize uses growing-season GDD when supplied (else latitude); soybean always
    uses latitude (its maturity group is photoperiodic)."""
    if crop_code not in ZONE_CROPS:
        return None
    cultivars = _maturity_cultivars(crop_code, cul_dir)
    if not cultivars:
        return None
    if crop_code == "MZ" and gdd is not None:
        target = _maize_target_gdd(gdd)
    else:
        target = _maturity_target(crop_code, abs(latitude))
    # Nearest index; ties resolve to the smallest index, matching the Java loop
    # (strict `<` over ascending TreeMap keys keeps the first minimum).
    best = min(sorted(cultivars.keys()), key=lambda k: abs(k - target))
    code, name = cultivars[best]
    return {"code": code, "name": name}
