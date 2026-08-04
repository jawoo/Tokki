# Tokki

## Prerequisites

- **Java 25** (or compatible JRE)

This repository includes the **Maven Wrapper** (`./mvnw`), which downloads **Apache Maven 3.9.10** on first use. Prefer that over a system `mvn` install: older Maven releases (for example 3.6.x from `apt`) ship libraries that trigger JDK 24+ warnings about deprecated `sun.misc.Unsafe` and related APIs when you run `mvn package`.

If you use a global Maven install instead, use **3.9.10 or newer** (3.9.12+ once available is ideal for further Guice fixes).

Verify Java:

```bash
java -version
./mvnw -version
```

## Compile DSSAT

```bash
sudo apt install gfortran
sudo apt install maven
sudo apt install cmake
mkdir codebase
cd codebase
git clone https://github.com/dssat/dssat-csm-os
cd dssat-csm-os
mkdir release
cd release
cmake -DCMAKE_BUILD_TYPE=RELEASE ..
make
```

## Preparation

Clone this project

```bash
cd ~/codebase
git clone https://github.com/jawoo/Tokki
cd Tokki
```

Copy DSSAT files to the resource directory

```bash
cd ~/codebase/Tokki/res
cp ~/codebase/dssat-csm-os/Data/* ./.csm
cp ~/codebase/dssat-csm-os/Data/BatchFiles/* ./.csm
cp ~/codebase/dssat-csm-os/Data/Default/* ./.csm
cp ~/codebase/dssat-csm-os/Data/Genotype/* ./.csm
cp ~/codebase/dssat-csm-os/Data/Pest/* ./.csm
cp ~/codebase/dssat-csm-os/Data/StandardData/* ./.csm
cp ~/codebase/dssat-csm-os/release/bin/dscsm048 ./.csm/DSCSM048.EXE

# The Genotype copy above overwrites the calibrated maize/soybean cultivar
# files tracked in this repo with stock DSSAT ones — restore the calibrated ones:
git checkout -- .csm/SBGRO048.CUL .csm/MZCER048.CUL
```

The two `.CUL` files above (`MZCER048.CUL`, `SBGRO048.CUL`) carry this project's calibration and are tracked in the repo; every other file in `res/.csm/` is stock DSSAT, copied in by the commands above. Re-run that final `git checkout` any time a fresh DSSAT copy overwrites them.

The working directories (`res/result` and the `res/.temp` subdirectories) are created automatically on the first run, so no manual setup is needed here. The program also validates its prerequisites (DSSAT binary, weather directory, input files) at startup and exits with a clear message if any are missing.

## Selecting cultivars

For maize and soybean, Tokki assigns a single **climate-matched cultivar to each grid cell** rather than running one variety everywhere. The choice is recorded explicitly in each crop record of `unit-information.jsonl`, so the input file documents exactly what was simulated where:

```json
{ "code": "MZ", "plantingDate": 120, ..., "cultivar": { "code": "990001", "name": "LONG SEASON" } }
```

The assignment uses each crop's physiological driver:

- **Maize — growing-season GDD (thermal time).** Corn is day-neutral, so relative maturity tracks accumulated heat, which varies with longitude and elevation as well as latitude. Each cell's climatological growing-season GDD (base 10 °C, 30 °C cap, planting → first fall frost, 1995–2025 mean) is stored in `res/input/cell-gdd.csv`; thresholds map it to a LONG / MEDIUM / SHORT season cultivar.
- **Soybean — maturity-group (MG) zones by latitude.** Soybean flowering is photoperiod-driven, so MG tracks latitude. Cells are assigned to published MG-zone band centres, `MG = (46 − latitude) / 2` (clamped to 0–8).

These `cultivar` fields are written by the input generator (see [Input data](#input-data)). If a crop record has no `cultivar` field, the model falls back to a latitude-matched maturity cultivar for maize/soybean, computed at run time — so older input files still run.

**Other crops, or running an explicit set.** For crops other than maize and soybean — or to force a specific set of cultivars — flag them in the cultivar file (`*.CUL`) by adding a space and an asterisk at the end of the line:

```
990002 MEDIUM SEASON        . IB0001 200.0 0.300 800.0 700.0  8.50 38.90 *
```

You can flag as many cultivars as you like; the model runs the cross-product of all flagged cultivars for those crops. (For maize and soybean the per-cell assignment above takes precedence, so flagging is not required.)

## Weather data files

Get the weather data for USA covering the maize and soybean grid cells from [here](https://huggingface.co/buckets/feedcomposer/tokki) and extract the subfolder to ./weather directory.

## Input data

The model reads three files from `res/input/` (all tracked in the repository, so a fresh clone runs as-is):

- `unit-information.jsonl` — one JSON object per grid cell, each with a nested list of crops (planting date, fertilizer rates, water supply, planting density, and an optional per-cell `cultivar`; see [Selecting cultivars](#selecting-cultivars)). The base name is set by `tableNameUnitInformation` in `config.yml`.
- `US.SOL` — DSSAT soil profiles, referenced from each cell by `soilProfileId`.
- `CO2048.csv` — annual atmospheric CO₂ history.

`unit-information.jsonl` and `US.SOL` are generated from the wide source CSV and validated against `res/input/unit-information.schema.json`. The generator also stamps each maize/soybean crop with its climate-matched `cultivar` (maize from `res/input/cell-gdd.csv`, soybean from its latitude MG zone). Regenerate them whenever the source data changes:

```bash
python3 prep/csv_to_jsonl.py   # source CSV -> unit-information.jsonl + US.SOL (schema-validated, cultivars stamped)
python3 prep/export_gis.py     # flatten to a per-crop GIS view for visual QC (res/input/qc/)
```

To (re)assign cultivars on an existing `unit-information.jsonl` without regenerating from the source CSV — for example after re-tuning maturity zones — run the stamper in place:

```bash
python3 prep/stamp_cultivars.py res/input/unit-information.jsonl   # maize by GDD, soybean by MG zone
```

`res/input/cell-gdd.csv` (the per-cell growing-season GDD used for maize assignment) is derived from the weather data; if you regenerate the weather, recompute it before re-stamping. The assignment rules live in `prep/cultivar_select.py`, mirrored by the model's run-time latitude fallback in `Utility.java` — keep the two in step if you change a rule.

See `prep/constructing_unit-information.md` for the provenance of every data column.

## Compile the project

```bash
cd ~/codebase/Tokki
./mvnw clean compile
```

Create the JAR (including dependencies):

```bash
./mvnw clean package
```

This produces:

- `target/tokki-1.0-SNAPSHOT.jar` — project classes only
- `target/tokki-1.0-SNAPSHOT-jar-with-dependencies.jar` — executable JAR including all dependencies

## Execution

Run the executable JAR

```bash
java -jar target/tokki-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Note

- After all the batch runs are completed, you can pick up the merged CSV output file at ~/codebase/Tokki/res/result directory.
- The values of model input parameters are defined in the "config.yml" file in the root directory.
