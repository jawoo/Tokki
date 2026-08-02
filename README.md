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
```

The working directories (`res/result` and the `res/.temp` subdirectories) are created automatically on the first run, so no manual setup is needed here. The program also validates its prerequisites (DSSAT binary, weather directory, input files) at startup and exits with a clear message if any are missing.

## Selecting cultivars

You'll need to flag in the cultivar file (*.CUL) to tell the program which cultivar to use. Open the cultivar file for the crop you'd like to use (e.g., MZCER048.CUL) in the res/.csm directory and add a space and an asterisk at the end of the line, like the following:

```
990002 MEDIUM SEASON        . IB0001 200.0 0.300 800.0 700.0  8.50 38.90 *
```

You can flag as many cultivars as you like.

## Weather data files

Get the weather data for USA covering the maize and soybean grid cells from [here](https://cgiar-my.sharepoint.com/:u:/g/personal/j_koo_cgiar_org/IQCLPhLwhm9JRKrZJ_KLMUb1AXMOhLSemFW72fI61zo_rRM?e=9VogL4) and extract the subfolder to ./weather directory.

## Input data

The model reads three files from `res/input/` (all tracked in the repository, so a fresh clone runs as-is):

- `unit-information.jsonl` — one JSON object per grid cell, each with a nested list of crops (planting date, fertilizer rates, water supply, planting density, …). The base name is set by `tableNameUnitInformation` in `config.yml`.
- `US.SOL` — DSSAT soil profiles, referenced from each cell by `soilProfileId`.
- `CO2048.csv` — annual atmospheric CO₂ history.

`unit-information.jsonl` and `US.SOL` are generated from the wide source CSV and validated against `res/input/unit-information.schema.json`. Regenerate them whenever the source data changes:

```bash
python3 prep/csv_to_jsonl.py   # source CSV -> unit-information.jsonl + US.SOL (schema-validated)
python3 prep/export_gis.py     # flatten to a per-crop GIS view for visual QC (res/input/qc/)
```

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