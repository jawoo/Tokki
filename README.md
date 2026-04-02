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

Create some additional directories to collect and process temporary files

```bash
cd ~/codebase/Tokki/res
mkdir result
cd ~/codebase/Tokki/res/.temp
mkdir summary flowering planting error
```

## Selecting cultivars

You'll need to flag in the cultivar file (*.CUL) to tell the program which cultivar to use. Open the cultivar file for the crop you'd like to use (e.g., MZCER048.CUL) in the res/.csm directory and add a space and an asterisk at the end of the line, like the following:

```
990002 MEDIUM SEASON        . IB0001 200.0 0.300 800.0 700.0  8.50 38.90 *
```

You can flag as many cultivars as you like.

## Weather data files

Get the weather data for USA covering the maize and soybean grid cells from [here](https://cgiar-my.sharepoint.com/:u:/g/personal/j_koo_cgiar_org/IQCLPhLwhm9JRKrZJ_KLMUb1AXMOhLSemFW72fI61zo_rRM?e=9VogL4) and extract the subfolder to ./weather directory.


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

- After all the batch runs are completed, you can pick up the merged CSV output file at ~/codebase/Toco/res/result directory.
- The values of model input parameters are defined in the "config.yml" file in the root directory.