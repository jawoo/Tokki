# Tokki

## Prerequisites

- **DSSAT 4.8**
- **Java 25** (or compatible JRE)
- **Apache Maven** 3.6 or later

Verify installations:

```bash
java -version
mvn -version
```

## Compile DSSAT

```bash
sudo yum install gfortran
sudo yum install maven
sudo yum install cmake
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
cd ~/codebase/Toco/res/.temp
mkdir summary multipleplanting flowering planting error
```

## Selecting cultivars
You'll need to flag in the cultivar file (*.CUL) to tell the program which cultivar to use. Open the cultivar file for the crop you'd like to use (e.g., MZCER.048.CUL) in the res/.csm directory and add a space and an asterisk at the end of the line, like the following:

```
990002 MEDIUM SEASON        . IB0001 200.0 0.300 800.0 700.0  8.50 38.90 *
```
You can flag as many cultivars as you like.

## Compile the project

```bash
mvn clean compile
```

Create the JAR (including dependencies):

```bash
mvn clean package
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