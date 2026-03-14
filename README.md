# Tokki

## Prerequisites

- **Java 25** (or compatible JRE)
- **Apache Maven** 3.6 or later

Verify installations:

```bash
java -version
mvn -version
```

## Compilation

Compile the project:

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

### Option 1: Run the executable JAR

```bash
java -jar target/tokki-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Option 2: Run with Maven (no JAR)

```bash
mvn exec:java
```

### Option 3: Run from the project root

Ensure the `res` directory and required input files are present. Run the application from the project root so it can access them.
