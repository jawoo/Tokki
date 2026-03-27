package org.cgiar.tokki;

// Apache utilities
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;

// Java utilities
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public final class CsvIO {
    private CsvIO() {}

    public static CSVParser openRfc4180(String path) throws IOException {
        // CSVParser is Closeable; callers should use try-with-resources.
        Reader in = new FileReader(path, StandardCharsets.UTF_8);
        CSVFormat format = CSVFormat.RFC4180.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .get();
        return CSVParser.parse(in, format);
    }

    public static CSVPrinter openDefaultWithHeader(String path, String... header) throws IOException {
        // CSVPrinter is Closeable; callers should use try-with-resources.
        var format = CSVFormat.DEFAULT.builder().setHeader(header).get();
        return new CSVPrinter(new FileWriter(path, StandardCharsets.UTF_8), format);
    }
}

