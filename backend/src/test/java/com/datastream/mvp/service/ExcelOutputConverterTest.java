package com.datastream.mvp.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ExcelOutputConverterTest {

    @TempDir
    Path tempDir;

    @Test
    void convertCsvToExcel_handles150kRowsWithoutEmptyOutput() throws Exception {
        Path csv = tempDir.resolve("large.csv");
        Path xlsx = tempDir.resolve("large.xlsx");
        try (BufferedWriter writer = Files.newBufferedWriter(csv, StandardCharsets.UTF_8)) {
            writer.write("id,name\n");
            for (int i = 1; i <= 150_000; i++) {
                writer.write(i + ",student-" + i + "\n");
            }
        }

        boolean success = new ExcelOutputConverter()
                .convertCsvToExcel(csv.toString(), xlsx.toString(), ",", true);

        assertTrue(success);
        assertTrue(Files.size(xlsx) > 0);
        assertTrue(Files.notExists(xlsx.resolveSibling("large.xlsx.tmp")));
    }
}
