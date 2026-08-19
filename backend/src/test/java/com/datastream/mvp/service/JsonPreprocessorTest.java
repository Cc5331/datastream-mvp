package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JsonPreprocessor 单测：JSON 数组探测与 array → CSV 转换
 */
class JsonPreprocessorTest {

    private final JsonPreprocessor pre = new JsonPreprocessor(new ObjectMapper());

    @Test
    void arrayFile_isDetected() throws Exception {
        Path f = Files.createTempFile("arr", ".json");
        Files.writeString(f, "[{\"a\":1},{\"a\":2}]", StandardCharsets.UTF_8);
        assertTrue(JsonPreprocessor.isJsonArrayFile(f.toString()));
    }

    @Test
    void linesFile_isNotArray() throws Exception {
        Path f = Files.createTempFile("lines", ".json");
        Files.writeString(f, "{\"a\":1}\n{\"a\":2}", StandardCharsets.UTF_8);
        assertFalse(JsonPreprocessor.isJsonArrayFile(f.toString()));
    }

    @Test
    void arrayToCsv_convertsRowsWithHeader() throws Exception {
        Path f = Files.createTempFile("arr2", ".json");
        Files.writeString(f, "[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]", StandardCharsets.UTF_8);
        String csv = pre.convertArrayToCsv(f.toString(), ",", "UTF-8");
        assertNotNull(csv, "array 模式应成功转 CSV");
        String content = Files.readString(Path.of(csv), StandardCharsets.UTF_8);
        assertTrue(content.contains("id"), "CSV 应包含表头 id: " + content);
        assertTrue(content.contains("1"), "CSV 应包含数据行: " + content);
    }
}
