package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON 文件预处理服务（array 模式）
 * 将 JSON 数组文件（[{...},{...}]）拆为临时 CSV；嵌套对象/数组保留为 JSON 字符串列。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JsonPreprocessor {

    private final ObjectMapper objectMapper;

    /**
     * 判断文件是否为 JSON 数组（首个非空白字符为 '['）
     */
    public static boolean isJsonArrayFile(String path) {
        if (path == null || path.isEmpty()) return false;
        try (BufferedReader r = Files.newBufferedReader(new java.io.File(path).toPath(), StandardCharsets.UTF_8)) {
            int c;
            while ((c = r.read()) != -1) {
                if (c == 0xFEFF) continue;
                if (Character.isWhitespace(c)) continue;
                return c == '[';
            }
        } catch (Exception e) {
            // ignore
        }
        return false;
    }

    /**
     * 将 JSON 数组文件转换为 CSV 文件（含表头）
     * @param jsonPath  JSON 数组文件路径
     * @param delimiter CSV 分隔符
     * @param encoding  输入文件编码（默认 UTF-8）
     * @return 生成的 CSV 文件路径，失败返回 null
     */
    public String convertArrayToCsv(String jsonPath, String delimiter, String encoding) {
        java.io.File jsonFile = new java.io.File(jsonPath);
        if (!jsonFile.exists() || !jsonFile.isFile()) {
            log.warn("JSON file not found: {}", jsonPath);
            return null;
        }
        String csvPath = jsonPath.replaceAll("(?i)\\.json$", "") + "_converted.csv";
        java.io.File csvFile = new java.io.File(csvPath);
        Charset charset = parseCharset(encoding);

        try {
            byte[] raw = Files.readAllBytes(jsonFile.toPath());
            if (raw.length >= 3 && (raw[0] & 0xFF) == 0xEF && (raw[1] & 0xFF) == 0xBB && (raw[2] & 0xFF) == 0xBF) {
                raw = java.util.Arrays.copyOfRange(raw, 3, raw.length);
            }
            String text = new String(raw, charset);
            JsonNode root = objectMapper.readTree(text);
            if (root == null || !root.isArray()) {
                log.warn("Not a JSON array file: {}", jsonPath);
                return null;
            }

            LinkedHashMap<String, Integer> columnIndex = new LinkedHashMap<>();
            List<LinkedHashMap<String, String>> records = new ArrayList<>();
            for (JsonNode item : root) {
                LinkedHashMap<String, String> rec = new LinkedHashMap<>();
                if (item != null && item.isObject()) {
                    Iterator<Map.Entry<String, JsonNode>> it = item.fields();
                    while (it.hasNext()) {
                        Map.Entry<String, JsonNode> e = it.next();
                        if (!columnIndex.containsKey(e.getKey())) columnIndex.put(e.getKey(), columnIndex.size());
                        rec.put(e.getKey(), nodeToText(e.getValue()));
                    }
                }
                records.add(rec);
            }

            String[] columns = columnIndex.keySet().toArray(new String[0]);
            try (BufferedWriter writer = Files.newBufferedWriter(csvFile.toPath(), StandardCharsets.UTF_8)) {
                StringBuilder header = new StringBuilder();
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) header.append(delimiter);
                    header.append(escapeCsv(columns[i], delimiter));
                }
                writer.write(header.toString());
                writer.newLine();
                for (LinkedHashMap<String, String> rec : records) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < columns.length; i++) {
                        if (i > 0) sb.append(delimiter);
                        sb.append(escapeCsv(rec.getOrDefault(columns[i], ""), delimiter));
                    }
                    writer.write(sb.toString());
                    writer.newLine();
                }
            }
            log.info("JSON array converted to CSV: {} -> {} ({} rows)", jsonPath, csvPath, records.size());
            return csvPath;

        } catch (Exception e) {
            log.error("Failed to convert JSON array to CSV: {}", e.getMessage());
            try { Files.deleteIfExists(csvFile.toPath()); } catch (IOException ignored) {}
            return null;
        }
    }

    /** 值转文本：标量取 asText，对象/数组序列化为 JSON 字符串 */
    private String nodeToText(JsonNode node) {
        if (node == null || node.isNull()) return "";
        if (node.isValueNode()) return node.asText();
        try { return objectMapper.writeValueAsString(node); }
        catch (Exception e) { return node.toString(); }
    }

    private String escapeCsv(String value, String delimiter) {
        if (value.contains(delimiter) || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private Charset parseCharset(String encoding) {
        if (encoding == null || encoding.trim().isEmpty()) return StandardCharsets.UTF_8;
        try { return Charset.forName(encoding.trim()); }
        catch (Exception e) { return StandardCharsets.UTF_8; }
    }
}
