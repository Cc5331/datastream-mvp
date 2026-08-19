package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

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
 * XML 文件预处理服务
 * 将 XML 记录列表（<root><record>...</record>...</root>）转换为 Flink 可处理的临时 CSV。
 * 子元素作为列；嵌套对象/数组保留为 JSON 字符串列；忽略 XML 属性。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class XmlPreprocessor {

    private final ObjectMapper objectMapper;

    private static final XmlMapper XML_MAPPER = new XmlMapper();

    /**
     * 将 XML 文件转换为 CSV 文件（含表头）
     * @param xmlPath   XML 文件路径
     * @param rowTag    重复元素名（每行对应一个）；为空时自动探测出现最多的重复元素
     * @param delimiter CSV 分隔符
     * @param encoding  XML 文件编码（默认 UTF-8）
     * @return 生成的 CSV 文件路径，失败返回 null
     */
    public String convertToCsv(String xmlPath, String rowTag, String delimiter, String encoding) {
        java.io.File xmlFile = new java.io.File(xmlPath);
        if (!xmlFile.exists() || !xmlFile.isFile()) {
            log.warn("XML file not found: {}", xmlPath);
            return null;
        }
        String csvPath = xmlPath.replaceAll("(?i)\\.xml$", "") + "_converted.csv";
        java.io.File csvFile = new java.io.File(csvPath);
        Charset charset = parseCharset(encoding);

        try {
            byte[] raw = Files.readAllBytes(xmlFile.toPath());
            if (raw.length >= 3 && (raw[0] & 0xFF) == 0xEF && (raw[1] & 0xFF) == 0xBB && (raw[2] & 0xFF) == 0xBF) {
                raw = java.util.Arrays.copyOfRange(raw, 3, raw.length);
            }
            String xmlText = new String(raw, charset);
            JsonNode root = XML_MAPPER.readTree(xmlText);
            List<JsonNode> rows = findRows(root, rowTag);
            if (rows.isEmpty()) {
                log.warn("No rows found in XML file: {} (rowTag={})", xmlPath, rowTag);
                return null;
            }

            // 首行决定列顺序，后续行补充新列
            LinkedHashMap<String, Integer> columnIndex = new LinkedHashMap<>();
            List<LinkedHashMap<String, String>> records = new ArrayList<>();
            for (JsonNode row : rows) {
                LinkedHashMap<String, String> rec = new LinkedHashMap<>();
                if (row != null && row.isObject()) {
                    Iterator<Map.Entry<String, JsonNode>> it = row.fields();
                    while (it.hasNext()) {
                        Map.Entry<String, JsonNode> e = it.next();
                        String name = e.getKey();
                        if (name.startsWith("@")) continue; // 忽略 XML 属性
                        if (!columnIndex.containsKey(name)) columnIndex.put(name, columnIndex.size());
                        rec.put(name, nodeToText(e.getValue()));
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
            log.info("XML converted to CSV: {} -> {} ({} rows)", xmlPath, csvPath, records.size());
            return csvPath;

        } catch (Exception e) {
            log.error("Failed to convert XML to CSV: {}", e.getMessage());
            try { Files.deleteIfExists(csvFile.toPath()); } catch (IOException ignored) {}
            return null;
        }
    }

    /** 定位行节点列表：优先 rowTag，其次自动探测第一层数组，再退化为单行 */
    private List<JsonNode> findRows(JsonNode root, String rowTag) {
        List<JsonNode> rows = new ArrayList<>();
        if (root == null || !root.isObject()) return rows;
        String tag = rowTag != null ? rowTag.trim() : "";
        if (!tag.isEmpty()) {
            JsonNode t = root.get(tag);
            if (t != null) { collectNodes(t, rows); return rows; }
            // 兼容 <rows><row>...</row></rows> 包装：第一层唯一字段下再找 rowTag
            if (root.size() == 1) {
                JsonNode inner = root.elements().next();
                if (inner.isArray()) {
                    for (JsonNode c : inner) {
                        if (c.isObject() && c.has(tag)) collectNodes(c.get(tag), rows);
                    }
                    if (!rows.isEmpty()) return rows;
                }
            }
            return rows;
        }
        // 自动探测：第一层字段中元素最多的数组
        String best = null; int bestCount = -1;
        Iterator<Map.Entry<String, JsonNode>> it = root.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode v = e.getValue();
            if (v.isArray() && v.size() > bestCount) { best = e.getKey(); bestCount = v.size(); }
        }
        if (best != null && bestCount > 0) { collectNodes(root.get(best), rows); return rows; }
        // 单元素时 XmlMapper 给出对象而非数组：第一层唯一字段视为行集合
        if (root.size() == 1) {
            JsonNode only = root.elements().next();
            if (!only.isValueNode()) { collectNodes(only, rows); return rows; }
        }
        // 整棵树视为单行
        rows.add(root);
        return rows;
    }

    private void collectNodes(JsonNode node, List<JsonNode> out) {
        if (node.isArray()) { node.forEach(out::add); }
        else if (node.isObject()) { out.add(node); }
    }

    /** 值转文本：文本取 asText，对象/数组序列化为 JSON 字符串 */
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
