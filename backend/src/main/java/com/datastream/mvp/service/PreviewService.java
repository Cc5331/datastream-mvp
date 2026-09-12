package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * 通用数据预览服务：按文件类型读取前 N 行，返回结构化表格数据（列名 + 行）。
 * 支持 CSV / Excel / JSON / XML / 普通文本；用于前端画布节点“预览数据”。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PreviewService {

    private final ObjectMapper objectMapper;
    private final XmlPreprocessor xmlPreprocessor;

    @Value("${app.preview.allowed-roots:../data,../test-resources,../output}")
    private String allowedRoots;

    @Value("${app.preview.max-file-bytes:67108864}")
    private long maxFileBytes;

    public Map<String, Object> previewFile(String path, int limit) {
        Map<String, Object> result = new LinkedHashMap<>();
        java.io.File f = resolveAllowedFile(path).toFile();
        result.put("path", f.getAbsolutePath());
        if (!f.exists() || !f.isFile()) {
            result.put("message", "文件不存在: " + path);
            return result;
        }
        if (f.length() > maxFileBytes) {
            throw new IllegalArgumentException("文件过大，无法预览（最大 " + maxFileBytes + " 字节）");
        }
        int n = (limit <= 0) ? 20 : Math.min(limit, 100);
        String lower = f.getName().toLowerCase();
        try {
            if (lower.endsWith(".csv") || lower.endsWith(".txt")) return previewCsv(f, n);
            if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) return previewExcel(f, n);
            if (lower.endsWith(".json")) return previewJson(f, n);
            if (lower.endsWith(".xml")) return previewXml(f, n);
            return previewText(f, n);
        } catch (Exception e) {
            log.warn("Preview failed for {}: {}", path, e.getMessage());
            result.put("message", "预览失败: " + e.getMessage());
        }
        return result;
    }

    private Map<String, Object> previewCsv(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.parse(reader)) {
            Iterator<CSVRecord> it = parser.iterator();
            boolean first = true;
            int rowCount = 0;
            while (it.hasNext() && rowCount < n) {
                CSVRecord rec = it.next();
                if (rec.size() == 0 && rec.get(0).isEmpty()) continue;
                if (first) {
                    for (int i = 0; i < rec.size(); i++) columns.add(rec.get(i).isEmpty() ? "col" + (i + 1) : rec.get(i));
                    first = false;
                    continue;
                }
                List<String> row = new ArrayList<>();
                for (int i = 0; i < rec.size(); i++) row.add(rec.get(i));
                rows.add(row);
                rowCount++;
            }
            if (first) { // 空文件或只有空行
                columns.add("col1");
            }
            if (it.hasNext()) result.put("truncated", true);
        } catch (Exception e) {
            log.warn("CSV preview fallback to text: {}", e.getMessage());
            return previewText(f, n);
        }
        result.put("columns", columns);
        result.put("rows", rows);
        return result;
    }

    private Map<String, Object> previewExcel(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        DataFormatter fmt = new DataFormatter();
        try (Workbook wb = WorkbookFactory.create(f)) {
            Sheet sheet = wb.getSheetAt(0);
            if (sheet == null) {
                result.put("message", "Excel 文件没有工作表");
                return result;
            }
            int first = sheet.getFirstRowNum();
            int last = Math.min(first + n + 1, sheet.getLastRowNum() + 1);
            boolean header = true;
            for (int r = first; r < last; r++) {
                Row row = sheet.getRow(r);
                int cols = (row == null) ? 0 : row.getLastCellNum();
                List<String> vals = new ArrayList<>();
                for (int c = 0; c < cols; c++) {
                    Cell cell = row.getCell(c);
                    vals.add(cell == null ? "" : fmt.formatCellValue(cell));
                }
                if (header) {
                    for (int i = 0; i < vals.size(); i++) columns.add(vals.get(i).isEmpty() ? "col" + (i + 1) : vals.get(i));
                    header = false;
                } else {
                    rows.add(vals);
                }
            }
            if (columns.isEmpty()) columns.add("col1");
            if (sheet.getLastRowNum() + 1 > last) result.put("truncated", true);
        } catch (Exception e) {
            log.warn("Excel preview failed: {}", e.getMessage());
            result.put("message", "Excel 预览失败: " + e.getMessage());
            return result;
        }
        result.put("columns", columns);
        result.put("rows", rows);
        return result;
    }

    private Map<String, Object> previewJson(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        List<JsonNode> items = new ArrayList<>();
        try {
            String text = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
            if (text.startsWith("﻿")) text = text.substring(1);
            JsonNode root = objectMapper.readTree(text);
            if (root.isArray()) {
                root.forEach(items::add);
            } else if (root.isObject()) {
                items.add(root);
            }
            if (items.isEmpty()) {
                // JSON Lines
                for (String line : text.split("\\n")) {
                    String t = line.trim();
                    if (t.isEmpty()) continue;
                    try { items.add(objectMapper.readTree(t)); } catch (Exception ignore) {}
                }
            }
        } catch (Exception e) {
            log.warn("JSON preview failed: {}", e.getMessage());
            result.put("message", "JSON 预览失败: " + e.getMessage());
            return result;
        }
        LinkedHashSet<String> colSet = new LinkedHashSet<>();
        for (JsonNode it : items) {
            if (it != null && it.isObject()) it.fieldNames().forEachRemaining(colSet::add);
        }
        if (colSet.isEmpty()) colSet.add("value");
        List<String> columns = new ArrayList<>(colSet);
        List<List<String>> rows = new ArrayList<>();
        int rowCount = 0;
        for (JsonNode it : items) {
            if (rowCount >= n) { result.put("truncated", true); break; }
            if (it == null || !it.isObject()) continue;
            List<String> row = new ArrayList<>();
            for (String c : columns) {
                JsonNode v = it.get(c);
                row.add((v == null || v.isNull()) ? "" : (v.isValueNode() ? v.asText() : v.toString()));
            }
            rows.add(row);
            rowCount++;
        }
        result.put("columns", columns);
        result.put("rows", rows);
        return result;
    }

    private Map<String, Object> previewXml(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        try {
            String csvPath = xmlPreprocessor.convertToCsv(f.getAbsolutePath(), "", ",", "UTF-8");
            if (csvPath == null) {
                result.put("message", "XML 无法解析为记录列表结构");
                return result;
            }
            Map<String, Object> csv = previewCsv(new java.io.File(csvPath), n);
            csv.put("path", f.getAbsolutePath());
            csv.put("note", "XML 已按记录列表解析（嵌套子结构保留为 JSON 列）");
            return csv;
        } catch (Exception e) {
            log.warn("XML preview failed: {}", e.getMessage());
            result.put("message", "XML 预览失败: " + e.getMessage());
            return result;
        }
    }

    private Map<String, Object> previewText(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        List<String> rows = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while (rows.size() < n && (line = reader.readLine()) != null) {
                rows.add(line);
            }
            if (reader.readLine() != null) result.put("truncated", true);
        } catch (Exception e) {
            result.put("message", "读取失败: " + e.getMessage());
            return result;
        }
        List<String> columns = new ArrayList<>();
        columns.add("content");
        List<List<String>> table = new ArrayList<>();
        for (String r : rows) { List<String> row = new ArrayList<>(); row.add(r); table.add(row); }
        result.put("columns", columns);
        result.put("rows", table);
        return result;
    }

    private Path resolveAllowedFile(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            throw new IllegalArgumentException("文件路径不能为空");
        }
        Path candidate = Path.of(rawPath.trim()).toAbsolutePath().normalize();
        try {
            Path realCandidate = candidate.toRealPath();
            boolean allowed = Arrays.stream(allowedRoots.split(","))
                    .map(String::trim)
                    .filter(root -> !root.isEmpty())
                    .map(root -> Path.of(root).toAbsolutePath().normalize())
                    .filter(Files::exists)
                    .map(root -> {
                        try {
                            return root.toRealPath();
                        } catch (java.io.IOException e) {
                            return null;
                        }
                    })
                    .filter(java.util.Objects::nonNull)
                    .anyMatch(realCandidate::startsWith);
            if (!allowed) {
                throw new IllegalArgumentException("文件路径不在允许的预览目录中");
            }
            return realCandidate;
        } catch (java.nio.file.NoSuchFileException e) {
            return candidate;
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("无法解析文件路径", e);
        }
    }

    private Map<String, Object> baseResult(java.io.File f) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("path", f.getAbsolutePath());
        return r;
    }
}
