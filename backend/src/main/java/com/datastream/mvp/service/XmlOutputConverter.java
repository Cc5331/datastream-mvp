package com.datastream.mvp.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * XML 输出转换服务
 * 将 Flink 生成的 CSV 输出文件转换为 XML 文件（rootTag/rowTag 包裹，字段值 XML 转义）。
 */
@Slf4j
@Service
public class XmlOutputConverter {

    /**
     * 将 CSV 文件转换为 XML 文件
     * @param csvPath   CSV 文件路径（Flink 实际写入的临时文件/目录）
     * @param xmlPath   目标 .xml 文件路径
     * @param delimiter CSV 分隔符
     * @param rootTag   XML 根元素名（默认 root）
     * @param rowTag    XML 行元素名（默认 record）
     * @param encoding  输出文件编码（默认 UTF-8）
     * @return 转换是否成功
     */
    public boolean convertCsvToXml(String csvPath, String xmlPath, String delimiter,
                                   String rootTag, String rowTag, String encoding) {
        java.io.File csvFile = new java.io.File(csvPath);
        if (!csvFile.exists() || !csvFile.isFile()) {
            java.io.File dir = new java.io.File(csvPath);
            if (dir.isDirectory()) {
                java.io.File[] partFiles = dir.listFiles((d, name) -> name.startsWith("part-"));
                if (partFiles != null && partFiles.length > 0) {
                    csvFile = partFiles[0];
                    log.info("Using part file for CSV->XML conversion: {}", csvFile.getAbsolutePath());
                } else {
                    log.warn("No CSV file or part files found at: {}", csvPath);
                    return false;
                }
            } else {
                log.warn("CSV file not found at: {}", csvPath);
                return false;
            }
        }

        java.io.File xmlFile = new java.io.File(xmlPath);
        java.io.File parentDir = xmlFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) parentDir.mkdirs();
        String rt = (rootTag == null || rootTag.trim().isEmpty()) ? "root" : rootTag.trim();
        String wt = (rowTag == null || rowTag.trim().isEmpty()) ? "record" : rowTag.trim();
        Charset cs = parseCharset(encoding);

        try (BufferedReader reader = Files.newBufferedReader(csvFile.toPath(), StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(xmlFile.toPath(), cs)) {
            String line;
            boolean first = true;
            String[] headers = null;
            writer.write("<" + rt + ">\n");
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] fields = splitCsvLine(line, delimiter);
                if (first) { headers = fields; first = false; continue; } // 表头行
                writer.write("  <" + wt + ">\n");
                for (int i = 0; i < fields.length; i++) {
                    String name = headers != null && i < headers.length && !headers[i].trim().isEmpty()
                            ? headers[i].trim() : "field" + i;
                    writer.write("    <" + name + ">" + escapeXml(fields[i]) + "</" + name + ">\n");
                }
                writer.write("  </" + wt + ">\n");
            }
            writer.write("</" + rt + ">\n");
            log.info("CSV->XML conversion successful: {} -> {} (root={}, row={})", csvPath, xmlPath, rt, wt);
            return true;
        } catch (Exception e) {
            log.error("Failed to convert CSV to XML: {}", e.getMessage());
            return false;
        }
    }

    /** 生成 XML 输出对应的临时 CSV 路径 */
    public static String getTempCsvPath(String xmlPath) {
        if (xmlPath == null) return null;
        if (xmlPath.toLowerCase().endsWith(".xml")) {
            return xmlPath.substring(0, xmlPath.length() - 4) + "_temp_csv";
        }
        return xmlPath + "_temp_csv";
    }

    private String escapeXml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&apos;");
    }

    /** 解析 CSV 行，支持引号转义 */
    private String[] splitCsvLine(String line, String delimiter) {
        java.util.List<String> result = new java.util.ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;
        char delim = delimiter.charAt(0);
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    currentField.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == delim && !inQuotes) {
                result.add(currentField.toString());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }
        result.add(currentField.toString());
        return result.toArray(new String[0]);
    }

    private Charset parseCharset(String encoding) {
        if (encoding == null || encoding.trim().isEmpty()) return StandardCharsets.UTF_8;
        try { return Charset.forName(encoding.trim()); }
        catch (Exception e) { return StandardCharsets.UTF_8; }
    }
}
