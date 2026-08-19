package com.datastream.mvp.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Parquet 输出转换服务：将 Flink 生成的临时 CSV 转换为 .parquet 文件
 * 列类型基于数据轻量推断（INT64 / DOUBLE / BOOLEAN / STRING），避免手工建 schema
 */
@Slf4j
@Service
public class ParquetOutputConverter {

    /**
     * 将 CSV 文件转换为 .parquet 文件
     * @param csvPath CSV 文件路径（含表头，Flink 实际写入的临时文件或合并后的文件）
     * @param parquetPath 目标 .parquet 文件路径
     * @param delimiter CSV 分隔符
     * @return 转换是否成功
     */
    public boolean convertCsvToParquet(String csvPath, String parquetPath, String delimiter) {
        File csvFile = resolveCsvFile(csvPath);
        if (csvFile == null) {
            log.warn("CSV file not found at: {}", csvPath);
            return false;
        }
        File pqFile = new File(parquetPath);
        File parentDir = pqFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) parentDir.mkdirs();

        try (BufferedReader reader = Files.newBufferedReader(csvFile.toPath(), StandardCharsets.UTF_8)) {
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                log.warn("CSV is empty (no header): {}", csvPath);
                return false;
            }
            String[] headers = splitCsvLine(headerLine, delimiter);
            if (headers.length == 0) return false;

            // 读取所有行用于类型推断
            List<String[]> rows = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                rows.add(splitCsvLine(line, delimiter));
            }

            // 推断列类型
            org.apache.parquet.schema.Types.MessageTypeBuilder builder = Types.buildMessage();
            for (int i = 0; i < headers.length; i++) {
                String col = headers[i].trim().isEmpty() ? ("col" + i) : headers[i].trim();
                builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, inferType(rows, i), col));
            }
            MessageType schema = builder.named("record");

            Configuration conf = new Configuration();
            GroupWriteSupport.setSchema(schema, conf);
            Path out = new Path(parquetPath);
            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(out)
                    .withType(schema)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(128 * 1024 * 1024)
                    .withPageSize(1 * 1024 * 1024)
                    .withDictionaryEncoding(true)
                    .build()) {
                SimpleGroupFactory factory = new SimpleGroupFactory(schema);
                for (String[] row : rows) {
                    Group g = factory.newGroup();
                    for (int i = 0; i < headers.length; i++) {
                        String v = i < row.length ? row[i] : "";
                        appendValue(g, headers.length, i, v, schema.getType(i).asPrimitiveType().getPrimitiveTypeName());
                    }
                    writer.write(g);
                }
            }
            log.info("CSV->Parquet conversion successful: {} -> {} ({} columns, {} rows)", csvPath, parquetPath, headers.length, rows.size());
            return true;
        } catch (Exception e) {
            log.error("Failed to convert CSV to Parquet: {}", e.getMessage());
            try { Files.deleteIfExists(pqFile.toPath()); } catch (IOException ignored) {}
            return false;
        }
    }

    private void appendValue(Group g, int colCount, int idx, String v, PrimitiveType.PrimitiveTypeName t) {
        if (v == null || v.isEmpty()) return;
        String fieldName = g.getType().getFields().get(idx).getName();
        try {
            switch (t) {
                case INT64: g.add(fieldName, Long.parseLong(v.trim())); break;
                case DOUBLE: g.add(fieldName, Double.parseDouble(v.trim())); break;
                case BOOLEAN: g.add(fieldName, Boolean.parseBoolean(v.trim())); break;
                default: g.add(fieldName, v); break;
            }
        } catch (Exception e) {
            g.add(fieldName, v);
        }
    }

    private PrimitiveType.PrimitiveTypeName inferType(List<String[]> rows, int colIdx) {
        boolean allInt = true, allDouble = true, allBool = true, seen = false;
        for (String[] row : rows) {
            if (colIdx >= row.length) continue;
            String v = row[colIdx].trim();
            if (v.isEmpty()) continue;
            seen = true;
            if (!isLong(v)) allInt = false;
            if (!isDouble(v)) allDouble = false;
            if (!("true".equalsIgnoreCase(v) || "false".equalsIgnoreCase(v))) allBool = false;
        }
        if (!seen) return PrimitiveType.PrimitiveTypeName.BINARY;
        if (allInt) return PrimitiveType.PrimitiveTypeName.INT64;
        if (allDouble) return PrimitiveType.PrimitiveTypeName.DOUBLE;
        if (allBool) return PrimitiveType.PrimitiveTypeName.BOOLEAN;
        return PrimitiveType.PrimitiveTypeName.BINARY;
    }

    private boolean isLong(String s) { try { Long.parseLong(s); return true; } catch (Exception e) { return false; } }
    private boolean isDouble(String s) { try { Double.parseDouble(s); return true; } catch (Exception e) { return false; } }

    private File resolveCsvFile(String csvPath) {
        File f = new File(csvPath);
        if (f.exists() && f.isFile()) return f;
        if (f.isDirectory()) {
            File[] parts = f.listFiles((d, n) -> n.startsWith("part-"));
            if (parts != null && parts.length > 0) return parts[0];
        }
        return null;
    }

    /**
     * 解析 CSV 行，支持引号转义
     */
    private String[] splitCsvLine(String line, String delimiter) {
        List<String> result = new ArrayList<>();
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
};