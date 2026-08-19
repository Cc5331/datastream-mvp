package com.datastream.mvp.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.Type;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Parquet 文件预处理服务：将 .parquet 读取为临时 CSV（含表头），供 Flink CSV 输入链路使用
 */
@Slf4j
@Service
public class ParquetPreprocessor {

    /**
     * 将 Parquet 文件转换为 CSV 文件
     * @param parquetPath Parquet 文件路径
     * @param delimiter CSV 分隔符
     * @return 生成的 CSV 文件路径，失败返回 null
     */
    public String convertToCsv(String parquetPath, String delimiter) {
        File parquetFile = new File(parquetPath);
        if (!parquetFile.exists() || !parquetFile.isFile()) {
            log.warn("Parquet file not found: {}", parquetPath);
            return null;
        }

        String csvPath = parquetPath.replaceAll("(?i)\\.parquet$", "") + "_converted.csv";
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(parquetPath))
                .withConf(new Configuration()).build();
             BufferedWriter writer = Files.newBufferedWriter(new File(csvPath).toPath(), StandardCharsets.UTF_8)) {

            List<Type> fields = new ArrayList<>();
            org.apache.parquet.schema.GroupType schema = null;
            Group group;
            int rows = 0;
            while ((group = reader.read()) != null) {
                if (schema == null) {
                    schema = group.getType();
                    fields.addAll(schema.getFields());
                    // 表头
                    StringBuilder header = new StringBuilder();
                    for (int i = 0; i < fields.size(); i++) {
                        if (i > 0) header.append(delimiter);
                        header.append(escapeCsv(fields.get(i).getName(), delimiter));
                    }
                    writer.write(header.toString());
                    writer.newLine();
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < fields.size(); i++) {
                    if (i > 0) sb.append(delimiter);
                    sb.append(getValueAsString(group, fields.get(i).getName()));
                }
                writer.write(sb.toString());
                writer.newLine();
                rows++;
            }
            if (schema == null) {
                log.warn("Parquet file has no rows, cannot infer schema: {}", parquetPath);
                Files.deleteIfExists(new File(csvPath).toPath());
                return null;
            }
            log.info("Parquet converted to CSV: {} -> {} ({} rows)", parquetPath, csvPath, rows);
            return csvPath;
        } catch (Exception e) {
            log.error("Failed to convert Parquet to CSV: {}", e.getMessage());
            try { Files.deleteIfExists(new File(csvPath).toPath()); } catch (IOException ignored) {}
            return null;
        }
    }

    private String getValueAsString(Group group, String field) {
        try {
            if (group.getFieldRepetitionCount(field) == 0) return "";
                        org.apache.parquet.schema.Type ft = group.getType().getType(field);
            if (ft.isPrimitive()) {
                switch (ft.asPrimitiveType().getPrimitiveTypeName()) {
                    case INT64: return String.valueOf(group.getLong(field, 0));
                    case INT32: return String.valueOf(group.getInteger(field, 0));
                    case DOUBLE: return String.valueOf(group.getDouble(field, 0));
                    case FLOAT: return String.valueOf(group.getFloat(field, 0));
                    case BOOLEAN: return String.valueOf(group.getBoolean(field, 0));
                    default:
                        org.apache.parquet.io.api.Binary b = group.getBinary(field, 0);
                        if (b != null) {
                            try { return b.toStringUsingUTF8(); }
                            catch (Exception e) { return b.toString(); }
                        }
                        return "";
                }
            }
            return group.getGroup(field, 0).toString();
        } catch (Exception e) {
            return "";
        }
    }

    private String escapeCsv(String value, String delimiter) {
        if (value == null) return "";
        if (value.contains(delimiter) || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
};