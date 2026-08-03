package com.datastream.mvp.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/**
 * Excel 输出转换服务
 * 将 Flink 生成的 CSV 输出文件转换为 .xlsx 格式
 */
@Slf4j
@Service
public class ExcelOutputConverter {

    /**
     * 将 CSV 文件转换为 .xlsx 文件
     * @param csvPath   CSV 文件路径 (Flink 实际写入的临时文件)
     * @param xlsxPath  目标 .xlsx 文件路径
     * @param delimiter CSV 分隔符
     * @param hasHeader CSV 是否包含表头行
     * @return 转换是否成功
     */
    public boolean convertCsvToExcel(String csvPath, String xlsxPath, String delimiter, boolean hasHeader) {
        File csvFile = new File(csvPath);
        if (!csvFile.exists() || !csvFile.isFile()) {
            File dir = new File(csvPath);
            if (dir.isDirectory()) {
                File[] partFiles = dir.listFiles((d, name) -> name.startsWith("part-"));
                if (partFiles != null && partFiles.length > 0) {
                    csvFile = partFiles[0];
                    log.info("Using part file for CSV->Excel conversion: {}", csvFile.getAbsolutePath());
                } else {
                    log.warn("No CSV file or part files found at: {}", csvPath);
                    return false;
                }
            } else {
                log.warn("CSV file not found at: {}", csvPath);
                return false;
            }
        }

        File xlsxFile = new File(xlsxPath);
        File parentDir = xlsxFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        try (Workbook workbook = new XSSFWorkbook();
             BufferedReader reader = Files.newBufferedReader(csvFile.toPath(), StandardCharsets.UTF_8)) {
            Sheet sheet = workbook.createSheet("Data");
            String line;
            int rowNum = 0;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                Row row = sheet.createRow(rowNum++);
                String[] fields = splitCsvLine(line, delimiter);
                for (int i = 0; i < fields.length; i++) {
                    Cell cell = row.createCell(i);
                    cell.setCellValue(fields[i]);
                }
            }

            if (rowNum > 0) {
                Row firstRow = sheet.getRow(0);
                if (firstRow != null) {
                    for (int i = 0; i < firstRow.getLastCellNum(); i++) {
                        sheet.autoSizeColumn(i);
                    }
                }
            }

            try (FileOutputStream fos = new FileOutputStream(xlsxFile)) {
                workbook.write(fos);
            }

            log.info("CSV->Excel conversion successful: {} -> {} ({} rows)", csvPath, xlsxPath, rowNum);
            return true;

        } catch (Exception e) {
            log.error("Failed to convert CSV to Excel: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 生成 Excel 输出对应的临时 CSV 路径
     */
    public static String getTempCsvPath(String excelPath) {
        if (excelPath == null) return null;
        if (excelPath.toLowerCase().endsWith(".xlsx")) {
            return excelPath.substring(0, excelPath.length() - 5) + "_temp_csv";
        }
        return excelPath + "_temp_csv";
    }

    /**
     * 解析 CSV 行，支持引号转义
     */
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
}
