package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Excel 文件预处理服务
 * 将 .xlsx 文件转换为 Flink 可处理的 CSV 格式
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExcelPreprocessor {

    private final ObjectMapper objectMapper;

    /**
     * 将 Excel 文件转换为 CSV 文件
     * @param xlsxPath Excel 文件路径
     * @param delimiter CSV 分隔符
     * @param hasHeader 是否包含表头
     * @return 生成的 CSV 文件路径，如果失败返回 null
     */
    public String convertToCsv(String xlsxPath, String delimiter, boolean hasHeader) {
        return convertToCsv(xlsxPath, delimiter, hasHeader, null);
    }

    /**
     * 将 Excel 文件转换为 CSV 文件（支持指定 sheet）
     * @param xlsxPath Excel 文件路径
     * @param delimiter CSV 分隔符
     * @param hasHeader 是否包含表头
     * @param sheetName 指定工作表名（空则取第一个 sheet）
     * @return 生成的 CSV 文件路径，如果失败返回 null
     */
    public String convertToCsv(String xlsxPath, String delimiter, boolean hasHeader, String sheetName) {
        File xlsxFile = new File(xlsxPath);
        if (!xlsxFile.exists() || !xlsxFile.isFile()) {
            log.warn("Excel file not found: {}", xlsxPath);
            return null;
        }

        String csvPath = xlsxPath.replaceAll("(?i)\\.(xlsx|xls)$", "") + "_converted.csv";
        File csvFile = new File(csvPath);

        try (Workbook workbook = WorkbookFactory.create(xlsxFile, null, true)) {
            Sheet sheet = resolveSheet(workbook, sheetName);
            if (sheet == null) {
                log.warn("No sheets found in Excel file (sheetName={}): {}", sheetName, xlsxPath);
                return null;
            }

            try (BufferedWriter writer = Files.newBufferedWriter(csvFile.toPath(), java.nio.charset.StandardCharsets.UTF_8)) {
                for (Row row : sheet) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < row.getLastCellNum(); i++) {
                        if (i > 0) sb.append(delimiter);
                        Cell cell = row.getCell(i);
                        if (cell != null) {
                            String cellValue = getCellValueAsString(cell);
                            // Escape CSV fields containing delimiter, quotes, or newlines
                            if (cellValue.contains(delimiter) || cellValue.contains("\"") || cellValue.contains("\\n")) {
                                cellValue = "\"" + cellValue.replace("\"", "\"\"") + "\"";
                            }
                            sb.append(cellValue);
                        }
                    }
                    writer.write(sb.toString());
                    writer.newLine();
                }
            }

            log.info("Excel converted to CSV: {} -> {} ({} rows)", xlsxPath, csvPath, sheet.getPhysicalNumberOfRows());
            return csvPath;

        } catch (Exception e) {
            log.error("Failed to convert Excel to CSV: {}", e.getMessage());
            // Clean up partial output
            try { Files.deleteIfExists(csvFile.toPath()); } catch (IOException ignored) {}
            return null;
        }
    }

    /**
     * 从 Excel 文件中提取字段配置（从第一行表头推断）
     */
    public String extractFieldsConfig(String xlsxPath) {
        File xlsxFile = new File(xlsxPath);
        if (!xlsxFile.exists()) return null;

        try (Workbook workbook = WorkbookFactory.create(xlsxFile, null, true)) {
            Sheet sheet = workbook.getSheetAt(0);
            if (sheet == null) return null;

            Row headerRow = sheet.getRow(0);
            if (headerRow == null) return null;

            List<Map<String, Object>> fields = new ArrayList<>();
            for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                Cell cell = headerRow.getCell(i);
                String fieldName = cell != null ? getCellValueAsString(cell) : "field" + i;
                if (fieldName.trim().isEmpty()) fieldName = "field" + i;

                // Infer type from first data row
                String fieldType = "STRING";
                Row dataRow = sheet.getRow(1);
                if (dataRow != null) {
                    Cell dataCell = dataRow.getCell(i);
                    if (dataCell != null) {
                        switch (dataCell.getCellType()) {
                            case NUMERIC:
                                if (DateUtil.isCellDateFormatted(dataCell)) {
                                    fieldType = "TIMESTAMP(3)";
                                } else {
                                    double val = dataCell.getNumericCellValue();
                                    fieldType = (val == Math.floor(val) && !Double.isInfinite(val)) ? "INT" : "DOUBLE";
                                }
                                break;
                            case BOOLEAN:
                                fieldType = "BOOLEAN";
                                break;
                            case STRING:
                            default:
                                fieldType = "STRING";
                                break;
                        }
                    }
                }

                Map<String, Object> field = new LinkedHashMap<>();
                field.put("name", fieldName);
                field.put("type", fieldType);
                fields.add(field);
            }

            return objectMapper.writeValueAsString(fields);

        } catch (Exception e) {
            log.warn("Failed to extract fields from Excel: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 按名称或索引解析 sheet，空 sheetName 时取第一个
     */
    private Sheet resolveSheet(Workbook workbook, String sheetName) {
        if (sheetName != null && !sheetName.trim().isEmpty()) {
            String name = sheetName.trim();
            Sheet byName = workbook.getSheet(name);
            if (byName != null) return byName;
            // 兼容传入索引字符串
            try {
                int idx = Integer.parseInt(name);
                if (idx >= 0 && idx < workbook.getNumberOfSheets()) return workbook.getSheetAt(idx);
            } catch (NumberFormatException ignored) {}
            log.warn("Sheet not found: {} (available: {})", name, String.join(",", getSheetNames(workbook)));
            return null;
        }
        return workbook.getNumberOfSheets() > 0 ? workbook.getSheetAt(0) : null;
    }

    private String[] getSheetNames(Workbook workbook) {
        String[] names = new String[workbook.getNumberOfSheets()];
        for (int i = 0; i < names.length; i++) names[i] = workbook.getSheetName(i);
        return names;
    }

    private String getCellValueAsString(Cell cell) {
        if (cell == null) return "";
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getLocalDateTimeCellValue().toString();
                }
                double val = cell.getNumericCellValue();
                if (val == Math.floor(val) && !Double.isInfinite(val)) {
                    return String.valueOf((long) val);
                }
                return String.valueOf(val);
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    return String.valueOf(cell.getNumericCellValue());
                } catch (Exception e) {
                    try {
                        return cell.getStringCellValue();
                    } catch (Exception e2) {
                        return cell.getCellFormula();
                    }
                }
            case BLANK:
                return "";
            default:
                return "";
        }
    }
}
