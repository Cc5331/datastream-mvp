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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.Type;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * 画布「拖拽文件导入」服务：把浏览器拖入的文件暂存到 output/uploads，并自动分析成可直接使用的输入控件参数。
 *
 * <p>为什么必须落盘：浏览器拖拽只提供 File 对象（安全限制下拿不到本机绝对路径），
 * 而画布节点的 params.path 必须是**服务器可读路径**，所以文件先上传、再回填路径。
 * 暂存目录默认 {@code ../output/uploads}，正好落在 {@code app.preview.allowed-roots} 内，
 * 生成节点后可以直接复用 {@code /api/preview/file} 预览。</p>
 *
 * <p>分析结果（controlType / delimiter / hasHeader / sheetNames / mode / columns / rows）
 * 只作为「建议值」返回给前端确认对话框，落到节点上的参数以用户最终确认为准。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FileImportService {

    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS");
    private static final List<String> CSV_EXTS = List.of("csv", "tsv", "txt");
    private static final List<String> EXCEL_EXTS = List.of("xlsx", "xls");
    private static final List<String> JSON_EXTS = List.of("json", "jsonl", "ndjson");
    private static final List<String> XML_EXTS = List.of("xml");
    private static final List<String> PARQUET_EXTS = List.of("parquet");
    private static final int HARD_PREVIEW_LIMIT = 50;

    private final ObjectMapper objectMapper;
    private final XmlPreprocessor xmlPreprocessor;

    @Value("${app.storage.output-root:../output}")
    private String outputRoot;

    @Value("${app.files.upload-subdir:uploads}")
    private String uploadSubdir;

    @Value("${app.files.max-upload-bytes:52428800}")
    private long maxUploadBytes;

    @Value("${app.files.preview-rows:20}")
    private int previewRows;

    /** 暂存目录（绝对路径，规范化）。 */
    public Path uploadRoot() {
        return Path.of(outputRoot, uploadSubdir).toAbsolutePath().normalize();
    }

    /**
     * 暂存上传文件并分析，返回给前端确认对话框使用的建议参数。
     *
     * @param file 浏览器拖入的文件
     * @return path / fileName / controlType / delimiter / hasHeader / columns / rows / warnings 等
     */
    public Map<String, Object> analyze(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw badRequest("没有收到文件内容，请重新拖拽文件");
        }
        if (file.getSize() > maxUploadBytes) {
            throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE,
                    "文件过大（" + file.getSize() + " 字节），上限 " + maxUploadBytes + " 字节");
        }

        String fileName = sanitizeFileName(file.getOriginalFilename());
        String extension = extensionOf(fileName);
        String controlType = controlTypeForExtension(extension);

        Path root = uploadRoot();
        Path target;
        try {
            Files.createDirectories(root);
            String storedName = STAMP.format(LocalDateTime.now()) + "_"
                    + UUID.randomUUID().toString().substring(0, 8) + "_" + fileName;
            target = root.resolve(storedName).normalize();
            if (!target.startsWith(root)) throw badRequest("文件名不合法，无法保存");
            try (InputStream input = file.getInputStream()) {
                Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (ResponseStatusException e) {
            throw e;
        } catch (IOException e) {
            log.warn("Save uploaded file failed: {}", e.getMessage());
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "保存上传文件失败: " + e.getMessage());
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("path", target.toString());
        result.put("fileName", fileName);
        result.put("extension", extension);
        result.put("controlType", controlType);
        result.put("sizeBytes", safeSize(target));
        List<String> warnings = new ArrayList<>();

        try {
            switch (controlType) {
                case "csv_input" -> analyzeDelimited(target, extension, result, warnings);
                case "excel_input" -> analyzeExcel(target, result, warnings);
                case "json_input" -> analyzeJson(target, result, warnings);
                case "xml_input" -> analyzeXml(target, result, warnings);
                case "parquet_input" -> analyzeParquet(target, result, warnings);
                default -> throw badRequest("暂不支持的控件类型: " + controlType);
            }
        } catch (ResponseStatusException e) {
            deleteQuietly(target);
            throw e;
        } catch (Exception e) {
            deleteQuietly(target);
            log.warn("Analyze uploaded file failed ({}): {}", target, e.getMessage());
            throw badRequest("文件解析失败: " + e.getMessage());
        }

        result.put("warnings", warnings);
        log.info("导入文件已分析: {} -> {} (controlType={}, columns={})",
                fileName, target, controlType,
                result.get("columns") instanceof List<?> list ? list.size() : 0);
        return result;
    }

    /**
     * 丢弃暂存的导入文件（用户在确认框点了取消）。
     * 只允许删除 uploads 根目录内的文件，避免变成任意文件删除接口。
     */
    public Map<String, Object> discard(String path) {
        if (path == null || path.isBlank()) throw badRequest("文件路径不能为空");
        Path root = uploadRoot();
        Path target = Path.of(path.trim()).toAbsolutePath().normalize();
        if (!target.startsWith(root) || target.equals(root)) {
            throw badRequest("只能丢弃本次导入的暂存文件");
        }
        boolean deleted = deleteQuietly(target);
        // 预处理可能产生的旁路文件（.nohdr / _converted.csv）一并清理
        deleteQuietly(Path.of(target + ".nohdr"));
        deleteQuietly(Path.of(target.toString().replaceAll("(?i)\\.xml$", "") + "_converted.csv"));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deleted", deleted);
        result.put("path", target.toString());
        return result;
    }

    // ===================== 各格式分析 =====================

    private void analyzeDelimited(Path file, String extension, Map<String, Object> result, List<String> warnings)
            throws IOException {
        Charset charset = detectCharset(file);
        String delimiter = "tsv".equals(extension) ? "\t" : detectDelimiter(file, charset);
        List<String> lines = readSampleLines(file, charset, previewRows + 5);
        if (lines.isEmpty()) throw badRequest("文件内容为空，无法分析列结构");

        List<CSVRecord> records = parseCsv(lines, delimiter);
        if (records.isEmpty()) throw badRequest("文件没有可解析的数据行");

        CSVRecord first = records.get(0);
        boolean hasHeader = looksLikeHeader(first);
        List<String> names = new ArrayList<>();
        if (hasHeader) {
            LinkedHashSet<String> used = new LinkedHashSet<>();
            for (int i = 0; i < first.size(); i++) {
                String name = first.get(i) == null ? "" : first.get(i).trim();
                if (name.isEmpty()) name = "col" + (i + 1);
                String candidate = name;
                int k = 2;
                while (used.contains(candidate)) {
                    candidate = name + "_" + k;
                    k++;
                }
                used.add(candidate);
                names.add(candidate);
            }
        } else {
            for (int i = 1; i <= first.size(); i++) names.add("col" + i);
        }
        if (names.isEmpty()) throw badRequest("未能识别出任何列");

        int columnCount = names.size();
        List<List<String>> rows = new ArrayList<>();
        boolean ragged = false;
        boolean truncated = false;
        int start = hasHeader ? 1 : 0;
        for (int i = start; i < records.size(); i++) {
            CSVRecord record = records.get(i);
            if (rows.size() >= previewRows) {
                truncated = true;
                break;
            }
            if (record.size() != columnCount) ragged = true;
            rows.add(normalizeRow(record, columnCount));
        }
        if (truncated || hasMoreLines(file, charset, rows.size() + (hasHeader ? 1 : 0))) truncated = true;
        if (ragged) warnings.add("部分数据行的列数与表头不一致，已按表头列数补齐/截断");
        if (!StandardCharsets.UTF_8.equals(charset)) {
            warnings.add("检测到非 UTF-8 编码（" + charset.name() + "）：平台按 UTF-8 读取 CSV，建议先转码，否则中文可能乱码");
        }

        result.put("delimiter", delimiter);
        result.put("hasHeader", hasHeader);
        result.put("encoding", charset.name());
        result.put("columns", buildColumns(names, rows, warnings));
        result.put("rows", rows);
        result.put("truncated", truncated);
    }

    private void analyzeExcel(Path file, Map<String, Object> result, List<String> warnings) throws IOException {
        DataFormatter formatter = new DataFormatter();
        try (Workbook workbook = WorkbookFactory.create(file.toFile())) {
            List<String> sheetNames = new ArrayList<>();
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) sheetNames.add(workbook.getSheetName(i));
            if (sheetNames.isEmpty()) throw badRequest("Excel 文件没有工作表");
            Sheet sheet = workbook.getSheetAt(0);
            int firstRow = sheet.getFirstRowNum();
            int lastRow = sheet.getLastRowNum();
            if (lastRow < firstRow) throw badRequest("Excel 工作表为空，无法分析列结构");

            Row headerRow = sheet.getRow(firstRow);
            int columnCount = headerRow == null ? 0 : headerRow.getLastCellNum();
            if (columnCount <= 0) throw badRequest("Excel 首行没有可用列");

            LinkedHashSet<String> used = new LinkedHashSet<>();
            List<String> names = new ArrayList<>();
            for (int c = 0; c < columnCount; c++) {
                String name = cellText(headerRow.getCell(c), formatter);
                if (name.isEmpty()) name = "col" + (c + 1);
                String candidate = name;
                int k = 2;
                while (used.contains(candidate)) {
                    candidate = name + "_" + k;
                    k++;
                }
                used.add(candidate);
                names.add(candidate);
            }

            List<List<String>> rows = new ArrayList<>();
            for (int r = firstRow + 1; r <= lastRow && rows.size() < previewRows; r++) {
                Row row = sheet.getRow(r);
                List<String> values = new ArrayList<>();
                for (int c = 0; c < columnCount; c++) {
                    values.add(cellText(row == null ? null : row.getCell(c), formatter));
                }
                rows.add(values);
            }
            boolean truncated = lastRow > firstRow + rows.size();
            if (truncated) warnings.add("仅分析前 " + rows.size() + " 行，后续行按同结构读取");

            result.put("delimiter", ",");
            result.put("hasHeader", true);
            result.put("sheetNames", sheetNames);
            result.put("sheetName", sheetNames.get(0));
            result.put("columns", buildColumns(names, rows, warnings));
            result.put("rows", rows);
            result.put("truncated", truncated);
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            throw badRequest("Excel 解析失败（请确认为 .xlsx/.xls 文件）: " + e.getMessage());
        }
    }

    private void analyzeJson(Path file, Map<String, Object> result, List<String> warnings) throws IOException {
        Charset charset = detectCharset(file);
        String text = readAll(file, charset);
        if (text.startsWith("\uFEFF")) text = text.substring(1);
        String trimmed = text.trim();
        if (trimmed.isEmpty()) throw badRequest("JSON 文件内容为空");

        boolean arrayMode = trimmed.startsWith("[");
        List<JsonNode> items = new ArrayList<>();
        List<String> lineIssues = new ArrayList<>();
        if (arrayMode) {
            JsonNode root = objectMapper.readTree(trimmed);
            if (root == null || !root.isArray()) throw badRequest("JSON 数组结构无法解析");
            for (JsonNode item : root) {
                if (items.size() >= previewRows + 1) break;
                items.add(item);
            }
        } else {
            // JSON Lines：逐行解析，坏行跳过（真实日志文件常见）
            try (BufferedReader buffered = reader(file, charset)) {
                String line;
                while ((line = buffered.readLine()) != null) {
                    String row = line.trim();
                    if (row.isEmpty()) continue;
                    if (items.size() >= previewRows + 1) break;
                    try {
                        items.add(objectMapper.readTree(row));
                    } catch (Exception e) {
                        if (lineIssues.size() < 3) lineIssues.add(row.length() > 40 ? row.substring(0, 40) + "..." : row);
                    }
                }
            }
        }
        if (items.isEmpty()) throw badRequest("JSON 中没有可解析的记录");

        LinkedHashSet<String> keySet = new LinkedHashSet<>();
        for (JsonNode item : items) {
            if (item != null && item.isObject()) item.fieldNames().forEachRemaining(keySet::add);
        }
        List<String> names = new ArrayList<>(keySet);
        if (names.isEmpty()) {
            names.add("value");
            warnings.add("JSON 记录不是对象结构，字段名按 value 处理");
        }
        if (!lineIssues.isEmpty()) warnings.add("有无法解析的行已跳过，例如: " + String.join(" | ", lineIssues));

        List<List<String>> rows = new ArrayList<>();
        for (JsonNode item : items) {
            if (rows.size() >= previewRows) break;
            List<String> row = new ArrayList<>();
            for (String name : names) {
                JsonNode value = item == null ? null : item.get(name);
                if (value == null || value.isNull()) row.add("");
                else if (value.isValueNode()) row.add(value.asText());
                else row.add(value.toString());
            }
            rows.add(row);
        }
        boolean truncated = items.size() > previewRows;
        if (truncated) warnings.add("JSON 记录较多，仅分析前 " + previewRows + " 条");

        result.put("mode", arrayMode ? "array" : "lines");
        result.put("delimiter", ",");
        result.put("hasHeader", true);
        result.put("encoding", charset.name());
        result.put("columns", buildColumns(names, rows, warnings));
        result.put("rows", rows);
        result.put("truncated", truncated);
    }

    private void analyzeXml(Path file, Map<String, Object> result, List<String> warnings) throws IOException {
        String csvPath = xmlPreprocessor.convertToCsv(file.toString(), "", ",", "UTF-8");
        if (csvPath == null) {
            throw badRequest("XML 无法解析为记录列表结构（未找到重复的记录元素）");
        }
        Path csvFile = Path.of(csvPath);
        try {
            analyzeDelimited(csvFile, "csv", result, warnings);
            result.put("delimiter", ",");
        } finally {
            deleteQuietly(csvFile);
        }
        result.put("hasHeader", true);
        result.put("rowTag", "");
    }

    private void analyzeParquet(Path file, Map<String, Object> result, List<String> warnings) {
        List<String> names = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        boolean truncated = false;
        try (ParquetReader<Group> reader = ParquetReader
                .builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toString()))
                .withConf(new org.apache.hadoop.conf.Configuration())
                .build()) {
            Group group;
            while ((group = reader.read()) != null) {
                if (names.isEmpty()) {
                    for (Type field : group.getType().getFields()) names.add(field.getName());
                }
                if (rows.size() >= previewRows) {
                    truncated = true;
                    break;
                }
                List<String> row = new ArrayList<>();
                for (int i = 0; i < names.size(); i++) {
                    row.add(group.getFieldRepetitionCount(i) > 0 ? group.getValueToString(i, 0) : "");
                }
                rows.add(row);
            }
        } catch (Exception e) {
            log.warn("Parquet analyze failed for {}: {}", file, e.getMessage());
            throw badRequest("Parquet 解析失败（请确认为有效的 .parquet 文件）: " + e.getMessage());
        }
        if (names.isEmpty()) throw badRequest("Parquet 文件没有可读取的字段");

        if (truncated) warnings.add("仅分析前 " + previewRows + " 行");
        result.put("delimiter", ",");
        result.put("hasHeader", true);
        result.put("columns", buildColumns(names, rows, warnings));
        result.put("rows", rows);
        result.put("truncated", truncated);
    }

    // ===================== 列名与类型推断 =====================

    /** 生成 [{name, type}] 形式的字段建议，供前端确认对话框编辑后写入 fieldsConfig。 */
    private List<Map<String, String>> buildColumns(List<String> names, List<List<String>> rows, List<String> warnings) {
        List<Map<String, String>> columns = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            List<String> values = new ArrayList<>();
            for (List<String> row : rows) values.add(i < row.size() ? row.get(i) : "");
            Map<String, String> column = new LinkedHashMap<>();
            column.put("name", names.get(i));
            column.put("type", inferType(values));
            columns.add(column);
        }
        return columns;
    }

    /**
     * 样本类型推断：**空值一律退化为 STRING**（CSV/JSON 读取时空值会让 INT/DOUBLE 解析失败变 null），
     * 其余按 int → bigint → boolean → double → date 的顺序判定，兜底 STRING。
     * 规则实现已抽到 {@link com.datastream.mvp.util.DataTypeInference}，与 Kafka 字段推断共用同一口径。
     */
    private String inferType(List<String> values) {
        return com.datastream.mvp.util.DataTypeInference.infer(values);
    }

    // ===================== 编解码与解析工具 =====================

    /** 首行是否像表头：非空、无重复、且至少有一个非纯数字值。 */
    private boolean looksLikeHeader(CSVRecord record) {
        if (record == null || record.size() == 0) return false;
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        boolean anyText = false;
        for (int i = 0; i < record.size(); i++) {
            String value = record.get(i) == null ? "" : record.get(i).trim();
            if (value.isEmpty()) return false;
            if (!seen.add(value)) return false;
            if (!value.matches("[+-]?\\d+(\\.\\d+)?")) anyText = true;
        }
        return anyText;
    }

    private List<CSVRecord> parseCsv(List<String> lines, String delimiter) {
        String text = String.join("\n", lines);
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter.charAt(0))
                .setIgnoreEmptyLines(true)
                .build();
        try {
            return CSVParser.parse(text, format).getRecords();
        } catch (Exception e) {
            log.warn("CSV sample parse failed: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    private List<String> normalizeRow(CSVRecord record, int columnCount) {
        List<String> row = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) row.add(i < record.size() && record.get(i) != null ? record.get(i) : "");
        return row;
    }

    /** 分隔符探测：取样本行里出现次数最多且各行列数一致的候选。 */
    private String detectDelimiter(Path file, Charset charset) throws IOException {
        List<String> lines = readSampleLines(file, charset, 5);
        if (lines.isEmpty()) return ",";
        char[] candidates = {',', '\t', ';', '|'};
        char best = ',';
        int bestScore = -1;
        for (char candidate : candidates) {
            int headerCount = countChar(lines.get(0), candidate);
            if (headerCount == 0) continue;
            int consistent = 0;
            for (int i = 1; i < lines.size(); i++) {
                if (countChar(lines.get(i), candidate) == headerCount) consistent++;
            }
            int score = headerCount * 10 + consistent;
            if (score > bestScore) {
                bestScore = score;
                best = candidate;
            }
        }
        return String.valueOf(best);
    }

    private int countChar(String text, char target) {
        int count = 0;
        for (int i = 0; i < text.length(); i++) if (text.charAt(i) == target) count++;
        return count;
    }

    private List<String> readSampleLines(Path file, Charset charset, int limit) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = reader(file, charset)) {
            String line;
            while (lines.size() < limit && (line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    private boolean hasMoreLines(Path file, Charset charset, int consumed) throws IOException {
        int count = 0;
        try (BufferedReader reader = reader(file, charset)) {
            while (reader.readLine() != null) {
                count++;
                if (count > consumed) return true;
            }
        }
        return false;
    }

    private BufferedReader reader(Path file, Charset charset) throws IOException {
        return new BufferedReader(new InputStreamReader(Files.newInputStream(file), charset));
    }

    private String readAll(Path file, Charset charset) throws IOException {
        StringBuilder builder = new StringBuilder();
        try (Reader reader = new InputStreamReader(Files.newInputStream(file), charset);
             BufferedReader buffered = new BufferedReader(reader)) {
            char[] buffer = new char[8192];
            int read;
            while ((read = buffered.read(buffer)) != -1) {
                builder.append(buffer, 0, read);
                if (builder.length() > 8 * 1024 * 1024) break;   // 只分析前 8MB
            }
        }
        return builder.toString();
    }

    /**
     * 编码探测：BOM 优先，其次 UTF-8 严格解码，失败退 GB18030（中文 Windows 导出的 CSV 常见 GBK）。
     * 只读文件头部 64KB，避免大文件整读。
     */
    private Charset detectCharset(Path file) {
        byte[] head = new byte[0];
        try (InputStream input = Files.newInputStream(file)) {
            head = input.readNBytes(64 * 1024);
        } catch (IOException e) {
            log.warn("Read head failed for charset detection: {}", e.getMessage());
            return StandardCharsets.UTF_8;
        }
        if (head.length >= 3 && (head[0] & 0xFF) == 0xEF && (head[1] & 0xFF) == 0xBB && (head[2] & 0xFF) == 0xBF) {
            return StandardCharsets.UTF_8;
        }
        if (head.length >= 2 && (head[0] & 0xFF) == 0xFF && (head[1] & 0xFF) == 0xFE) {
            return StandardCharsets.UTF_16LE;
        }
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(head));
            return StandardCharsets.UTF_8;
        } catch (CharacterCodingException e) {
            try {
                return Charset.forName("GB18030");
            } catch (Exception ignored) {
                return StandardCharsets.UTF_8;
            }
        }
    }

    private String cellText(Cell cell, DataFormatter formatter) {
        if (cell == null) return "";
        try {
            return formatter.formatCellValue(cell).trim();
        } catch (Exception e) {
            return "";
        }
    }

    // ===================== 文件与类型工具 =====================

    private String controlTypeForExtension(String extension) {
        if (CSV_EXTS.contains(extension)) return "csv_input";
        if (EXCEL_EXTS.contains(extension)) return "excel_input";
        if (JSON_EXTS.contains(extension)) return "json_input";
        if (XML_EXTS.contains(extension)) return "xml_input";
        if (PARQUET_EXTS.contains(extension)) return "parquet_input";
        throw badRequest("暂不支持的文件类型: " + (extension.isEmpty() ? "（无扩展名）" : "." + extension)
                + "，支持 CSV / Excel / JSON / XML / Parquet");
    }

    /** 去掉浏览器可能带上的路径前缀（C:\fakepath\），并移除危险字符。 */
    private String sanitizeFileName(String original) {
        String name = original == null ? "" : original.trim();
        name = name.replace('\\', '/');
        int slash = name.lastIndexOf('/');
        if (slash >= 0) name = name.substring(slash + 1);
        name = name.replaceAll("[\\p{Cntrl}<>:\"/\\\\|?*]", "_");
        if (name.length() > 120) {
            String extension = extensionOf(name);
            String base = name.substring(0, name.length() - extension.length() - (extension.isEmpty() ? 0 : 1));
            name = base.substring(0, Math.min(base.length(), 100)) + (extension.isEmpty() ? "" : "." + extension);
        }
        return name.isEmpty() ? "upload" : name;
    }

    private String extensionOf(String fileName) {
        int dot = fileName.lastIndexOf('.');
        if (dot < 0 || dot == fileName.length() - 1) return "";
        return fileName.substring(dot + 1).toLowerCase(Locale.ROOT);
    }

    private long safeSize(Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            return 0L;
        }
    }

    private boolean deleteQuietly(Path file) {
        try {
            return Files.deleteIfExists(file);
        } catch (IOException e) {
            log.debug("Delete failed for {}: {}", file, e.getMessage());
            return false;
        }
    }

    private ResponseStatusException badRequest(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }
}
