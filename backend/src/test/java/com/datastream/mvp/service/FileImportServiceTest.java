package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 画布「拖拽文件导入」分析服务回归：暂存路径、分隔符/表头/类型识别、编码告警、类型与目录校验。
 */
class FileImportServiceTest {

    @TempDir
    Path tempDir;

    private FileImportService service;
    private Path uploadRoot;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        service = new FileImportService(objectMapper, new XmlPreprocessor(objectMapper));
        ReflectionTestUtils.setField(service, "outputRoot", tempDir.toString());
        ReflectionTestUtils.setField(service, "uploadSubdir", "uploads");
        ReflectionTestUtils.setField(service, "maxUploadBytes", 1024L * 1024L);
        ReflectionTestUtils.setField(service, "previewRows", 20);
        uploadRoot = tempDir.resolve("uploads").toAbsolutePath().normalize();
    }

    @Test
    @SuppressWarnings("unchecked")
    void analyzesCsvWithHeaderAndInfersColumnTypes() throws Exception {
        String csv = "id,name,amount,active,day\n"
                + "1,张三,12.5,true,2026-01-01\n"
                + "2,李四,20,false,2026-01-02\n";
        Map<String, Object> result = analyze("sales.csv", csv.getBytes(StandardCharsets.UTF_8));

        assertEquals("csv_input", result.get("controlType"));
        assertEquals(",", result.get("delimiter"));
        assertEquals(Boolean.TRUE, result.get("hasHeader"));
        assertTrue(Path.of((String) result.get("path")).startsWith(uploadRoot), "暂存路径必须落在 uploads 目录内");

        List<Map<String, String>> columns = (List<Map<String, String>>) result.get("columns");
        assertEquals(List.of("id", "name", "amount", "active", "day"),
                columns.stream().map(c -> c.get("name")).toList());
        assertEquals(List.of("INT", "STRING", "DOUBLE", "BOOLEAN", "DATE"),
                columns.stream().map(c -> c.get("type")).toList());

        List<List<String>> rows = (List<List<String>>) result.get("rows");
        assertEquals(2, rows.size());
        assertEquals("张三", rows.get(0).get(1));
        assertTrue(Files.exists(Path.of((String) result.get("path"))), "分析完成后文件应保留（节点参数指向它）");
    }

    @Test
    @SuppressWarnings("unchecked")
    void detectsSemicolonDelimiterAndHeaderlessFile() throws Exception {
        String csv = "1;2;3\n4;5;6\n";
        Map<String, Object> result = analyze("plain.csv", csv.getBytes(StandardCharsets.UTF_8));

        assertEquals(";", result.get("delimiter"));
        assertEquals(Boolean.FALSE, result.get("hasHeader"));
        List<Map<String, String>> columns = (List<Map<String, String>>) result.get("columns");
        assertEquals(List.of("col1", "col2", "col3"), columns.stream().map(c -> c.get("name")).toList());
        assertEquals(List.of("INT", "INT", "INT"), columns.stream().map(c -> c.get("type")).toList());
        // 无表头时首行也算数据
        assertEquals(2, ((List<List<String>>) result.get("rows")).size());
    }

    @Test
    void warnsOnNonUtf8EncodingAndKeepsChineseReadable() throws Exception {
        Charset gbk = Charset.forName("GB18030");
        String csv = "编号,名称\n1,测试数据\n";
        Map<String, Object> result = analyze("gbk.csv", csv.getBytes(gbk));

        assertEquals("GB18030", result.get("encoding"));
        assertTrue(String.valueOf(result.get("warnings")).contains("非 UTF-8"), "非 UTF-8 文件必须给出转码告警");
        @SuppressWarnings("unchecked")
        List<List<String>> rows = (List<List<String>>) result.get("rows");
        assertEquals("测试数据", rows.get(0).get(1));
    }

    @Test
    void rejectsUnsupportedOrEmptyUploads() {
        assertEquals(400, assertThrows(ResponseStatusException.class,
                () -> analyze("note.md", "hello".getBytes(StandardCharsets.UTF_8))).getStatusCode().value());
        assertEquals(400, assertThrows(ResponseStatusException.class,
                () -> analyze("empty.csv", new byte[0])).getStatusCode().value());
        // 超限文件必须在写盘前被拦下
        byte[] big = new byte[1024 * 1024 + 1];
        assertEquals(413, assertThrows(ResponseStatusException.class,
                () -> analyze("big.csv", big)).getStatusCode().value());
    }

    @Test
    void discardsOnlyFilesInsideUploadRoot() throws Exception {
        Map<String, Object> result = analyze("sales.csv", "id,name\n1,a\n".getBytes(StandardCharsets.UTF_8));
        Path stored = Path.of((String) result.get("path"));

        Map<String, Object> deleted = service.discard(stored.toString());
        assertEquals(Boolean.TRUE, deleted.get("deleted"));
        assertFalse(Files.exists(stored));

        Path outside = tempDir.resolve("keep.csv");
        Files.writeString(outside, "id\n1\n");
        assertEquals(400, assertThrows(ResponseStatusException.class,
                () -> service.discard(outside.toString())).getStatusCode().value());
        assertTrue(Files.exists(outside), "uploads 之外的文件不允许被删除");
    }

    private Map<String, Object> analyze(String fileName, byte[] content) {
        return service.analyze(new MockMultipartFile("file", fileName, "application/octet-stream", content));
    }
}
