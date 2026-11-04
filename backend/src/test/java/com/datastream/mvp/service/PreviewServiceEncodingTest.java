package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 数据预览的编码回归：
 * ① 真 GBK 文件要能正确预览（旧实现写死 UTF-8，中文全变 \uFFFD）；
 * ② 真 UTF-8 大文件（64KB 边界切在汉字中间）不能误判成 GB18030（2026-11-04 事故）。
 */
class PreviewServiceEncodingTest {

    @TempDir
    Path tempDir;

    private PreviewService previewService;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper();
        previewService = new PreviewService(mapper, new XmlPreprocessor(mapper));
        ReflectionTestUtils.setField(previewService, "allowedRoots", tempDir.toString());
        ReflectionTestUtils.setField(previewService, "maxFileBytes", 64L * 1024 * 1024);
    }

    @Test
    @SuppressWarnings("unchecked")
    void previewsRealGbkFile() throws Exception {
        Charset gbk = Charset.forName("GB18030");
        Path file = tempDir.resolve("gbk.csv");
        Files.write(file, "编号,名称,金额\n1,测试数据,12.5\n2,平板,99.9\n".getBytes(gbk));

        Map<String, Object> result = previewService.previewFile(file.toString(), 10);

        assertEquals("GB18030", result.get("encoding"));
        assertEquals(List.of("编号", "名称", "金额"), (List<String>) result.get("columns"));
        List<List<String>> rows = (List<List<String>>) result.get("rows");
        assertEquals("测试数据", rows.get(0).get(1));
        assertEquals("平板", rows.get(1).get(1));
        assertFalse(rows.toString().contains("\uFFFD"), "GBK 文件不应出现替换符");
    }

    @Test
    @SuppressWarnings("unchecked")
    void previewsUtf8FileWhoseSampleSplitsACharacter() throws Exception {
        String header = "id,product\n";
        int headerLen = header.getBytes(StandardCharsets.UTF_8).length;
        int padLen = com.datastream.mvp.util.TextEncoding.SAMPLE_BYTES - 1 - headerLen;
        StringBuilder content = new StringBuilder(header);
        content.append("0,").append("x".repeat(Math.max(0, padLen - 3))).append('\n');
        for (int i = 1; i <= 5; i++) content.append(i).append(",平板\n");

        Path file = tempDir.resolve("boundary.csv");
        Files.write(file, content.toString().getBytes(StandardCharsets.UTF_8));

        Map<String, Object> result = previewService.previewFile(file.toString(), 10);

        assertEquals("UTF-8", result.get("encoding"));
        List<List<String>> rows = (List<List<String>>) result.get("rows");
        assertEquals("平板", rows.get(1).get(1));
        assertFalse(rows.toString().contains("\uFFFD"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void repairsMojibakeInPreviewRows() throws Exception {
        Charset gbk = Charset.forName("GB18030");
        String mojibake = new String("平板".getBytes(StandardCharsets.UTF_8), gbk);
        Path file = tempDir.resolve("mojibake.csv");
        Files.write(file, ("id,product\n1," + mojibake + "\n").getBytes(StandardCharsets.UTF_8));

        Map<String, Object> result = previewService.previewFile(file.toString(), 10);

        List<List<String>> rows = (List<List<String>>) result.get("rows");
        assertEquals("平板", rows.get(0).get(1), "预览也要做乱码还原（与导入分析同口径）");
        assertNotNull(result.get("note"), "还原后要提示内容编码有问题");
    }
}
