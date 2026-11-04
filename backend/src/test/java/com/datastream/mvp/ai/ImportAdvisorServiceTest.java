package com.datastream.mvp.ai;

import com.datastream.mvp.service.PreviewService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 文件导入建议器回归：本地规则必须离线可用（未配置 LLM 也能给出建议），
 * 且覆盖「编号列别用 INT」「全空列」「字段名规范化」「主键候选」「缺时间列」这些真实踩过的坑。
 */
class ImportAdvisorServiceTest {

    private PreviewService previewService;
    private LlmClient llmClient;
    private ImportAdvisorService advisor;

    @BeforeEach
    void setUp() {
        previewService = mock(PreviewService.class);
        llmClient = mock(LlmClient.class);
        when(llmClient.isConfigured()).thenReturn(false);   // 默认离线：只跑本地规则
        advisor = new ImportAdvisorService(previewService, llmClient, new ObjectMapper());
    }

    private void givenPreview(List<String> columns, List<List<String>> rows) {
        Map<String, Object> preview = new LinkedHashMap<>();
        preview.put("path", "output/uploads/x.csv");
        preview.put("columns", columns);
        preview.put("rows", rows);
        when(previewService.previewFile(anyString(), anyInt())).thenReturn(preview);
    }

    private static List<String> row(String... cells) {
        return new ArrayList<>(List.of(cells));
    }

    private Map<String, Object> suggestionByKind(List<Map<String, Object>> suggestions, String kind) {
        return suggestions.stream().filter(s -> kind.equals(s.get("kind"))).findFirst().orElse(null);
    }

    @Test
    void flagsLeadingZeroIdAndSuggestsString() {
        givenPreview(List.of("order_id", "region", "sale_date"), List.of(
                row("123", "华北", "2026-01-01"),
                row("456", "华南", "2026-01-02"),
                row("789", "华东", "2026-01-03")));

        Map<String, Object> result = advisor.inspect("output/uploads/x.csv", "csv_input");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> suggestions = (List<Map<String, Object>>) result.get("suggestions");

        Map<String, Object> s = suggestionByKind(suggestions, "leading-zero");
        assertNotNull(s, "编号列（sample 全为整数）必须提示前导零风险: " + suggestions);
        assertTrue(String.valueOf(s.get("detail")).contains("STRING"));
        assertEquals("local-rules", result.get("source"), "未配置 LLM 时应标记为本地规则");
        assertNull(suggestionByKind(suggestions, "no-time-column"), "有 sale_date 列就不该提示缺时间列");
    }

    @Test
    void flagsEmptyColumnAndMissingTimeColumn() {
        givenPreview(List.of("amount", "note"), List.of(
                row("12.5", ""),
                row("18", ""),
                row("7.5", "")));

        Map<String, Object> result = advisor.inspect("output/uploads/x.csv", "csv_input");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> suggestions = (List<Map<String, Object>>) result.get("suggestions");

        assertNotNull(suggestionByKind(suggestions, "empty-column"), "全空列必须提示: " + suggestions);
        assertNotNull(suggestionByKind(suggestions, "no-time-column"), "没有时间列必须提示");
    }

    @Test
    void normalizesBadColumnNamesAndExposesRenames() {
        givenPreview(List.of("Order ID", "金额(元)"), List.of(
                row("A-1", "12.5"),
                row("A-2", "18"),
                row("A-3", "7.5")));

        Map<String, Object> result = advisor.inspect("output/uploads/x.csv", "csv_input");
        @SuppressWarnings("unchecked")
        Map<String, String> renames = (Map<String, String>) result.get("renames");
        assertEquals("order_id", renames.get("Order ID"), "含空格的字段名应给出规范化建议");
        assertEquals("金额_元", renames.get("金额(元)"), "含括号的字段名应给出规范化建议");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> suggestions = (List<Map<String, Object>>) result.get("suggestions");
        assertNotNull(suggestionByKind(suggestions, "name-style"));
    }

    @Test
    void detectsUniqueKeyCandidate() {
        givenPreview(List.of("user_id", "name"), List.of(
                row("U001", "张三"),
                row("U002", "李四"),
                row("U003", "王五")));

        Map<String, Object> result = advisor.inspect("output/uploads/x.csv", "csv_input");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> suggestions = (List<Map<String, Object>>) result.get("suggestions");
        Map<String, Object> unique = suggestionByKind(suggestions, "unique-key");
        assertNotNull(unique, "唯一且像 id 的列应提示可作为去重键: " + suggestions);
        assertTrue(String.valueOf(unique.get("title")).contains("user_id"));
    }

    @Test
    void degradesToLocalRulesWhenLlmFails() throws Exception {
        givenPreview(List.of("id", "name"), List.of(
                row("1", "a"), row("2", "b"), row("3", "c")));
        when(llmClient.isConfigured()).thenReturn(true);
        when(llmClient.providerName()).thenReturn("deepseek");
        when(llmClient.chatJson(anyString(), anyString(), any())).thenThrow(new RuntimeException("LLM 超时"));

        Map<String, Object> result = advisor.inspect("output/uploads/x.csv", "csv_input");
        // LLM 挂了不能让确认框也跟着挂：仍返回本地建议
        assertEquals("local-rules", result.get("source"));
        assertFalse(((List<?>) result.get("suggestions")).isEmpty());
    }

    @Test
    void rejectsUnknownOrUnreadableFile() {
        Map<String, Object> preview = new LinkedHashMap<>();
        preview.put("message", "文件不存在: x.csv");
        when(previewService.previewFile(anyString(), anyInt())).thenReturn(preview);

        assertEquals(400, assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> advisor.inspect("output/uploads/x.csv", "csv_input")).getStatusCode().value());
        assertEquals(400, assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> advisor.inspect(" ", "csv_input")).getStatusCode().value());
    }
}
