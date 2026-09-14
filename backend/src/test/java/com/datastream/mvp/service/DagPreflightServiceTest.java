package com.datastream.mvp.service;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DagPreflightServiceTest {
    @Test
    void validatesRequiredEnumGraphAndParallelismWithoutSideEffects() {
        JobService jobs = mock(JobService.class);
        ControlRegistryService controls = mock(ControlRegistryService.class);
        DataSourceNodeResolver resolver = mock(DataSourceNodeResolver.class);
        CurrentUser user = new CurrentUser(2L, "u", "U", "OPERATOR");
        JobDefinition job = new JobDefinition();
        job.setParallelism(1);
        job.setDagJson("{\"parallelism\":129,\"nodes\":[{\"id\":\"in\",\"type\":\"source\",\"params\":{}},{\"id\":\"out\",\"type\":\"sink\",\"params\":{\"mode\":\"bad\"}}],\"edges\":[{\"source\":\"in\",\"target\":\"out\"},{\"source\":\"in\",\"target\":\"out\"}]}");
        when(jobs.findByIdForUser(1L, user)).thenReturn(job);
        when(controls.findAll()).thenReturn(List.of(control("source", "input", "{\"type\":\"object\",\"required\":[\"path\"],\"properties\":{\"path\":{\"type\":\"string\"}}}"), control("sink", "output", "{\"type\":\"object\",\"properties\":{\"mode\":{\"type\":\"string\",\"enum\":[\"append\"]}}}")));

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver).validate(1L, user);

        assertEquals(false, result.get("valid"));
        String errors = result.get("errors").toString();
        assertTrue(errors.contains("并行度"));
        assertTrue(errors.contains("缺少必填参数"));
        assertTrue(errors.contains("枚举"));
        assertTrue(errors.contains("重复边"));
        verify(jobs).findByIdForUser(1L, user);
        verifyNoMoreInteractions(jobs);
    }

    @Test
    void returnsPureDagWarningsWithoutInvalidatingDag() {
        JobService jobs = mock(JobService.class);
        ControlRegistryService controls = mock(ControlRegistryService.class);
        DataSourceNodeResolver resolver = mock(DataSourceNodeResolver.class);
        CurrentUser user = new CurrentUser(2L, "u", "U", "OPERATOR");
        JobDefinition job = new JobDefinition();
        job.setParallelism(1);
        job.setDagJson("{\"parallelism\":17,\"nodes\":[{\"id\":\"in\",\"type\":\"source\",\"params\":{}},{\"id\":\"out\",\"type\":\"sink\",\"label\":\"输出\",\"params\":{}}],\"edges\":[{\"source\":\"in\",\"target\":\"out\"}]}");
        when(jobs.findByIdForUser(1L, user)).thenReturn(job);
        when(controls.findAll()).thenReturn(List.of(control("source", "input", "{}"), control("sink", "output", "{}")));

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver).validate(1L, user);

        assertEquals(true, result.get("valid"));
        assertTrue(((List<?>) result.get("errors")).isEmpty());
        String warnings = result.get("warnings").toString();
        assertTrue(warnings.contains("并行度大于 16"));
        assertTrue(warnings.contains("未设置显示名称"));
    }

    @Test
    void dataSourceReferenceSkipsInlineConnectionRequiredFields() {
        JobService jobs = mock(JobService.class);
        ControlRegistryService controls = mock(ControlRegistryService.class);
        DataSourceNodeResolver resolver = mock(DataSourceNodeResolver.class);
        CurrentUser user = new CurrentUser(2L, "u", "U", "OPERATOR");
        JobDefinition job = new JobDefinition();
        job.setParallelism(1);
        // 引用数据源时节点不再内联 url/username，只有资产级 table
        job.setDagJson("{\"parallelism\":1,\"nodes\":[{\"id\":\"in\",\"type\":\"mysql_input\",\"label\":\"in\",\"params\":{\"dataSourceId\":7,\"table\":\"orders\"}},{\"id\":\"out\",\"type\":\"sink\",\"label\":\"out\",\"params\":{}}],\"edges\":[{\"source\":\"in\",\"target\":\"out\"}]}");
        when(jobs.findByIdForUser(1L, user)).thenReturn(job);
        when(controls.findAll()).thenReturn(List.of(
                control("mysql_input", "input", "{\"type\":\"object\",\"required\":[\"url\",\"table\",\"username\"],\"properties\":{}}"),
                control("sink", "output", "{}")));

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver).validate(1L, user);

        assertEquals(true, result.get("valid"), "引用数据源时不应因缺少连接级参数而失败: " + result.get("errors"));
    }

    @Test
    void acceptsStringFormsOfBooleanAndNumberParamsFromLegacyData() {
        // 历史数据与旧前端把开关/数值存成字符串，后端翻译层按 toString() 取值，预检不应拦下
        JobService jobs = mock(JobService.class);
        ControlRegistryService controls = mock(ControlRegistryService.class);
        DataSourceNodeResolver resolver = mock(DataSourceNodeResolver.class);
        CurrentUser user = new CurrentUser(2L, "u", "U", "OPERATOR");
        JobDefinition job = new JobDefinition();
        job.setParallelism(1);
        job.setDagJson("{\"parallelism\":1,\"nodes\":[{\"id\":\"in\",\"type\":\"src\",\"label\":\"in\","
                + "\"params\":{\"hasHeader\":\"true\",\"batchSize\":\"1000\",\"port\":\"6379\"}},"
                + "{\"id\":\"out\",\"type\":\"sink\",\"label\":\"out\",\"params\":{}}],"
                + "\"edges\":[{\"source\":\"in\",\"target\":\"out\"}]}");
        when(jobs.findByIdForUser(1L, user)).thenReturn(job);
        when(controls.findAll()).thenReturn(List.of(
                control("src", "input", "{\"type\":\"object\",\"properties\":{\"hasHeader\":{\"type\":\"boolean\"},"
                        + "\"batchSize\":{\"type\":\"number\"},\"port\":{\"type\":\"integer\"}}}"),
                control("sink", "output", "{}")));

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver).validate(1L, user);

        assertEquals(true, result.get("valid"), "字符串形式的布尔/数值参数不应报类型错误: " + result.get("errors"));
    }

    @Test
    void rejectsNonBooleanAndBlankOptionalParamsAreTreatedAsAbsent() {
        JobService jobs = mock(JobService.class);
        ControlRegistryService controls = mock(ControlRegistryService.class);
        DataSourceNodeResolver resolver = mock(DataSourceNodeResolver.class);
        CurrentUser user = new CurrentUser(2L, "u", "U", "OPERATOR");
        JobDefinition job = new JobDefinition();
        job.setParallelism(1);
        // 可选数值参数被表单留空回传空串，应视为未提供；hasHeader 填了非布尔值才是真错误
        job.setDagJson("{\"parallelism\":1,\"nodes\":[{\"id\":\"in\",\"type\":\"src\",\"label\":\"in\","
                + "\"params\":{\"hasHeader\":\"yes\",\"port\":\"\"}},"
                + "{\"id\":\"out\",\"type\":\"sink\",\"label\":\"out\",\"params\":{}}],"
                + "\"edges\":[{\"source\":\"in\",\"target\":\"out\"}]}");
        when(jobs.findByIdForUser(1L, user)).thenReturn(job);
        when(controls.findAll()).thenReturn(List.of(
                control("src", "input", "{\"type\":\"object\",\"properties\":{\"hasHeader\":{\"type\":\"boolean\"},"
                        + "\"port\":{\"type\":\"integer\"}}}"),
                control("sink", "output", "{}")));

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver).validate(1L, user);

        String errors = result.get("errors").toString();
        assertTrue(errors.contains("参数类型错误: hasHeader"), "非法布尔值仍应报错: " + errors);
        assertFalse(errors.contains("port"), "空串的可选参数不应报类型错误: " + errors);
    }

    private ControlRegistry control(String type, String category, String schema) {
        ControlRegistry c = new ControlRegistry(); c.setType(type); c.setCategory(category); c.setParamSchema(schema); c.setEnabled(true); return c;
    }
}
