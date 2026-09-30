package com.datastream.mvp.service;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver, mock(KafkaMonitorService.class)).validate(1L, user);

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

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver, mock(KafkaMonitorService.class)).validate(1L, user);

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

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver, mock(KafkaMonitorService.class)).validate(1L, user);

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

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver, mock(KafkaMonitorService.class)).validate(1L, user);

        assertEquals(true, result.get("valid"), "字符串形式的布尔/数值参数不应报类型错误: " + result.get("errors"));
    }

    @Test
    void acceptsArrayValueForStringFieldParamsFromLegacyData() {
        // field_concat.fields 历史数据存的是 JSON 数组，翻译层能解析，预检不应拦下
        JobService jobs = mock(JobService.class);
        ControlRegistryService controls = mock(ControlRegistryService.class);
        DataSourceNodeResolver resolver = mock(DataSourceNodeResolver.class);
        CurrentUser user = new CurrentUser(2L, "u", "U", "OPERATOR");
        JobDefinition job = new JobDefinition();
        job.setParallelism(1);
        job.setDagJson("{\"parallelism\":1,\"nodes\":[{\"id\":\"in\",\"type\":\"src\",\"label\":\"in\",\"params\":{}},"
                + "{\"id\":\"fc\",\"type\":\"concat\",\"label\":\"fc\",\"params\":{\"fields\":[\"a\",\"b\"]}},"
                + "{\"id\":\"out\",\"type\":\"sink\",\"label\":\"out\",\"params\":{}}],"
                + "\"edges\":[{\"source\":\"in\",\"target\":\"fc\"},{\"source\":\"fc\",\"target\":\"out\"}]}");
        when(jobs.findByIdForUser(1L, user)).thenReturn(job);
        when(controls.findAll()).thenReturn(List.of(
                control("src", "input", "{}"),
                control("concat", "transform", "{\"type\":\"object\",\"properties\":{\"fields\":{\"type\":\"string\"}}}"),
                control("sink", "output", "{}")));

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver, mock(KafkaMonitorService.class)).validate(1L, user);

        assertEquals(true, result.get("valid"), "数组形式的字符串参数不应报类型错误: " + result.get("errors"));
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

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper(), resolver, mock(KafkaMonitorService.class)).validate(1L, user);

        String errors = result.get("errors").toString();
        assertTrue(errors.contains("参数类型错误: hasHeader"), "非法布尔值仍应报错: " + errors);
        assertFalse(errors.contains("port"), "空串的可选参数不应报类型错误: " + errors);
    }

    /**
     * 回归：DAG 引用了输入节点未声明的字段时必须提交前拦下。
     * 复现 job 2726 的真实结构：kafka_input 只声明 key/value，
     * 但 redis_lookup 的 keyField=id —— 此前一路放行到 Flink，在 CREATE VIEW 阶段
     * 被 Calcite 以 "Column 'id' not found in any table" 拦下，而 Gateway REST 只回
     * {"status":"ERROR"} / 500 空 body，用户最终只看到 "Failed to fetchResults"。
     */
    @Test
    void rejectsFieldReferenceMissingFromDeclaredInputSchema() {
        Map<String, Object> result = preflightForRedisPipeline("[{\"name\":\"key\",\"type\":\"STRING\"},{\"name\":\"value\",\"type\":\"STRING\"}]");

        List<String> fieldErrors = fieldReferenceErrors(result);
        assertEquals(1, fieldErrors.size(), "应恰好报出一条字段引用错误: " + result.get("errors"));
        assertTrue(fieldErrors.get(0).contains("`id`"), "应点名缺失字段 id: " + fieldErrors.get(0));
        assertTrue(fieldErrors.get(0).contains("Redis 富化"), "应点名出错节点: " + fieldErrors.get(0));
        assertTrue(fieldErrors.get(0).contains("key"), "应列出可用字段: " + fieldErrors.get(0));
    }

    /** 字段由输入节点正确声明时不得误报（redis_lookup 的字段引用合法） */
    @Test
    void acceptsFieldReferencePresentInDeclaredInputSchema() {
        Map<String, Object> result = preflightForRedisPipeline("[{\"name\":\"key\",\"type\":\"STRING\"},{\"name\":\"id\",\"type\":\"STRING\"}]");

        assertTrue(fieldReferenceErrors(result).isEmpty(),
                "合法字段引用不应产生字段引用错误: " + result.get("errors"));
    }

    /** 只挑出「字段引用」这一类错误，便于把本用例与 Kafka topic 等环境相关校验解耦 */
    private List<String> fieldReferenceErrors(Map<String, Object> result) {
        List<String> out = new ArrayList<>();
        for (Object e : (List<?>) result.get("errors")) {
            if (String.valueOf(e).contains("上游输入声明的字段中不存在")) out.add(String.valueOf(e));
        }
        return out;
    }

    /**
     * 用 Jackson 构造 DAG（避免手写转义），复现 job 2726 的真实结构：
     * kafka_input(fieldsConfig) -> redis_lookup(keyField=id) -> mysql_output
     */
    private Map<String, Object> preflightForRedisPipeline(String kafkaFieldsConfig) {
        JobService jobs = mock(JobService.class);
        ControlRegistryService controls = mock(ControlRegistryService.class);
        DataSourceNodeResolver resolver = mock(DataSourceNodeResolver.class);
        CurrentUser user = new CurrentUser(2L, "u", "U", "OPERATOR");
        ObjectMapper mapper = new ObjectMapper();

        var kafkaIn = mapper.createObjectNode();
        kafkaIn.put("id", "kafka_in").put("type", "kafka_input").put("label", "Kafka 输入");
        var kafkaParams = kafkaIn.putObject("params");
        kafkaParams.put("topic", "kafka_sales_flow").put("bootstrapServers", "localhost:29092").put("fieldsConfig", kafkaFieldsConfig);

        var redis = mapper.createObjectNode();
        redis.put("id", "redis_enrich").put("type", "redis_lookup").put("label", "Redis 富化");
        var redisParams = redis.putObject("params");
        redisParams.put("host", "localhost").put("port", "6379")
                .put("keyField", "id").put("targetField", "extra_info");

        var mysqlOut = mapper.createObjectNode();
        mysqlOut.put("id", "mysql_out").put("type", "mysql_output").put("label", "MySQL Output");
        mysqlOut.putObject("params");

        var root = mapper.createObjectNode();
        root.put("parallelism", 1);
        var nodes = root.putArray("nodes");
        nodes.add(kafkaIn); nodes.add(redis); nodes.add(mysqlOut);
        var edges = root.putArray("edges");
        edges.addObject().put("source", "kafka_in").put("target", "redis_enrich");
        edges.addObject().put("source", "redis_enrich").put("target", "mysql_out");

        JobDefinition job = new JobDefinition();
        job.setParallelism(1);
        job.setDagJson(root.toString());
        when(jobs.findByIdForUser(1L, user)).thenReturn(job);
        when(controls.findAll()).thenReturn(List.of(
                control("kafka_input", "input", "{}"),
                control("redis_lookup", "transform", "{}"),
                control("mysql_output", "output", "{}")));

        return new DagPreflightService(jobs, controls, mapper, resolver, mock(KafkaMonitorService.class))
                .validate(1L, user);
    }

    private ControlRegistry control(String type, String category, String schema) {
        ControlRegistry c = new ControlRegistry(); c.setType(type); c.setCategory(category); c.setParamSchema(schema); c.setEnabled(true); return c;
    }
}
