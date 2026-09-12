package com.datastream.mvp.service;

import com.datastream.mvp.dag.DagDefinition;
import com.datastream.mvp.model.ControlRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * DagTranslationService 翻译层单测（保护核心 schema 传播 / 路径重写 / 函数注入逻辑）
 */
class DagTranslationServiceTest {

    // CI 兼容：从模块目录定位项目根（test-resources 位于仓库根）
    private static final java.nio.file.Path PROJECT_ROOT = java.nio.file.Paths.get(System.getProperty("user.dir")).getParent();
    private static final String SALES_CSV = PROJECT_ROOT.resolve("test-resources/data/sales.csv").toString();
    private static final String SAMPLE_JSON = PROJECT_ROOT.resolve("test-resources/data/sample.json").toString();
    private static final String OUT_DIR = PROJECT_ROOT.resolve("output").toString();

    private ControlRegistryService controlService;
    private MysqlTableCreator mysqlTableCreator;
    private PostgresqlTableCreator postgresqlTableCreator;
    private OracleTableCreator oracleTableCreator;
    private ExcelPreprocessor excelPreprocessor;
    private XmlPreprocessor xmlPreprocessor;
    private JsonPreprocessor jsonPreprocessor;
    private ParquetPreprocessor parquetPreprocessor;
    private DagTranslationService service;

    @BeforeEach
    void setUp() {
        controlService = mock(ControlRegistryService.class);
        mysqlTableCreator = mock(MysqlTableCreator.class);
        postgresqlTableCreator = mock(PostgresqlTableCreator.class);
        oracleTableCreator = mock(OracleTableCreator.class);
        excelPreprocessor = mock(ExcelPreprocessor.class);
        xmlPreprocessor = mock(XmlPreprocessor.class);
        jsonPreprocessor = mock(JsonPreprocessor.class);
        parquetPreprocessor = mock(ParquetPreprocessor.class);
        service = new DagTranslationService(controlService, new ObjectMapper(),
                excelPreprocessor, mysqlTableCreator, postgresqlTableCreator, oracleTableCreator,
                xmlPreprocessor, jsonPreprocessor, parquetPreprocessor);
        stubControls();
    }

    private ControlRegistry control(String type, String category, String template) {
        ControlRegistry c = new ControlRegistry();
        c.setType(type);
        c.setCategory(category);
        c.setFlinkTemplate(template);
        return c;
    }

    private void stubControls() {
        when(controlService.findByType("csv_input")).thenReturn(control("csv_input", "input",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = ',',\n  'csv.ignore-parse-errors' = 'true'\n);"));
        when(controlService.findByType("csv_output")).thenReturn(control("csv_output", "output",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);"));
        when(controlService.findByType("json_input")).thenReturn(control("json_input", "input", ""));
        when(controlService.findByType("json_output")).thenReturn(control("json_output", "output",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'json'\n);"));
        when(controlService.findByType("datagen_input")).thenReturn(control("datagen_input", "input", ""));
        when(controlService.findByType("hdfs_input")).thenReturn(control("hdfs_input", "input", ""));
        when(controlService.findByType("hdfs_output")).thenReturn(control("hdfs_output", "output", ""));
        when(controlService.findByType("pg_output")).thenReturn(control("pg_output", "output", ""));
        when(controlService.findByType("oracle_output")).thenReturn(control("oracle_output", "output", ""));
        when(postgresqlTableCreator.qualifiedTable(anyMap())).thenReturn("public.target_table");
        when(oracleTableCreator.qualifiedTable(anyMap())).thenReturn("DATAFLOW.TARGET_TABLE");
        when(controlService.findByType("field_concat")).thenReturn(control("field_concat", "transform",
                "SELECT *, CONCAT(${fields}) AS `${newFieldName}` FROM ${id}"));
        when(controlService.findByType("xml_json")).thenReturn(control("xml_json", "transform",
                "SELECT *, ${direction}(`${sourceField}`) AS `${targetField}` FROM ${id}"));
        when(controlService.findByType("redis_lookup")).thenReturn(control("redis_lookup", "transform", ""));
    }

    private DagDefinition.DagNode node(String id, String type, Map<String, Object> params) {
        DagDefinition.DagNode n = new DagDefinition.DagNode();
        n.setId(id);
        n.setType(type);
        n.setLabel(type);
        n.setParams(params);
        return n;
    }

    private DagDefinition.DagEdge edge(String id, String source, String target) {
        DagDefinition.DagEdge e = new DagDefinition.DagEdge();
        e.setId(id);
        e.setSource(source);
        e.setTarget(target);
        return e;
    }

    private DagDefinition dag(String name, int parallelism, List<DagDefinition.DagNode> nodes, List<DagDefinition.DagEdge> edges) {
        DagDefinition d = new DagDefinition();
        d.setJobName(name);
        d.setParallelism(parallelism);
        d.setNodes(nodes);
        d.setEdges(edges);
        return d;
    }

    private Map<String, Object> csvInputParams() {
        Map<String, Object> p = new HashMap<>();
        p.put("path", SALES_CSV);
        p.put("delimiter", ",");
        p.put("hasHeader", "true");
        return p;
    }

    private Map<String, Object> csvOutputParams(String name) {
        Map<String, Object> p = new HashMap<>();
        p.put("path", OUT_DIR + java.io.File.separator + name);
        p.put("delimiter", ",");
        return p;
    }

    @Test
    void csvToCsv_propagatesSchemaAndRewritesOutputPath() {
        List<DagDefinition.DagNode> nodes = List.of(
                node("csv_input_1", "csv_input", csvInputParams()),
                node("csv_output_1", "csv_output", csvOutputParams("out.csv")));
        List<DagDefinition.DagEdge> edges = List.of(edge("e1", "csv_input_1", "csv_output_1"));

        String sql = service.translate(dag("csv-to-csv", 2, nodes, edges));

        assertTrue(sql.contains("SET 'parallelism.default' = '2'"), "并行度应写入 SQL");
        assertTrue(sql.contains("CREATE TABLE csv_input_1"), "应生成输入表 DDL");
        assertTrue(sql.contains("CREATE TABLE csv_output_1"), "应生成输出表 DDL");
        assertTrue(sql.contains("sale_id"), "schema 应从 CSV 表头传播到输出表: " + sql);
        assertTrue(sql.contains("out.csv.tmp"), "输出路径应重写为 .tmp 临时目录（完成后合并）");
        assertTrue(sql.contains("INSERT INTO csv_output_1"), "应生成 INSERT");
    }

    @Test
    void datagenToCsv_generatesDatagenDdl() {
        Map<String, Object> in = new HashMap<>();
        in.put("rowsPerSecond", "100");
        in.put("fieldsConfig", "[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]");
        List<DagDefinition.DagNode> nodes = List.of(
                node("dg_1", "datagen_input", in),
                node("out_1", "csv_output", csvOutputParams("dg.csv")));
        List<DagDefinition.DagEdge> edges = List.of(edge("e1", "dg_1", "out_1"));

        String sql = service.translate(dag("datagen", 1, nodes, edges));

        assertTrue(sql.contains("datagen"), "应生成 datagen DDL: " + sql);
        assertTrue(sql.contains("id"), "应提取 fieldsConfig 字段");
        assertTrue(sql.contains("INSERT INTO out_1"), "应生成 INSERT");
    }

    @Test
    void csvThroughFieldConcat_propagatesNewField() {
        Map<String, Object> tf = new HashMap<>();
        tf.put("fields", "`sale_id`, `amount`");
        tf.put("separator", ",");
        tf.put("newFieldName", "merged");
        List<DagDefinition.DagNode> nodes = List.of(
                node("csv_input_1", "csv_input", csvInputParams()),
                node("fc_1", "field_concat", tf),
                node("csv_output_1", "csv_output", csvOutputParams("concat.csv")));
        List<DagDefinition.DagEdge> edges = List.of(
                edge("e1", "csv_input_1", "fc_1"),
                edge("e2", "fc_1", "csv_output_1"));

        String sql = service.translate(dag("concat", 1, nodes, edges));

        assertTrue(sql.contains("sale_id"), "上游 schema 应传播");
        assertTrue(sql.contains("merged"), "field_concat 新字段应出现在链路: " + sql);
        assertTrue(sql.contains("INSERT INTO csv_output_1\nSELECT *, CONCAT(`sale_id`, `amount`) AS `merged` FROM csv_input_1;"),
                "应生成完整可执行 INSERT SELECT: " + sql);
    }

    @Test
    void jsonLinesToJsonOutput_generatesJsonTables() {
        Map<String, Object> in = new HashMap<>();
        in.put("path", SAMPLE_JSON);
        in.put("mode", "lines");
        in.put("fieldsConfig", "[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]");
        Map<String, Object> out = new HashMap<>();
        out.put("path", OUT_DIR + java.io.File.separator + "out.json");
        out.put("mode", "lines");
        List<DagDefinition.DagNode> nodes = List.of(
                node("json_input_1", "json_input", in),
                node("json_output_1", "json_output", out));
        List<DagDefinition.DagEdge> edges = List.of(edge("e1", "json_input_1", "json_output_1"));

        String sql = service.translate(dag("json", 1, nodes, edges));

        assertTrue(sql.contains("CREATE TABLE json_input_1"), "应生成 JSON 输入表");
        assertTrue(sql.contains("CREATE TABLE json_output_1"), "应生成 JSON 输出表");
        assertTrue(sql.contains("out.json.tmp"), "JSON 输出路径应重写为 .tmp");
        assertTrue(sql.contains("INSERT INTO json_output_1"), "应生成 INSERT");
    }

    @Test
    void datagenToPostgresql_generatesSinkAndPreparesTable() {
        Map<String, Object> in = new HashMap<>();
        in.put("rowsPerSecond", "10");
        in.put("fieldsConfig", "[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]");
        Map<String, Object> out = new HashMap<>();
        out.put("url", "jdbc:postgresql://localhost:5432/dataflow");
        out.put("schema", "public");
        out.put("table", "target_table");
        out.put("username", "postgres");
        out.put("password", "secret");
        out.put("createTablePolicy", "CREATE_IF_MISSING");
        out.put("batchSize", 500);
        List<DagDefinition.DagNode> nodes = List.of(
                node("dg_1", "datagen_input", in),
                node("pg_out_1", "pg_output", out));

        String sql = service.translate(dag("pg-output", 1, nodes,
                List.of(edge("e1", "dg_1", "pg_out_1"))));

        assertTrue(sql.contains("CREATE TABLE pg_out_1"));
        assertTrue(sql.contains("'url' = 'jdbc:postgresql://localhost:5432/dataflow'"));
        assertTrue(sql.contains("'table-name' = 'public.target_table'"));
        assertTrue(sql.contains("'driver' = 'org.postgresql.Driver'"));
        assertTrue(sql.contains("'sink.buffer-flush.max-rows' = '500'"));
        assertTrue(sql.contains("INSERT INTO pg_out_1 SELECT * FROM dg_1;"));
        verify(postgresqlTableCreator).ensureTable(anyMap(), anyString());
    }

    @Test
    void datagenToOracle_generatesSinkAndPreparesTable() {
        Map<String, Object> in = new HashMap<>();
        in.put("rowsPerSecond", "10");
        in.put("fieldsConfig", "[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]");
        Map<String, Object> out = new HashMap<>();
        out.put("url", "jdbc:oracle:thin:@localhost:1521/FREEPDB1");
        out.put("schema", "DATAFLOW");
        out.put("table", "TARGET_TABLE");
        out.put("username", "dataflow");
        out.put("password", "secret");
        out.put("createTablePolicy", "CREATE_IF_MISSING");
        List<DagDefinition.DagNode> nodes = List.of(
                node("dg_1", "datagen_input", in),
                node("oracle_out_1", "oracle_output", out));

        String sql = service.translate(dag("oracle-output", 1, nodes,
                List.of(edge("e1", "dg_1", "oracle_out_1"))));

        assertTrue(sql.contains("CREATE TABLE oracle_out_1"));
        assertTrue(sql.contains("'table-name' = 'DATAFLOW.TARGET_TABLE'"));
        assertTrue(sql.contains("'driver' = 'oracle.jdbc.OracleDriver'"));
        assertTrue(sql.contains("INSERT INTO oracle_out_1 SELECT * FROM dg_1;"));
        verify(oracleTableCreator).ensureTable(anyMap(), anyString());
    }

    @Test
    void hdfsInputToOutput_generatesFilesystemTablesAndPropagatesSchema() {
        Map<String, Object> in = new HashMap<>();
        in.put("path", "hdfs://localhost:9000/data/students.csv");
        in.put("delimiter", ",");
        // 源文件无表头：按声明类型读取，schema 原样传播
        in.put("hasHeader", false);
        in.put("fieldsConfig", "[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]");
        Map<String, Object> out = new HashMap<>();
        out.put("path", "hdfs://localhost:9000/output/result");
        out.put("delimiter", "|");
        List<DagDefinition.DagNode> nodes = List.of(
                node("hdfs-input-1", "hdfs_input", in),
                node("hdfs-output-1", "hdfs_output", out));
        List<DagDefinition.DagEdge> edges = List.of(edge("e1", "hdfs-input-1", "hdfs-output-1"));

        String sql = service.translate(dag("hdfs", 1, nodes, edges));

        assertTrue(sql.contains("CREATE TABLE hdfs_input_1"), "HDFS 输入表名应安全转换: " + sql);
        assertTrue(sql.contains("'path' = 'hdfs://localhost:9000/data/students.csv'"), "应保留 HDFS 输入路径");
        assertTrue(sql.contains("CREATE TABLE hdfs_output_1"), "应生成 HDFS 输出表");
        assertTrue(sql.contains("`id` INT"), "输入 schema 应传播到输出表: " + sql);
        assertTrue(sql.contains("'csv.delimiter' = '|'"), "应使用输出分隔符");
        assertTrue(sql.contains("INSERT INTO hdfs_output_1 SELECT * FROM hdfs_input_1;"), "应生成 HDFS 写入语句: " + sql);
        assertTrue(!sql.contains("WHERE"), "无表头时不应生成表头过滤条件");
    }

    @Test
    void hdfsInputWithHeader_readsAsStringAndFiltersHeaderRow() {
        Map<String, Object> in = new HashMap<>();
        in.put("path", "hdfs://localhost:9000/data/with_header.csv");
        in.put("delimiter", ",");
        in.put("hasHeader", true);
        in.put("fieldsConfig", "[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]");
        Map<String, Object> out = new HashMap<>();
        out.put("path", "hdfs://localhost:9000/output/result2");
        out.put("delimiter", ",");
        List<DagDefinition.DagNode> nodes = List.of(
                node("hdfs-input-2", "hdfs_input", in),
                node("hdfs-output-2", "hdfs_output", out));
        List<DagDefinition.DagEdge> edges = List.of(edge("e1", "hdfs-input-2", "hdfs-output-2"));

        String sql = service.translate(dag("hdfs-header", 1, nodes, edges));

        // 表头行首列为字段名文本，必须全列按 STRING 读，否则整行解析失败被丢弃
        assertTrue(sql.contains("`id` STRING"), "跳表头时输入列应为 STRING: " + sql);
        assertTrue(sql.contains("WHERE `id` <> 'id'"), "应生成表头行过滤条件: " + sql);
        assertTrue(sql.contains("`id` STRING"), "下游 schema 应同步为 STRING，避免 sink 类型校验失败: " + sql);
    }

    @Test
    void hdfsInputWithoutFieldsConfig_throws() {
        Map<String, Object> in = new HashMap<>();
        in.put("path", "hdfs://localhost:9000/data/students.csv");
        List<DagDefinition.DagNode> nodes = List.of(node("hdfs_1", "hdfs_input", in));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> service.translate(dag("hdfs-invalid", 1, nodes, List.of())));

        assertTrue(ex.getMessage().contains("fieldsConfig"), "异常信息应指出缺少字段定义: " + ex.getMessage());
    }

    @Test
    void missingCsvFile_throws() {
        Map<String, Object> in = new HashMap<>();
        in.put("path", "D:\\不存在的目录\\missing.csv");
        List<DagDefinition.DagNode> nodes = List.of(
                node("csv_input_1", "csv_input", in),
                node("csv_output_1", "csv_output", csvOutputParams("x.csv")));
        List<DagDefinition.DagEdge> edges = List.of(edge("e1", "csv_input_1", "csv_output_1"));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> service.translate(dag("missing", 1, nodes, edges)));
        assertTrue(ex.getMessage().contains("CSV 输入文件不存在"), "异常信息应明确: " + ex.getMessage());
    }

    @Test
    void xmlJsonNode_injectsUdfFunctions() {
        Map<String, Object> transform = new HashMap<>();
        transform.put("direction", "xml2json");
        transform.put("sourceField", "sale_id");
        transform.put("targetField", "result");
        List<DagDefinition.DagNode> nodes = List.of(
                node("csv_input_1", "csv_input", csvInputParams()),
                node("xj_1", "xml_json", transform),
                node("csv_output_1", "csv_output", csvOutputParams("xj.csv")));
        List<DagDefinition.DagEdge> edges = List.of(
                edge("e1", "csv_input_1", "xj_1"),
                edge("e2", "xj_1", "csv_output_1"));

        String sql = service.translate(dag("xmljson", 1, nodes, edges));

        assertTrue(sql.contains("CREATE FUNCTION IF NOT EXISTS xml2json"), "应注入 xml2json UDF");
        assertTrue(sql.contains("CREATE FUNCTION IF NOT EXISTS json2xml"), "应注入 json2xml UDF");
        assertTrue(sql.contains("INSERT INTO csv_output_1\nSELECT *, xml2json(`sale_id`) AS `result` FROM csv_input_1;"),
                "应生成完整可执行 INSERT SELECT: " + sql);
    }

    @Test
    void redisLookup_callsUdfWithConnectionAndPrefixedKey() {
        Map<String, Object> transform = new HashMap<>();
        transform.put("host", "redis");
        transform.put("port", 6379);
        transform.put("password", "p'ass");
        transform.put("keyField", "sale_id");
        transform.put("keyPrefix", "sale:");
        transform.put("targetField", "redis_value");
        List<DagDefinition.DagNode> nodes = List.of(
                node("csv_input_1", "csv_input", csvInputParams()),
                node("redis_1", "redis_lookup", transform),
                node("csv_output_1", "csv_output", csvOutputParams("redis.csv")));
        List<DagDefinition.DagEdge> edges = List.of(
                edge("e1", "csv_input_1", "redis_1"),
                edge("e2", "redis_1", "csv_output_1"));

        String sql = service.translate(dag("redis-lookup", 1, nodes, edges));

        assertTrue(sql.contains("CREATE FUNCTION IF NOT EXISTS redis_lookup"), "应注册 Redis UDF: " + sql);
        assertTrue(sql.contains("redis_lookup('redis', '6379', 'p''ass', CONCAT('sale:', CAST(`sale_id` AS STRING))) AS `redis_value`"),
                "SELECT 必须实际调用 Redis UDF，而不是只输出 key: " + sql);
        assertTrue(sql.contains("`redis_value` STRING"), "输出 schema 应包含 Redis 富化字段: " + sql);
    }

    @Test
    void redisLookup_invalidPort_throws() {
        Map<String, Object> transform = new HashMap<>();
        transform.put("host", "redis");
        transform.put("port", 70000);
        transform.put("keyField", "sale_id");
        transform.put("targetField", "redis_value");
        List<DagDefinition.DagNode> nodes = List.of(
                node("csv_input_1", "csv_input", csvInputParams()),
                node("redis_1", "redis_lookup", transform),
                node("csv_output_1", "csv_output", csvOutputParams("redis-invalid.csv")));
        List<DagDefinition.DagEdge> edges = List.of(
                edge("e1", "csv_input_1", "redis_1"),
                edge("e2", "redis_1", "csv_output_1"));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> service.translate(dag("redis-invalid", 1, nodes, edges)));
        assertTrue(ex.getMessage().contains("1-65535"));
    }

    @Test
    void unknownControlType_throws() {
        when(controlService.findByType("nope")).thenThrow(new RuntimeException("Control not found: type=nope"));
        List<DagDefinition.DagNode> nodes = List.of(
                node("n_1", "nope", new HashMap<>()),
                node("out_1", "csv_output", csvOutputParams("n.csv")));
        List<DagDefinition.DagEdge> edges = List.of(edge("e1", "n_1", "out_1"));

        assertThrows(RuntimeException.class, () -> service.translate(dag("unknown", 1, nodes, edges)));
    }
}
