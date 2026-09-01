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
import static org.mockito.Mockito.mock;
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
    private ExcelPreprocessor excelPreprocessor;
    private XmlPreprocessor xmlPreprocessor;
    private JsonPreprocessor jsonPreprocessor;
    private ParquetPreprocessor parquetPreprocessor;
    private DagTranslationService service;

    @BeforeEach
    void setUp() {
        controlService = mock(ControlRegistryService.class);
        mysqlTableCreator = mock(MysqlTableCreator.class);
        excelPreprocessor = mock(ExcelPreprocessor.class);
        xmlPreprocessor = mock(XmlPreprocessor.class);
        jsonPreprocessor = mock(JsonPreprocessor.class);
        parquetPreprocessor = mock(ParquetPreprocessor.class);
        service = new DagTranslationService(controlService, new ObjectMapper(),
                excelPreprocessor, mysqlTableCreator, xmlPreprocessor, jsonPreprocessor, parquetPreprocessor);
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
        when(controlService.findByType("field_concat")).thenReturn(control("field_concat", "transform",
                "SELECT *, CONCAT(${fields}) AS `${newFieldName}` FROM ${id}"));
        when(controlService.findByType("xml_json")).thenReturn(control("xml_json", "transform",
                "SELECT *, ${direction}(`${sourceField}`) AS `${targetField}` FROM ${id}"));
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
    void unknownControlType_throws() {
        when(controlService.findByType("nope")).thenThrow(new RuntimeException("Control not found: type=nope"));
        List<DagDefinition.DagNode> nodes = List.of(
                node("n_1", "nope", new HashMap<>()),
                node("out_1", "csv_output", csvOutputParams("n.csv")));
        List<DagDefinition.DagEdge> edges = List.of(edge("e1", "n_1", "out_1"));

        assertThrows(RuntimeException.class, () -> service.translate(dag("unknown", 1, nodes, edges)));
    }
}
