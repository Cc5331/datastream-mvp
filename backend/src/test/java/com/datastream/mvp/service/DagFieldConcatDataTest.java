package com.datastream.mvp.service;

import com.datastream.mvp.dag.DagDefinition;
import com.datastream.mvp.model.ControlRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 字段拼接（field_concat）测试数据回归。
 *
 * <p>把 test-resources/dags 下三个「字段拼接」场景的真实 DAG JSON 文件喂给
 * {@link DagTranslationService#translate}，断言生成的是可执行 SQL：
 * <ul>
 *   <li>02-csv-concat-csv.json：CSV → 字段拼接 → CSV（含空值/长文本/字段带分隔符的边界数据）</li>
 *   <li>02b-csv-concat-types.json：CSV → 字段拼接 → CSV（数值列 + 日期列，验证逐个 CAST 到 STRING）</li>
 *   <li>02c-csv-concat-slim-validate.json：CSV → 字段拼接 → 空值校验(checkFields) → CSV</li>
 * </ul>
 * 这些 DAG 同时是「人工跑一次作业、把输出与 concat_expected_*.csv 对比」的对照数据，
 * 所以单测必须证明它们本身是合法输入（字段名与源 CSV 表头一致、参数形态是当前契约的逗号串写法）。
 */
class DagFieldConcatDataTest {

    private static final Path PROJECT_ROOT = Paths.get(System.getProperty("user.dir")).getParent();
    private static final Path DAG_DIR = PROJECT_ROOT.resolve("test-resources/dags");
    private static final Path DATA_DIR = PROJECT_ROOT.resolve("test-resources/data");
    private static final String SOURCE_CSV = DATA_DIR.resolve("concat_source.csv").toString();
    private static final String SLIM_CSV = DATA_DIR.resolve("concat_source_slim.csv").toString();

    private DagTranslationService service;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        ControlRegistryService controlService = mock(ControlRegistryService.class);
        MysqlTableCreator mysqlTableCreator = mock(MysqlTableCreator.class);
        PostgresqlTableCreator postgresqlTableCreator = mock(PostgresqlTableCreator.class);
        OracleTableCreator oracleTableCreator = mock(OracleTableCreator.class);
        ExcelPreprocessor excelPreprocessor = mock(ExcelPreprocessor.class);
        XmlPreprocessor xmlPreprocessor = mock(XmlPreprocessor.class);
        JsonPreprocessor jsonPreprocessor = mock(JsonPreprocessor.class);
        ParquetPreprocessor parquetPreprocessor = mock(ParquetPreprocessor.class);
        mapper = new ObjectMapper();
        service = new DagTranslationService(controlService, mapper,
                excelPreprocessor, mysqlTableCreator, postgresqlTableCreator, oracleTableCreator,
                xmlPreprocessor, jsonPreprocessor, parquetPreprocessor);
        when(postgresqlTableCreator.qualifiedTable(anyMap())).thenReturn("public.t");
        when(oracleTableCreator.qualifiedTable(anyMap())).thenReturn("DATAFLOW.T");

        when(controlService.findByType("csv_input")).thenReturn(control("csv_input", "input",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = ',',\n  'csv.ignore-parse-errors' = 'true'\n);"));
        when(controlService.findByType("csv_output")).thenReturn(control("csv_output", "output",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);"));
        when(controlService.findByType("field_concat")).thenReturn(control("field_concat", "transform",
                "SELECT *, CONCAT(${fields}) AS `${newFieldName}` FROM ${id}"));
        when(controlService.findByType("row_filter")).thenReturn(control("row_filter", "transform",
                "SELECT * FROM ${id} WHERE ${params.condition}"));
        when(controlService.findByType("validate")).thenReturn(control("validate", "transform", ""));
    }

    private ControlRegistry control(String type, String category, String template) {
        ControlRegistry c = new ControlRegistry();
        c.setType(type);
        c.setCategory(category);
        c.setFlinkTemplate(template);
        return c;
    }

    /** 真读 test-resources/dags 下的 DAG JSON（证明测试数据文件本身可被后端直接消费）。 */
    private DagDefinition loadDag(String fileName) {
        try {
            String json = Files.readString(DAG_DIR.resolve(fileName), StandardCharsets.UTF_8);
            return resolveDagPaths(mapper.readValue(json, DagDefinition.class));
        } catch (Exception e) {
            throw new IllegalStateException("读取测试 DAG 失败: " + fileName, e);
        }
    }

    /**
     * 测试数据里的路径是相对仓库根的（前端导入时按运行目录解析）。
     * 单测的 user.dir 是 backend/，所以这里把相对路径补成相对仓库根的绝对路径，
     * 与「后端从仓库根启动」时的解析结果一致。
     */
    private DagDefinition resolveDagPaths(DagDefinition dag) {
        if (dag.getNodes() == null) return dag;
        for (DagDefinition.DagNode n : dag.getNodes()) {
            if (n.getParams() == null || n.getParams().get("path") == null) continue;
            String raw = n.getParams().get("path").toString().trim();
            if (raw.isEmpty() || Paths.get(raw).isAbsolute()) continue;
            n.getParams().put("path", PROJECT_ROOT.resolve(raw).toString());
        }
        return dag;
    }

    /** 源 CSV 表头（Flink CSV connector 从这里推 schema，拼接字段名必须与之逐字一致）。 */
    private String sourceHeader(String csvFile) {
        try {
            String first = Files.readAllLines(DATA_DIR.resolve(csvFile), StandardCharsets.UTF_8).get(0);
            return first.replace("\uFEFF", "").trim();
        } catch (Exception e) {
            throw new IllegalStateException("读取源 CSV 失败: " + csvFile, e);
        }
    }

    @Test
    void csvConcatCsv_generatesExecutableConcatAndPropagatesNewColumn() {
        DagDefinition dag = loadDag("02-csv-concat-csv.json");
        assertTrue(dag.getNodes().stream().anyMatch(n -> "field_concat".equals(n.getType())),
                "02-csv-concat-csv.json 应含字段拼接节点");

        Path input = Paths.get(dag.getNodes().get(0).getParams().get("path").toString());
        assertTrue(Files.isRegularFile(input),
                "DAG 引用的源 CSV 应存在: " + input);

        String sql = service.translate(dag);

        assertEqualsContains(sql, "CONCAT(CAST(`category` AS STRING), ',', CAST(`channel` AS STRING)) AS `category_channel`");
        assertEqualsContains(sql, "INSERT INTO csv_output_1");
        assertEqualsContains(sql, "FROM csv_input_1;");
        assertFalse(sql.contains("CONCAT(["),
                "不得把 JSON 数组原样拼进 SQL（历史上产生过 SqlParseException）: " + sql);
        assertFalse(sql.contains("["),
                "生成 SQL 中不应出现方括号: " + sql);

        // schema 传播：拼接列必须出现在输出表 DDL 里，否则落盘表头/列数对不上（期望文件是 源12列+1 列）
        String outputDdl = sql.substring(sql.indexOf("CREATE TABLE csv_output_1"));
        assertEqualsContains(outputDdl.substring(0, outputDdl.indexOf(");")), "category_channel STRING");

        // 每个拼接字段都必须在源 CSV 表头里真实存在，否则作业提交后才会报「字段不存在」
        String header = sourceHeader("concat_source.csv");
        for (String f : new String[]{"category", "channel"}) {
            assertTrue(java.util.Arrays.asList(header.split(",")).contains(f),
                    "拼接字段 " + f + " 必须在源 CSV 表头中: " + header);
        }
        assertTrue(SOURCE_CSV.endsWith("concat_source.csv"), "测试数据路径自检");
    }

    @Test
    void csvConcatCsv_castsNonStringColumnsBeforeConcat() {
        // 跨类型拼接：amount 是数值列、join_day 是日期串，两列都必须先 CAST 到 STRING
        DagDefinition dag = loadDag("02b-csv-concat-types.json");

        String sql = service.translate(dag);

        assertEqualsContains(sql, "CONCAT(CAST(`amount` AS STRING), '|', CAST(`join_day` AS STRING)) AS `amount_day`");
        assertEqualsContains(sql, "INSERT INTO csv_output_1");
        assertEqualsContains(sql, "FROM csv_input_1;");
        assertEqualsContains(sql, "amount_day STRING");

        String header = sourceHeader("concat_source.csv");
        for (String f : new String[]{"amount", "join_day"}) {
            assertTrue(java.util.Arrays.asList(header.split(",")).contains(f),
                    "拼接字段 " + f + " 必须在源 CSV 表头中: " + header);
        }
    }

    @Test
    void csvConcatValidateCsv_keepsNewColumnAndUsesCheckFieldsParam() {
        DagDefinition dag = loadDag("02c-csv-concat-slim-validate.json");

        String sql = service.translate(dag);

        assertEqualsContains(sql, "CONCAT(CAST(`src_name` AS STRING), ' | ', CAST(`channel` AS STRING)) AS `name_channel`");
        // 分隔符两侧带空格：SQL 里必须是单个字符串字面量，不能被拆成两个参数
        assertFalse(sql.contains("CAST(`src_name` AS STRING), ' ', '|'"),
                "含空格的分隔符不应被拆开: " + sql);
        // 链中间节点物化为视图：拼接在前（物化），校验在末尾（内联进 INSERT），
        // 所以落盘仍然带着拼接列 name_channel
        assertEqualsContains(sql, "CREATE TEMPORARY VIEW fc_1 AS");
        assertEqualsContains(sql, "SELECT * FROM fc_1 WHERE `src_name` IS NOT NULL AND `src_name` <> '' AND `channel` IS NOT NULL AND `channel` <> ''");
        assertFalse(sql.contains("CREATE TEMPORARY VIEW validate_1"),
                "末尾 transform 应内联进 INSERT，不额外物化视图: " + sql);
        assertEqualsContains(sql, "WHERE `src_name` IS NOT NULL AND `src_name` <> '' AND `channel` IS NOT NULL AND `channel` <> ''");
        assertEqualsContains(sql, "name_channel STRING");

        Path input = Paths.get(dag.getNodes().get(0).getParams().get("path").toString());
        assertTrue(Files.isRegularFile(input), "DAG 引用的 slim 源 CSV 应存在: " + input);
        assertTrue(SLIM_CSV.endsWith("concat_source_slim.csv"), "测试数据路径自检");
    }

    /** 期望输出文件必须与源数据同批存在，人工对比时才不会拿到过期的旧结果。 */
    @Test
    void expectedOutputsExistForManualComparison() {
        for (String f : new String[]{"concat_expected_1.csv", "concat_expected_2.csv", "concat_expected_3.csv"}) {
            Path p = DATA_DIR.resolve(f);
            assertTrue(Files.isRegularFile(p), "期望输出文件缺失: " + p);
            try {
                String header = Files.readAllLines(p, StandardCharsets.UTF_8).get(0);
                String src = f.endsWith("_3.csv") ? sourceHeader("concat_source_slim.csv")
                        : sourceHeader("concat_source.csv");
                String newCol = f.endsWith("_1.csv") ? "category_channel"
                        : (f.endsWith("_2.csv") ? "amount_day" : "name_channel");
                assertTrue(header.startsWith(src),
                        f + " 的表头应以源表头开头（源全部列 + 拼接列）: " + header);
                assertTrue(header.endsWith(newCol), f + " 的表头最后一列应是 " + newCol + ": " + header);
            } catch (Exception e) {
                throw new IllegalStateException("读取期望输出失败: " + f, e);
            }
        }
    }

    private void assertEqualsContains(String actual, String expectedFragment) {
        assertNotNull(actual, "SQL 不应为空");
        assertTrue(actual.contains(expectedFragment),
                "生成 SQL 应包含:\n  " + expectedFragment + "\n实际 SQL:\n" + actual);
    }
}
