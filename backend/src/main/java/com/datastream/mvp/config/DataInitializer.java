package com.datastream.mvp.config;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.model.AppUser;
import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.AppUserRepository;
import com.datastream.mvp.repository.ControlRegistryRepository;
import com.datastream.mvp.repository.JobDefinitionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final ControlRegistryRepository controlRepo;
    private final AppUserRepository userRepo;
    private final JobDefinitionRepository jobRepo;
    private final AlertRecordRepository alertRepo;
    private final PasswordEncoder passwordEncoder;

    @Value("${app.mysql.default-username:root}")
    private String defaultMysqlUsername;

    @Value("${app.security.bootstrap-users-enabled:false}")
    private boolean bootstrapUsersEnabled;

    @Value("${app.security.admin-password:}")
    private String adminPassword;

    @Value("${app.security.operator-password:}")
    private String operatorPassword;

    @Value("${app.security.viewer-password:}")
    private String viewerPassword;

    @Value("${app.mysql.default-password:}")
    private String defaultMysqlPassword;

    @Value("${app.mysql.default-url:jdbc:mysql://localhost:3306/flink_demo?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai}")
    private String defaultMysqlUrl;

    @Value("${app.postgres.default-url:jdbc:postgresql://localhost:5432/dataflow}")
    private String defaultPostgresUrl;

    @Value("${app.oracle.default-url:jdbc:oracle:thin:@localhost:1521/FREEPDB1}")
    private String defaultOracleUrl;

    // 控件默认路径根目录：本机用相对路径（application.yml 默认 ../data 等），
    // 容器由 compose 注入 /data、/test-resources、/output。种子时统一转成绝对路径，
    // 避免默认值写死成某一环境的路径（Windows 盘符在容器内不存在，反之亦然）。
    @Value("${app.paths.data-root:../data}")
    private String dataRoot;

    @Value("${app.paths.test-resources-root:../test-resources}")
    private String testResourcesRoot;

    @Value("${app.storage.output-root:../output}")
    private String outputRoot;

    @Override
    public void run(String... args) {
        seedBuiltInControls();
        seedUsers();
        backfillJobOwners();
        backfillAlertOwners();
    }

    /** 本次启动由代码注册的内置控件类型（用于清理"已下线"的内置控件） */
    private final java.util.Set<String> builtInTypes = new java.util.LinkedHashSet<>();

    /**
     * 内置控件播种：**按 type upsert，绝不 deleteAll**。
     *
     * 历史实现是「count>0 就 deleteAll 再重建」，而插件加载器是 @PostConstruct（早于本 CommandLineRunner），
     * 于是插件注册的控件每次启动都被清空——插件功能名存实亡。现在：
     * - 已存在的内置控件原地更新（保留管理员设置的 enabled 与 createdAt，避免重启把启停状态重置）；
     * - 只删除「jarPath=built-in 且已从代码中移除」的陈旧行，插件行（jarPath=jar 名）永不删除。
     */
    void seedBuiltInControls() {
        builtInTypes.clear();
        log.info("Seeding built-in controls (upsert by type)...");

        // Datagen
        createControl("datagen_input", "Datagen", "input",
                "Generate test data",
                "{\"type\":\"object\",\"properties\":{\"rowsPerSecond\":{\"type\":\"number\",\"title\":\"Rows/s\",\"default\":10}},\"required\":[]}",
                "CUSTOM_DATAGEN",
                "1.0.0", "built-in");

        // CSV Input
        createControl("csv_input", "CSV Input", "input",
                "Read CSV",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"文件路径\",\"default\":\"" + jsonPath(testPath("data/sales.csv")) + "\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"},\"hasHeader\":{\"type\":\"boolean\",\"title\":\"包含表头\",\"default\":true}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = ',',\n  'csv.ignore-parse-errors' = 'true'\n);",
                "1.0.0", "built-in");

        // CSV Output
        createControl("csv_output", "CSV Output", "output",
                "Write CSV",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Path\",\"default\":\"" + jsonPath(outPath("output.csv")) + "\"},\"delimiter\":{\"type\":\"string\",\"title\":\"Delimiter\",\"default\":\",\"},\"sheetName\":{\"type\":\"string\",\"title\":\"工作表名（默认 Data，多输出写同一文件时可区分 sheet）\",\"default\":\"Data\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // Excel Input
        createControl("excel_input", "Excel Input", "input",
                "Read Excel (xlsx)",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Path\",\"default\":\"" + jsonPath(testPath("data/sample_data.xlsx")) + "\"},\"delimiter\":{\"type\":\"string\",\"title\":\"Delimiter\",\"default\":\",\"},\"hasHeader\":{\"type\":\"boolean\",\"title\":\"First row is header\",\"default\":true},\"sheetName\":{\"type\":\"string\",\"title\":\"工作表名（留空取第一个）\",\"default\":\"\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // Excel Output
        createControl("excel_output", "Excel Output", "output",
                "Excel (CSV->xlsx)",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Excel path\",\"default\":\"" + jsonPath(outPath("output.xlsx")) + "\"},\"delimiter\":{\"type\":\"string\",\"title\":\"Delimiter\",\"default\":\",\"},\"sheetName\":{\"type\":\"string\",\"title\":\"工作表名（默认 Data，多输出写同一文件时可区分 sheet）\",\"default\":\"Data\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // Parquet Input
        createControl("parquet_input", "Parquet Input", "input",
                "Read Parquet",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Parquet 文件路径\",\"default\":\"" + jsonPath(outPath("input.parquet")) + "\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // Parquet Output
        createControl("parquet_output", "Parquet Output", "output",
                "Parquet (CSV->parquet)",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Parquet 文件路径\",\"default\":\"" + jsonPath(outPath("output.parquet")) + "\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // MySQL Input
        createControl("mysql_input", "MySQL Input", "input",
                "Read MySQL",
                mysqlParamSchema(),
                "",
                "1.0.0", "built-in");

        // PostgreSQL Input
        createControl("pg_input", "PostgreSQL Input", "input",
                "Read PostgreSQL (JDBC 自动推导字段)",
                jdbcParamSchema(defaultPostgresUrl, "postgres", "POSTGRES_PASSWORD", "postgres"),
                "",
                "1.0.0", "built-in");

        // PostgreSQL Output
        createControl("pg_output", "PostgreSQL Output", "output",
                "Write PostgreSQL（可选安全自动建表）",
                jdbcOutputParamSchema(defaultPostgresUrl, "public", "postgres", "POSTGRES"),
                "",
                "1.0.0", "built-in");

        // Oracle Input
        createControl("oracle_input", "Oracle Input", "input",
                "Read Oracle (JDBC 自动推导字段)",
                "{\"type\":\"object\",\"properties\":{\"url\":{\"type\":\"string\",\"title\":\"JDBC URL\",\"default\":\"" + defaultOracleUrl + "\"},\"schema\":{\"type\":\"string\",\"title\":\"Schema\",\"default\":\"DATAFLOW\"},\"table\":{\"type\":\"string\",\"title\":\"Table\",\"default\":\"SOURCE_TABLE\"},\"username\":{\"type\":\"string\",\"title\":\"User\",\"default\":\"${ORACLE_USERNAME}\"},\"password\":{\"type\":\"string\",\"title\":\"Password\",\"default\":\"${ORACLE_PASSWORD}\"}},\"required\":[\"url\",\"table\"]}",
                "",
                "1.0.0", "built-in");

        // Oracle Output
        createControl("oracle_output", "Oracle Output", "output",
                "Write Oracle（可选安全自动建表）",
                jdbcOutputParamSchema(defaultOracleUrl, "DATAFLOW", "dataflow", "ORACLE"),
                "",
                "1.0.0", "built-in");

        // Redis 字段富化（transform：字段A → GET keyPrefix+值 → 扩充 targetField）
        createControl("redis_lookup", "Redis 富化", "transform",
                "根据字段A查询 Redis 扩充新字段（GET keyPrefix+值）",
                "{\"type\":\"object\",\"properties\":{\"host\":{\"type\":\"string\",\"title\":\"Redis 地址\",\"default\":\"localhost\"},\"port\":{\"type\":\"number\",\"title\":\"端口\",\"default\":6379},\"password\":{\"type\":\"string\",\"title\":\"密码（留空无密码）\",\"default\":\"redis123\"},\"keyField\":{\"type\":\"string\",\"title\":\"字段A（值作 Redis key）\",\"default\":\"id\"},\"keyPrefix\":{\"type\":\"string\",\"title\":\"key 前缀（如 user: → GET user:1）\",\"default\":\"\"},\"targetField\":{\"type\":\"string\",\"title\":\"扩充字段名\",\"default\":\"extra_info\"}},\"required\":[\"keyField\",\"targetField\"]}",
                "SELECT *, redis_lookup('${host}', '${port}', '${password}', `${keyField}`) AS `${targetField}` FROM ${id}",
                "1.0.0", "built-in");

        // HDFS Input（filesystem connector，fieldsConfig 声明 schema）
        createControl("hdfs_input", "HDFS 输入", "input",
                "读取 HDFS 上的 CSV 文件（hdfs://，字段需声明）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"HDFS 路径\",\"default\":\"hdfs://localhost:9000/data/input.csv\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"},\"hasHeader\":{\"type\":\"boolean\",\"title\":\"首行是表头（读入后自动过滤）\",\"default\":true},\"fieldsConfig\":{\"type\":\"string\",\"title\":\"字段定义（JSON 数组，HDFS 无法自动推导表头）\",\"default\":\"[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"INT\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"STRING\\\"}]\"}},\"required\":[\"path\",\"fieldsConfig\"]}",
                "",
                "1.0.0", "built-in");

        // HDFS Output（filesystem connector，schema 沿用上游）
        createControl("hdfs_output", "HDFS 输出", "output",
                "将数据写出到 HDFS CSV 目录（hdfs://，目录内生成 part 文件）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"HDFS 输出目录\",\"default\":\"hdfs://localhost:9000/output/result\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // MySQL Output
        createControl("mysql_output", "MySQL Output", "output",
                "Write MySQL",
                mysqlOutputParamSchema(),
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'jdbc',\n  'url' = '${url}',\n  'table-name' = '${table}',\n  'username' = '${username}',\n  'password' = '${password}',\n  'driver' = 'com.mysql.cj.jdbc.Driver'\n);",
                "1.0.0", "built-in");

        // Kafka Input
        createControl("kafka_input", "Kafka Input", "input",
                "Read Kafka",
                "{\"type\":\"object\",\"properties\":{\"topic\":{\"type\":\"string\",\"title\":\"Topic\",\"default\":\"test-topic\"},\"bootstrapServers\":{\"type\":\"string\",\"title\":\"Bootstrap\",\"default\":\"localhost:9092\"},\"fieldsConfig\":{\"type\":\"string\",\"title\":\"Fields config (JSON)\",\"default\":\"[{\\\"name\\\":\\\"key\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"value\\\",\\\"type\\\":\\\"STRING\\\"}]\"},\"autoStop\":{\"type\":\"boolean\",\"title\":\"体验模式：消费完自动停止\",\"default\":false},\"stopAfterSeconds\":{\"type\":\"number\",\"title\":\"自动停止延迟（秒）\",\"default\":30}},\"required\":[\"topic\",\"bootstrapServers\"]}",
                "",
                "1.0.0", "built-in");
        // Kafka Output
        createControl("kafka_output", "Kafka Output", "output",
                "Write Kafka",
                "{\"type\":\"object\",\"properties\":{\"topic\":{\"type\":\"string\",\"title\":\"Topic\",\"default\":\"test-topic\"},\"bootstrapServers\":{\"type\":\"string\",\"title\":\"Bootstrap\",\"default\":\"localhost:9092\"},\"fieldsConfig\":{\"type\":\"string\",\"title\":\"字段定义（JSON 数组）\",\"default\":\"[{\\\"name\\\":\\\"key\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"value\\\",\\\"type\\\":\\\"STRING\\\"}]\"}},\"required\":[\"topic\",\"bootstrapServers\"]}",
                "",
                "1.0.0", "built-in");
        // Field Concat (transform)
        createControl("field_concat", "Field Concat", "transform",
                "Concat multiple fields into one",
                "{\"type\":\"object\",\"properties\":{\"fields\":{\"type\":\"string\",\"title\":\"输入字段（逗号分隔）\",\"default\":\"field1,field2\"},\"separator\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"},\"newFieldName\":{\"type\":\"string\",\"title\":\"新字段名\",\"default\":\"concat_field\"}},\"required\":[\"fields\",\"newFieldName\"]}",
                "SELECT *, CONCAT(${fields}) AS ${newFieldName} FROM ${id}",
                "1.0.0", "built-in");

        // XML <-> JSON (transform, schema pass-through placeholder)
        createControl("xml_json", "XML<->JSON", "transform",
                "XML and JSON format conversion (schema pass-through)",
                "{\"type\":\"object\",\"properties\":{\"direction\":{\"type\":\"string\",\"title\":\"Direction\",\"enum\":[\"xml2json\",\"json2xml\"],\"default\":\"xml2json\"},\"sourceField\":{\"type\":\"string\",\"title\":\"Source field\",\"default\":\"payload\"},\"targetField\":{\"type\":\"string\",\"title\":\"Target field\",\"default\":\"result\"}},\"required\":[\"direction\",\"sourceField\"]}",
                "SELECT *, ${direction}(`${sourceField}`) AS `${targetField}` FROM ${id}",
                "1.0.0", "built-in");

        // 去重（transform）
        createControl("dedupe", "去重", "transform",
                "按字段去重（留空=整行去重）",
                "{\"type\":\"object\",\"properties\":{\"dedupeFields\":{\"type\":\"string\",\"title\":\"去重字段（逗号分隔，留空=整行去重）\",\"default\":\"\"}},\"required\":[]}",
                "",
                "1.0.0", "built-in");
        // 空值校验（transform）
        createControl("validate", "空值校验", "transform",
                "丢弃指定字段为空的行",
                "{\"type\":\"object\",\"properties\":{\"checkFields\":{\"type\":\"string\",\"title\":\"必填字段（逗号分隔）\",\"default\":\"id\"},\"ignoreEmpty\":{\"type\":\"boolean\",\"title\":\"空字符串也算空值\",\"default\":true}},\"required\":[\"checkFields\"]}",
                "",
                "1.0.0", "built-in");
        // 条件路由（transform：第一条出边=匹配，其余出边=不匹配）
        createControl("route", "条件路由", "transform",
                "按字段值分流：第一条出边=匹配，其余=不匹配",
                "{\"type\":\"object\",\"properties\":{\"routeField\":{\"type\":\"string\",\"title\":\"路由字段\",\"default\":\"status\"},\"matchValues\":{\"type\":\"string\",\"title\":\"匹配值（逗号分隔）\",\"default\":\"SUCCESS\"}},\"required\":[\"routeField\",\"matchValues\"]}",
                "",
                "1.0.0", "built-in");
        // 字段过滤
        createControl("field_filter", "字段过滤", "transform",
                "只保留指定字段",
                "{\"type\":\"object\",\"properties\":{\"fields\":{\"type\":\"string\",\"title\":\"保留字段（逗号分隔）\",\"default\":\"id,name\"}},\"required\":[\"fields\"]}",
                "",
                "1.0.0", "built-in");
        // 字段改名
        createControl("field_rename", "字段改名", "transform",
                "字段重命名",
                "{\"type\":\"object\",\"properties\":{\"mappings\":{\"type\":\"string\",\"title\":\"改名映射（old=new，逗号分隔）\",\"default\":\"id=userId\"}},\"required\":[\"mappings\"]}",
                "",
                "1.0.0", "built-in");
        // 行过滤
        createControl("row_filter", "行过滤", "transform",
                "按条件过滤行",
                "{\"type\":\"object\",\"properties\":{\"condition\":{\"type\":\"string\",\"title\":\"过滤条件（SQL WHERE 表达式）\",\"default\":\"age > 18\"}},\"required\":[\"condition\"]}",
                "SELECT * FROM ${id} WHERE ${params.condition}",
                "1.0.0", "built-in");
        // JSON 解析
        createControl("json_parse", "JSON 解析", "transform",
                "从 JSON 字段解析出多个字段",
                "{\"type\":\"object\",\"properties\":{\"sourceField\":{\"type\":\"string\",\"title\":\"JSON 源字段\",\"default\":\"payload\"},\"fieldsConfig\":{\"type\":\"string\",\"title\":\"解析字段（JSON 数组）\",\"default\":\"[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"INT\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"STRING\\\"}]\"}},\"required\":[\"sourceField\",\"fieldsConfig\"]}",
                "",
                "1.0.0", "built-in");
        // JSON 输入
        createControl("json_input", "JSON 输入", "input",
                "读取 JSON 文件（auto 自动识别 / lines 逐行 / array 数组）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"文件路径\",\"default\":\"" + jsonPath(testPath("data/sample.json")) + "\"},\"mode\":{\"type\":\"string\",\"title\":\"文件模式\",\"enum\":[\"auto\",\"lines\",\"array\"],\"default\":\"auto\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"},\"encoding\":{\"type\":\"string\",\"title\":\"文件编码\",\"default\":\"UTF-8\"},\"fieldsConfig\":{\"type\":\"string\",\"title\":\"字段定义（JSON 数组，lines 模式用）\",\"default\":\"[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"INT\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"STRING\\\"}]\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // JSON 输出
        createControl("json_output", "JSON 输出", "output",
                "将数据写入 JSON 文件（lines 逐行 / array 数组）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"输出路径（.json）\",\"default\":\"" + jsonPath(outPath("output.json")) + "\"},\"mode\":{\"type\":\"string\",\"title\":\"输出模式\",\"enum\":[\"lines\",\"array\"],\"default\":\"lines\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'json',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // XML Input
        createControl("xml_input", "XML 输入", "input",
                "读取 XML 文件（记录列表结构，嵌套子结构保留为 JSON）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"文件路径\",\"default\":\"" + jsonPath(testPath("data/orders.xml")) + "\"},\"rowTag\":{\"type\":\"string\",\"title\":\"行元素名（留空自动探测）\",\"default\":\"\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"},\"encoding\":{\"type\":\"string\",\"title\":\"文件编码\",\"default\":\"UTF-8\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // XML Output
        createControl("xml_output", "XML 输出", "output",
                "将数据写出为 XML 文件（rootTag/rowTag 包裹）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"输出路径（.xml）\",\"default\":\"" + jsonPath(outPath("output.xml")) + "\"},\"rootTag\":{\"type\":\"string\",\"title\":\"根元素名\",\"default\":\"root\"},\"rowTag\":{\"type\":\"string\",\"title\":\"行元素名\",\"default\":\"record\"},\"encoding\":{\"type\":\"string\",\"title\":\"输出编码\",\"default\":\"UTF-8\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        log.info("Built-in controls upserted: {} 个{}", builtInTypes.size(), removeStaleBuiltInControls());

        seedUsers();
        backfillJobOwners();
        backfillAlertOwners();
    }

    /**
     * 清理「曾经是内置、但已从代码移除」的控件行；插件控件（jarPath != built-in）一律保留。
     * @return 便于日志拼接的说明串
     */
    private String removeStaleBuiltInControls() {
        int removed = 0;
        int plugins = 0;
        for (ControlRegistry c : controlRepo.findAll()) {
            if (!"built-in".equals(c.getJarPath())) {
                plugins++;
                continue;
            }
            if (!builtInTypes.contains(c.getType())) {
                controlRepo.delete(c);
                removed++;
                log.info("Removed stale built-in control: {}", c.getType());
            }
        }
        return "（清理已下线内置控件 " + removed + " 个，保留插件控件 " + plugins + " 个）";
    }

    /**
     * 种子用户（仅首次启动创建；密码可用环境变量覆盖）
     */
    private void seedUsers() {
        if (userRepo.count() > 0) {
            log.info("Users already seeded ({}), skipping", userRepo.count());
            return;
        }
        if (!bootstrapUsersEnabled) {
            log.warn("No users exist and bootstrap user creation is disabled");
            return;
        }
        if (adminPassword.isBlank() || operatorPassword.isBlank() || viewerPassword.isBlank()) {
            throw new IllegalStateException("启用种子用户时必须配置 ADMIN_PASSWORD、OPERATOR_PASSWORD 和 VIEWER_PASSWORD");
        }
        createUser("admin", adminPassword, "管理员", AppUser.UserRole.ADMIN);
        createUser("operator", operatorPassword, "操作员", AppUser.UserRole.OPERATOR);
        createUser("viewer", viewerPassword, "观察员", AppUser.UserRole.VIEWER);
        log.info("Seeded default users: admin / operator / viewer");
    }

    private void createUser(String username, String rawPassword, String displayName, AppUser.UserRole role) {
        AppUser u = new AppUser();
        u.setUsername(username);
        u.setPasswordHash(passwordEncoder.encode(rawPassword));
        u.setDisplayName(displayName);
        u.setRole(role);
        u.setEnabled(true);
        u.setCreatedAt(LocalDateTime.now());
        userRepo.save(u);
    }

    /**
     * 历史作业归属回填：ownerId 为空的一律归到第一个管理员
     */
    private void backfillJobOwners() {
        AppUser admin = userRepo.findByUsername("admin").orElse(null);
        if (admin == null) return;
        List<JobDefinition> jobs = jobRepo.findAll();
        int fixed = 0;
        for (JobDefinition j : jobs) {
            if (j.getOwnerId() == null) {
                j.setOwnerId(admin.getId());
                j.setOwnerName(admin.getDisplayName() == null ? admin.getUsername() : admin.getDisplayName());
                jobRepo.save(j);
                fixed++;
            }
        }
        if (fixed > 0) log.info("Backfilled owner for {} historical jobs to admin", fixed);
    }

    /**
     * 历史告警归属回填：ownerId 为空且 jobId 可反查到作业的告警，按作业 ownerId 补齐；
 * 作业也被删除的（jobId 悬空）回填给第一个管理员，保证非 admin 用户与 admin 的可见性一致。
     */
    private void backfillAlertOwners() {
        AppUser admin = userRepo.findByUsername("admin").orElse(null);
        List<AlertRecord> orphans = alertRepo.findAll().stream()
                .filter(a -> a.getOwnerId() == null && a.getJobId() != null)
                .toList();
        if (orphans.isEmpty()) return;
        int fixed = 0;
        for (AlertRecord a : orphans) {
            Long ownerId = jobRepo.findById(a.getJobId())
                    .map(JobDefinition::getOwnerId)
                    .orElseGet(() -> admin == null ? null : admin.getId());
            if (ownerId == null) continue;
            a.setOwnerId(ownerId);
            alertRepo.save(a);
            fixed++;
        }
        if (fixed > 0) log.info("Backfilled owner for {} historical alert records", fixed);
    }

    /**
     * MySQL 输入/输出控件参数 schema（用户名/密码默认值来自环境变量，避免硬编码密钥）
     */
    private String mysqlParamSchema() {
        // 默认值使用占位符（不预填真实密码），提交时由后端解析为环境变量 MYSQL_USERNAME / MYSQL_PASSWORD
        // URL 由 app.mysql.default-url 注入：本机 localhost:3306，容器内 mysql:3306
        return "{\"type\":\"object\",\"properties\":{\"url\":{\"type\":\"string\",\"title\":\"JDBC URL\",\"default\":\"" + defaultMysqlUrl + "\"},\"table\":{\"type\":\"string\",\"title\":\"Table\",\"default\":\"output_table\"},\"username\":{\"type\":\"string\",\"title\":\"User\",\"default\":\"${MYSQL_USERNAME}\"},\"password\":{\"type\":\"string\",\"title\":\"Password\",\"default\":\"${MYSQL_PASSWORD}\"}},\"required\":[\"url\",\"table\",\"username\"]}";
    }

    /**
     * MySQL 输出参数 schema：在输入 schema 基础上增加建表策略，
     * 默认 CREATE_IF_MISSING，使 AI 生成或手工搭建的落库作业无需预先建表。
     */
    private String mysqlOutputParamSchema() {
        String base = mysqlParamSchema();
        return base.substring(0, base.lastIndexOf("},"))
                + ",\"createTablePolicy\":{\"type\":\"string\",\"title\":\"建表策略\",\"enum\":[\"CREATE_IF_MISSING\",\"FAIL_IF_MISSING\",\"VALIDATE_EXISTING\"],\"default\":\"CREATE_IF_MISSING\"}"
                + ",\"writeMode\":{\"type\":\"string\",\"title\":\"写入模式\",\"enum\":[\"append\"],\"default\":\"append\"}"
                + "},"
                + "\"required\":[\"url\",\"table\",\"username\"]}";
    }

    /**
     * 通用 JDBC 输入参数 schema（PostgreSQL / Oracle 共用，默认值来自环境变量占位符）
     */
    private String jdbcParamSchema(String defaultUrl, String defaultUser, String passwordEnv, String defaultPassword) {
        return "{\"type\":\"object\",\"properties\":{\"url\":{\"type\":\"string\",\"title\":\"JDBC URL\",\"default\":\"" + defaultUrl + "\"},\"table\":{\"type\":\"string\",\"title\":\"Table\",\"default\":\"source_table\"},\"username\":{\"type\":\"string\",\"title\":\"User\",\"default\":\"${" + passwordEnv.replace("PASSWORD", "USERNAME") + ":" + defaultUser + "}\"},\"password\":{\"type\":\"string\",\"title\":\"Password\",\"default\":\"${" + passwordEnv + ":" + defaultPassword + "}\"}},\"required\":[\"url\",\"table\"]}";
    }

    private String jdbcOutputParamSchema(String defaultUrl, String defaultSchema, String defaultUser, String envPrefix) {
        return "{\"type\":\"object\",\"properties\":{"
                + "\"url\":{\"type\":\"string\",\"title\":\"JDBC URL\",\"default\":\"" + defaultUrl + "\"},"
                + "\"schema\":{\"type\":\"string\",\"title\":\"Schema\",\"default\":\"" + defaultSchema + "\"},"
                + "\"table\":{\"type\":\"string\",\"title\":\"Table\",\"default\":\"target_table\"},"
                + "\"username\":{\"type\":\"string\",\"title\":\"User\",\"default\":\"${" + envPrefix + "_USERNAME}\"},"
                + "\"password\":{\"type\":\"string\",\"title\":\"Password\",\"default\":\"${" + envPrefix + "_PASSWORD}\"},"
                + "\"createTablePolicy\":{\"type\":\"string\",\"title\":\"建表策略\",\"enum\":[\"FAIL_IF_MISSING\",\"CREATE_IF_MISSING\",\"VALIDATE_EXISTING\"],\"default\":\"FAIL_IF_MISSING\"},"
                + "\"writeMode\":{\"type\":\"string\",\"title\":\"写入模式\",\"enum\":[\"append\"],\"default\":\"append\"},"
                + "\"batchSize\":{\"type\":\"number\",\"title\":\"批量条数\",\"default\":1000}},"
                + "\"required\":[\"url\",\"schema\",\"table\"]}";
    }

    /**
     * 内置控件 upsert：同 type 已存在则原地更新（保留 enabled/createdAt），否则新建。
     * 这样重复启动不会产生重复行，也不会把管理员设置的启停状态重置。
     */
    private void createControl(String type, String name, String category, String description,
                                String paramSchema, String flinkTemplate, String version, String jarPath) {
        builtInTypes.add(type);
        ControlRegistry c = controlRepo.findByType(type).orElse(null);
        boolean isNew = (c == null);
        if (isNew) {
            c = new ControlRegistry();
            c.setType(type);
            c.setEnabled(true);
            c.setCreatedAt(LocalDateTime.now());
        }
        c.setName(name);
        c.setCategory(category);
        c.setDescription(description);
        c.setParamSchema(paramSchema);
        c.setFlinkTemplate(flinkTemplate);
        c.setVersion(version);
        c.setJarPath(jarPath);
        c.setUpdatedAt(LocalDateTime.now());
        controlRepo.save(c);
    }

    // ---------- 默认路径生成 ----------

    /** 输出文件默认路径（宿主机/容器各自解析为绝对路径） */
    private String outPath(String fileName) {
        return absoluteUnder(outputRoot, fileName);
    }

    /** 测试数据默认路径（test-resources 下的相对路径） */
    private String testPath(String relative) {
        return absoluteUnder(testResourcesRoot, relative);
    }

    private String absoluteUnder(String root, String relative) {
        return java.nio.file.Paths.get(root).toAbsolutePath().normalize().resolve(relative).toString();
    }

    /** 转义为可嵌入 JSON 字符串字面量的路径（Windows 反斜杠需双写） */
    private String jsonPath(String absolutePath) {
        return absolutePath.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
