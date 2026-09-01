package com.datastream.mvp.config;

import com.datastream.mvp.model.AppUser;
import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
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
    private final PasswordEncoder passwordEncoder;

    @Value("${app.mysql.default-username:root}")
    private String defaultMysqlUsername;

    @Value("${app.security.admin-password:admin123}")
    private String adminPassword;

    @Value("${app.security.operator-password:operator123}")
    private String operatorPassword;

    @Value("${app.security.viewer-password:viewer123}")
    private String viewerPassword;

    @Value("${app.mysql.default-password:}")
    private String defaultMysqlPassword;

    @Override
    public void run(String... args) {
        if (controlRepo.count() > 0) {
            log.info("Control registry already initialized, clearing for re-seed...");
            controlRepo.deleteAll();
        }

        log.info("Seeding built-in controls...");

        // Datagen
        createControl("datagen_input", "Datagen", "input",
                "Generate test data",
                "{\"type\":\"object\",\"properties\":{\"rowsPerSecond\":{\"type\":\"number\",\"title\":\"Rows/s\",\"default\":10}},\"required\":[]}",
                "CUSTOM_DATAGEN",
                "1.0.0", "built-in");

        // CSV Input
        createControl("csv_input", "CSV Input", "input",
                "Read CSV",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"文件路径\",\"default\":\"/data/test.csv\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"},\"hasHeader\":{\"type\":\"boolean\",\"title\":\"包含表头\",\"default\":true}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = ',',\n  'csv.ignore-parse-errors' = 'true'\n);",
                "1.0.0", "built-in");

        // CSV Output
        createControl("csv_output", "CSV Output", "output",
                "Write CSV",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Path\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.csv\"},\"delimiter\":{\"type\":\"string\",\"title\":\"Delimiter\",\"default\":\",\"},\"sheetName\":{\"type\":\"string\",\"title\":\"工作表名（默认 Data，多输出写同一文件时可区分 sheet）\",\"default\":\"Data\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // Excel Input
        createControl("excel_input", "Excel Input", "input",
                "Read Excel (xlsx)",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Path\",\"default\":\"/data/test.xlsx\"},\"delimiter\":{\"type\":\"string\",\"title\":\"Delimiter\",\"default\":\",\"},\"hasHeader\":{\"type\":\"boolean\",\"title\":\"First row is header\",\"default\":true},\"sheetName\":{\"type\":\"string\",\"title\":\"工作表名（留空取第一个）\",\"default\":\"\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // Excel Output
        createControl("excel_output", "Excel Output", "output",
                "Excel (CSV->xlsx)",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Excel path\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.xlsx\"},\"delimiter\":{\"type\":\"string\",\"title\":\"Delimiter\",\"default\":\",\"},\"sheetName\":{\"type\":\"string\",\"title\":\"工作表名（默认 Data，多输出写同一文件时可区分 sheet）\",\"default\":\"Data\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // Parquet Input
        createControl("parquet_input", "Parquet Input", "input",
                "Read Parquet",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Parquet 文件路径\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\input.parquet\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // Parquet Output
        createControl("parquet_output", "Parquet Output", "output",
                "Parquet (CSV->parquet)",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Parquet 文件路径\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.parquet\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // MySQL Input
        createControl("mysql_input", "MySQL Input", "input",
                "Read MySQL",
                mysqlParamSchema(),
                "",
                "1.0.0", "built-in");

        // MySQL Output
        createControl("mysql_output", "MySQL Output", "output",
                "Write MySQL",
                mysqlParamSchema(),
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
                "{\"type\":\"object\",\"properties\":{\"fields\":{\"type\":\"array\",\"title\":\"Fields (comma separated)\",\"default\":\"field1,field2\"},\"separator\":{\"type\":\"string\",\"title\":\"Separator\",\"default\":\",\"},\"newFieldName\":{\"type\":\"string\",\"title\":\"New field name\",\"default\":\"concat_field\"}},\"required\":[\"fields\",\"newFieldName\"]}",
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
                "SELECT * FROM ${id} WHERE ${condition}",
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
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"文件路径\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\test-resources\\\\data\\\\sample.json\"},\"mode\":{\"type\":\"string\",\"title\":\"文件模式\",\"enum\":[\"auto\",\"lines\",\"array\"],\"default\":\"auto\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"},\"encoding\":{\"type\":\"string\",\"title\":\"文件编码\",\"default\":\"UTF-8\"},\"fieldsConfig\":{\"type\":\"string\",\"title\":\"字段定义（JSON 数组，lines 模式用）\",\"default\":\"[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"INT\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"STRING\\\"}]\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // JSON 输出
        createControl("json_output", "JSON 输出", "output",
                "将数据写入 JSON 文件（lines 逐行 / array 数组）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"输出路径（.json）\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.json\"},\"mode\":{\"type\":\"string\",\"title\":\"输出模式\",\"enum\":[\"lines\",\"array\"],\"default\":\"lines\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'json',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // XML Input
        createControl("xml_input", "XML 输入", "input",
                "读取 XML 文件（记录列表结构，嵌套子结构保留为 JSON）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"文件路径\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\test-resources\\\\data\\\\orders.xml\"},\"rowTag\":{\"type\":\"string\",\"title\":\"行元素名（留空自动探测）\",\"default\":\"\"},\"delimiter\":{\"type\":\"string\",\"title\":\"分隔符\",\"default\":\",\"},\"encoding\":{\"type\":\"string\",\"title\":\"文件编码\",\"default\":\"UTF-8\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // XML Output
        createControl("xml_output", "XML 输出", "output",
                "将数据写出为 XML 文件（rootTag/rowTag 包裹）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"输出路径（.xml）\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.xml\"},\"rootTag\":{\"type\":\"string\",\"title\":\"根元素名\",\"default\":\"root\"},\"rowTag\":{\"type\":\"string\",\"title\":\"行元素名\",\"default\":\"record\"},\"encoding\":{\"type\":\"string\",\"title\":\"输出编码\",\"default\":\"UTF-8\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        log.info("Seeded {} built-in controls", controlRepo.count());

        seedUsers();
        backfillJobOwners();
    }

    /**
     * 种子用户（仅首次启动创建；密码可用环境变量覆盖）
     */
    private void seedUsers() {
        if (userRepo.count() > 0) {
            log.info("Users already seeded ({}), skipping", userRepo.count());
            return;
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
     * MySQL 输入/输出控件参数 schema（用户名/密码默认值来自环境变量，避免硬编码密钥）
     */
    private String mysqlParamSchema() {
        // 默认值使用占位符（不预填真实密码），提交时由后端解析为环境变量 MYSQL_USERNAME / MYSQL_PASSWORD
        return "{\"type\":\"object\",\"properties\":{\"url\":{\"type\":\"string\",\"title\":\"JDBC URL\",\"default\":\"jdbc:mysql://localhost:3306/flink_demo?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai\"},\"table\":{\"type\":\"string\",\"title\":\"Table\",\"default\":\"output_table\"},\"username\":{\"type\":\"string\",\"title\":\"User\",\"default\":\"${MYSQL_USERNAME}\"},\"password\":{\"type\":\"string\",\"title\":\"Password\",\"default\":\"${MYSQL_PASSWORD}\"}},\"required\":[\"url\",\"table\",\"username\"]}";
    }

    private void createControl(String type, String name, String category, String description,
                                String paramSchema, String flinkTemplate, String version, String jarPath) {
        ControlRegistry c = new ControlRegistry();
        c.setType(type);
        c.setName(name);
        c.setCategory(category);
        c.setDescription(description);
        c.setParamSchema(paramSchema);
        c.setFlinkTemplate(flinkTemplate);
        c.setVersion(version);
        c.setJarPath(jarPath);
        c.setEnabled(true);
        c.setCreatedAt(LocalDateTime.now());
        c.setUpdatedAt(LocalDateTime.now());
        controlRepo.save(c);
    }
}
