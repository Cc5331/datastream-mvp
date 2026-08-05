package com.datastream.mvp.config;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.repository.ControlRegistryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final ControlRegistryRepository controlRepo;

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
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Path\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.csv\"},\"delimiter\":{\"type\":\"string\",\"title\":\"Delimiter\",\"default\":\",\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // Excel Input
        createControl("excel_input", "Excel Input", "input",
                "Read Excel (xlsx)",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Path\",\"default\":\"/data/test.xlsx\"},\"delimiter\":{\"type\":\"string\",\"title\":\"Delimiter\",\"default\":\",\"},\"hasHeader\":{\"type\":\"boolean\",\"title\":\"First row is header\",\"default\":true}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");

        // Excel Output
        createControl("excel_output", "Excel Output", "output",
                "Excel (CSV->xlsx)",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"Excel path\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.xlsx\"},\"delimiter\":{\"type\":\"string\",\"title\":\"Delimiter\",\"default\":\",\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'csv',\n  'csv.delimiter' = '${delimiter}',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        // MySQL Input
        createControl("mysql_input", "MySQL Input", "input",
                "Read MySQL",
                "{\"type\":\"object\",\"properties\":{\"url\":{\"type\":\"string\",\"title\":\"JDBC URL\",\"default\":\"jdbc:mysql://localhost:3306/flink_demo\"},\"table\":{\"type\":\"string\",\"title\":\"Table\",\"default\":\"output_table\"},\"username\":{\"type\":\"string\",\"title\":\"User\",\"default\":\"root\"},\"password\":{\"type\":\"string\",\"title\":\"Password\",\"default\":\"YOUR_MYSQL_PASSWORD\"}},\"required\":[\"url\",\"table\",\"username\"]}",
                "",
                "1.0.0", "built-in");

        // MySQL Output
        createControl("mysql_output", "MySQL Output", "output",
                "Write MySQL",
                "{\"type\":\"object\",\"properties\":{\"url\":{\"type\":\"string\",\"title\":\"JDBC URL\",\"default\":\"jdbc:mysql://localhost:3306/flink_demo\"},\"table\":{\"type\":\"string\",\"title\":\"Table\",\"default\":\"output_table\"},\"username\":{\"type\":\"string\",\"title\":\"User\",\"default\":\"root\"},\"password\":{\"type\":\"string\",\"title\":\"Password\",\"default\":\"YOUR_MYSQL_PASSWORD\"}},\"required\":[\"url\",\"table\",\"username\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'jdbc',\n  'url' = '${url}',\n  'table-name' = '${table}',\n  'username' = '${username}',\n  'password' = '${password}',\n  'driver' = 'com.mysql.cj.jdbc.Driver'\n);",
                "1.0.0", "built-in");

        // Kafka Input
        createControl("kafka_input", "Kafka Input", "input",
                "Read Kafka",
                "{\"type\":\"object\",\"properties\":{\"topic\":{\"type\":\"string\",\"title\":\"Topic\",\"default\":\"test-topic\"},\"bootstrapServers\":{\"type\":\"string\",\"title\":\"Bootstrap\",\"default\":\"localhost:9092\"},\"fieldsConfig\":{\"type\":\"string\",\"title\":\"Fields config (JSON)\",\"default\":\"[{\\\"name\\\":\\\"key\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"value\\\",\\\"type\\\":\\\"STRING\\\"}]\"}},\"autoStop\":{\"type\":\"boolean\",\"title\":\"体验模式：消费完自动停止\",\"default\":false},\"stopAfterSeconds\":{\"type\":\"number\",\"title\":\"自动停止延迟（秒）\",\"default\":30},\"required\":[\"topic\",\"bootstrapServers\"]}",
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
                "读取 JSON 文件（JSON Lines）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"文件路径\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\test-resources\\\\data\\\\sample.json\"},\"fieldsConfig\":{\"type\":\"string\",\"title\":\"字段定义（JSON 数组）\",\"default\":\"[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"INT\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"STRING\\\"}]\"}},\"required\":[\"path\"]}",
                "",
                "1.0.0", "built-in");
        // JSON 输出
        createControl("json_output", "JSON 输出", "output",
                "将数据写入 JSON 文件（JSON Lines）",
                "{\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\",\"title\":\"输出路径（.json）\",\"default\":\"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.json\"}},\"required\":[\"path\"]}",
                "CREATE TABLE ${id} (\n  data STRING\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${path}',\n  'format' = 'json',\n  'sink.parallelism' = '1'\n);",
                "1.0.0", "built-in");

        log.info("Seeded {} built-in controls", controlRepo.count());
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
