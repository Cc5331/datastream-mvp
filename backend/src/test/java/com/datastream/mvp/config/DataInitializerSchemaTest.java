package com.datastream.mvp.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 控件 paramSchema 必须能被前端 JSON.parse，且输出类控件要暴露建表策略。
 */
class DataInitializerSchemaTest {

    private String invoke(String methodName) throws Exception {
        Method method = DataInitializer.class.getDeclaredMethod(methodName);
        method.setAccessible(true);
        // 绕过 @RequiredArgsConstructor 的依赖参数：schema 生成不依赖任何注入字段
        sun.misc.Unsafe unsafe = null;
        java.lang.reflect.Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        unsafe = (sun.misc.Unsafe) field.get(null);
        DataInitializer instance = (DataInitializer) unsafe.allocateInstance(DataInitializer.class);
        return (String) method.invoke(instance);
    }

    @Test
    void mysqlOutputSchemaIsValidJsonAndExposesCreateTablePolicy() throws Exception {
        String schema = invoke("mysqlOutputParamSchema");
        JsonNode root = new ObjectMapper().readTree(schema);

        JsonNode policy = root.path("properties").path("createTablePolicy");
        assertEquals("string", policy.path("type").asText(), "createTablePolicy 应为字符串枚举");
        assertEquals("CREATE_IF_MISSING", policy.path("default").asText(), "默认应自动建表");
        boolean hasCreate = false;
        for (JsonNode option : policy.path("enum")) {
            if ("CREATE_IF_MISSING".equals(option.asText())) hasCreate = true;
        }
        assertTrue(hasCreate, "枚举应包含 CREATE_IF_MISSING");
        assertEquals("CREATE_IF_MISSING", root.path("properties").path("createTablePolicy").path("default").asText());
        assertEquals("url", root.path("required").get(0).asText(), "原有 required 应保留");
    }

    @Test
    void mysqlInputSchemaStaysWithoutCreateTablePolicy() throws Exception {
        String schema = invoke("mysqlParamSchema");
        JsonNode root = new ObjectMapper().readTree(schema);
        assertTrue(root.path("properties").path("createTablePolicy").isMissingNode(),
                "输入控件不应出现建表策略");
    }

    @Test
    void fieldConcatSchemaDeclaresFieldsAsCommaSeparatedString() throws Exception {
        // fields 声明为 array 时前端控件会把值存成 JSON 数组，翻译层易生成 CONCAT([a, b])
        String source = new String(java.nio.file.Files.readAllBytes(
                java.nio.file.Paths.get("src/main/java/com/datastream/mvp/config/DataInitializer.java")),
                java.nio.charset.StandardCharsets.UTF_8);
        int idx = source.indexOf("createControl(\"field_concat\"");
        String block = source.substring(idx, source.indexOf("\"1.0.0\"", idx));
        assertTrue(block.contains("\\\"fields\\\":{\\\"type\\\":\\\"string\\\""), "fields 应声明为 string: " + block);
        assertTrue(block.contains("输入字段（逗号分隔）"), "应给出中文提示");
    }

    /**
     * 字段拼接控件（转换类）必须落在 transform 分组、带中文名与可发现的参数提示，
     * 否则画布「转换控件」区域要么看不到它、要么拖出来不知道填什么。
     */
    @Test
    void fieldConcatControlIsDiscoverableInTransformGroup() throws Exception {
        String source = new String(java.nio.file.Files.readAllBytes(
                java.nio.file.Paths.get("src/main/java/com/datastream/mvp/config/DataInitializer.java")),
                java.nio.charset.StandardCharsets.UTF_8);
        int idx = source.indexOf("createControl(\"field_concat\"");
        assertTrue(idx > 0, "应注册 field_concat 控件");
        String block = source.substring(idx, source.indexOf("\"1.0.0\"", idx));
        assertTrue(block.contains("\"字段拼接\""), "控件名应为中文「字段拼接」: " + block);
        assertTrue(block.contains("\"transform\""), "应归属 transform（转换控件区域）: " + block);
        assertTrue(block.contains("placeholder"), "fields 应带 placeholder 提示字段名写法: " + block);
    }

    /** 字段拼接的参数 schema 必须是合法 JSON，且 fields 带默认值（拖拽建节点时用它初始化 params）。 */
    @Test
    void fieldConcatSchemaIsValidJsonWithDefaults() throws Exception {
        JsonNode root = new ObjectMapper().readTree(fieldConcatParamSchema());
        JsonNode fields = root.path("properties").path("fields");
        assertEquals("string", fields.path("type").asText(), "fields 应为逗号分隔字符串");
        assertEquals("field1,field2", fields.path("default").asText(), "fields 应带默认值");
        assertEquals("concat_field", root.path("properties").path("newFieldName").path("default").asText());
        assertTrue(root.path("required").toString().contains("fields"), "fields 应为必填");
    }

    /**
     * 抽出 field_concat 的 paramSchema（Java 字符串字面量 → 原样 JSON）。
     * 用「左花括号 + 逐个转义双引号配对」定位结尾：相邻字符串参数里的 \" 不会被误当成结束引号。
     */
    private String fieldConcatParamSchema() throws Exception {
        String source = new String(java.nio.file.Files.readAllBytes(
                java.nio.file.Paths.get("src/main/java/com/datastream/mvp/config/DataInitializer.java")),
                java.nio.charset.StandardCharsets.UTF_8);
        int idx = source.indexOf("createControl(\"field_concat\"");
        assertTrue(idx > 0, "应注册 field_concat 控件");
        int jsonStart = source.indexOf('{', idx);
        assertTrue(jsonStart > 0, "应能定位 paramSchema 起始花括号");
        StringBuilder json = new StringBuilder();
        for (int i = jsonStart; i < source.length(); i++) {
            char ch = source.charAt(i);
            if (ch == '\\' && i + 1 < source.length()) {
                json.append(source.charAt(++i));
                continue;
            }
            if (ch == '"') break;
            json.append(ch);
        }
        return json.toString();
    }
}
