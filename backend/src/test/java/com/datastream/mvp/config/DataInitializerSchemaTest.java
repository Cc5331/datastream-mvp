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
}
