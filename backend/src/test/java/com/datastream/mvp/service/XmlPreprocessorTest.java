package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * XmlPreprocessor 单测：XML → CSV 转换（自动探测行元素）
 */
class XmlPreprocessorTest {

    // CI 兼容：从模块目录定位项目根（test-resources 位于仓库根）
    private static final String ORDERS_XML = java.nio.file.Paths.get(System.getProperty("user.dir")).getParent()
            .resolve("test-resources/data/orders.xml").toString();
    private final XmlPreprocessor pre = new XmlPreprocessor(new ObjectMapper());

    @Test
    void convertOrdersXml_toCsv() throws Exception {
        String csv = pre.convertToCsv(ORDERS_XML, "", ",", "UTF-8");
        assertNotNull(csv, "XML 转换应返回 CSV 路径");
        String content = Files.readString(Path.of(csv), StandardCharsets.UTF_8);
        assertTrue(content.trim().length() > 0, "CSV 不应为空");
        assertTrue(content.contains(","), "CSV 应包含分隔符列: " + content.substring(0, Math.min(200, content.length())));
    }
}
