package com.datastream.mvp.service;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MysqlTableCreatorTest {

    private final MysqlTableCreator creator = new MysqlTableCreator();

    private Map<String, Object> params(String policy) {
        Map<String, Object> params = new HashMap<>();
        params.put("url", "jdbc:mysql://localhost:3306/dataflow?useSSL=false");
        params.put("table", "ai_demo");
        params.put("username", "root");
        if (policy != null) params.put("createTablePolicy", policy);
        return params;
    }

    @Test
    void rejectsUnknownCreateTablePolicy() {
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> creator.ensureTable(params("DROP_EVERYTHING"), "`id` INT"));
        assertTrue(ex.getMessage().contains("createTablePolicy"), "应说明策略非法: " + ex.getMessage());
    }

    @Test
    void rejectsTableWithoutColumns() {
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> creator.ensureTable(params(null), ""));
        assertTrue(ex.getMessage().contains("字段定义"), "应提示缺少上游字段: " + ex.getMessage());
    }

    @Test
    void rejectsUrlWithoutDatabase() {
        Map<String, Object> params = params(null);
        params.put("url", "jdbc:mysql://localhost:3306");
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> creator.ensureTable(params, "`id` INT"));
        assertTrue(ex.getMessage().contains("数据库名"), "应提示 URL 缺少库名: " + ex.getMessage());
    }

    @Test
    void rejectsMissingTableName() {
        Map<String, Object> params = params(null);
        params.put("table", "  ");
        assertThrows(RuntimeException.class, () -> creator.ensureTable(params, "`id` INT"));
    }

    @Test
    void missingParamsIsRejected() {
        assertEquals("MySQL 输出缺少连接参数",
                assertThrows(RuntimeException.class, () -> creator.ensureTable(null, "`id` INT")).getMessage());
    }
}
