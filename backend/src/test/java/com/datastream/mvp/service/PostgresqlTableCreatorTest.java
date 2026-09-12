package com.datastream.mvp.service;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostgresqlTableCreatorTest {

    private final PostgresqlTableCreator creator = new PostgresqlTableCreator();

    @Test
    void qualifiedTable_acceptsSafeSchemaAndTable() {
        Map<String, Object> params = new HashMap<>();
        params.put("schema", "reporting");
        params.put("table", "daily_sales");

        assertEquals("reporting.daily_sales", creator.qualifiedTable(params));
    }

    @Test
    void qualifiedTable_rejectsSqlInjection() {
        Map<String, Object> params = new HashMap<>();
        params.put("schema", "public");
        params.put("table", "sales; DROP TABLE users");

        assertThrows(IllegalArgumentException.class, () -> creator.qualifiedTable(params));
    }

    @Test
    void qualifiedTable_rejectsEmbeddedSchemaInTable() {
        Map<String, Object> params = new HashMap<>();
        params.put("schema", "public");
        params.put("table", "other.sales");

        assertThrows(IllegalArgumentException.class, () -> creator.qualifiedTable(params));
    }
}
