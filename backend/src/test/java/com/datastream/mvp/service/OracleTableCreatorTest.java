package com.datastream.mvp.service;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OracleTableCreatorTest {

    private final OracleTableCreator creator = new OracleTableCreator();

    @Test
    void qualifiedTable_normalizesIdentifiersToUppercase() {
        Map<String, Object> params = new HashMap<>();
        params.put("schema", "dataflow");
        params.put("table", "daily_sales");

        assertEquals("DATAFLOW.DAILY_SALES", creator.qualifiedTable(params));
    }

    @Test
    void qualifiedTable_rejectsSqlInjection() {
        Map<String, Object> params = new HashMap<>();
        params.put("schema", "DATAFLOW");
        params.put("table", "SALES; DROP TABLE USERS");

        assertThrows(IllegalArgumentException.class, () -> creator.qualifiedTable(params));
    }

    @Test
    void qualifiedTable_rejectsEmbeddedSchemaInTable() {
        Map<String, Object> params = new HashMap<>();
        params.put("schema", "DATAFLOW");
        params.put("table", "OTHER.SALES");

        assertThrows(IllegalArgumentException.class, () -> creator.qualifiedTable(params));
    }
}
