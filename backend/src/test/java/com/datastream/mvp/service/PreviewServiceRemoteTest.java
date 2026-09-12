package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class PreviewServiceRemoteTest {

    private PreviewService service;

    @BeforeEach
    void setUp() {
        service = new PreviewService(new ObjectMapper(), mock(XmlPreprocessor.class));
        ReflectionTestUtils.setField(service, "allowedJdbcHosts", "localhost,postgres,oracle,mysql");
        ReflectionTestUtils.setField(service, "allowedHdfsAuthorities", "localhost:9000,namenode:9000");
        ReflectionTestUtils.setField(service, "defaultPostgresUsername", "postgres");
        ReflectionTestUtils.setField(service, "defaultPostgresPassword", "secret");
        ReflectionTestUtils.setField(service, "defaultOracleUsername", "dataflow");
        ReflectionTestUtils.setField(service, "defaultOraclePassword", "secret");
        ReflectionTestUtils.setField(service, "defaultMysqlUsername", "root");
        ReflectionTestUtils.setField(service, "defaultMysqlPassword", "secret");
    }

    @Test
    void previewNode_rejectsJdbcHostOutsideAllowlistBeforeConnecting() {
        JobDefinition job = job("{\"nodes\":[{\"id\":\"pg1\",\"type\":\"pg_input\",\"params\":{"
                + "\"url\":\"jdbc:postgresql://192.0.2.1:5432/dataflow\","
                + "\"schema\":\"public\",\"table\":\"students\"}}]}");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> service.previewNode(job, "pg1", 20));

        assertTrue(ex.getMessage().contains("白名单"));
    }

    @Test
    void previewNode_rejectsHdfsAuthorityOutsideAllowlistBeforeConnecting() {
        JobDefinition job = job("{\"nodes\":[{\"id\":\"h1\",\"type\":\"hdfs_input\",\"params\":{"
                + "\"path\":\"hdfs://192.0.2.2:9000/data/a.csv\",\"delimiter\":\",\","
                + "\"fieldsConfig\":\"[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"INT\\\"}]\"}}]}");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> service.previewNode(job, "h1", 20));

        assertTrue(ex.getMessage().contains("白名单"));
    }

    @Test
    void previewNode_rejectsNodeNotStoredInAuthorizedJob() {
        JobDefinition job = job("{\"nodes\":[]}");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> service.previewNode(job, "missing", 20));

        assertTrue(ex.getMessage().contains("不存在节点"));
    }

    private JobDefinition job(String dagJson) {
        JobDefinition job = new JobDefinition();
        job.setId(1L);
        job.setDagJson(dagJson);
        return job;
    }
}
