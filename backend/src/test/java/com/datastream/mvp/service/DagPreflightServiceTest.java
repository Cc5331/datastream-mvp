package com.datastream.mvp.service;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DagPreflightServiceTest {
    @Test
    void validatesRequiredEnumGraphAndParallelismWithoutSideEffects() {
        JobService jobs = mock(JobService.class);
        ControlRegistryService controls = mock(ControlRegistryService.class);
        CurrentUser user = new CurrentUser(2L, "u", "U", "OPERATOR");
        JobDefinition job = new JobDefinition();
        job.setParallelism(1);
        job.setDagJson("{\"parallelism\":129,\"nodes\":[{\"id\":\"in\",\"type\":\"source\",\"params\":{}},{\"id\":\"out\",\"type\":\"sink\",\"params\":{\"mode\":\"bad\"}}],\"edges\":[{\"source\":\"in\",\"target\":\"out\"},{\"source\":\"in\",\"target\":\"out\"}]}");
        when(jobs.findByIdForUser(1L, user)).thenReturn(job);
        when(controls.findAll()).thenReturn(List.of(control("source", "input", "{\"type\":\"object\",\"required\":[\"path\"],\"properties\":{\"path\":{\"type\":\"string\"}}}"), control("sink", "output", "{\"type\":\"object\",\"properties\":{\"mode\":{\"type\":\"string\",\"enum\":[\"append\"]}}}")));

        Map<String, Object> result = new DagPreflightService(jobs, controls, new ObjectMapper()).validate(1L, user);

        assertEquals(false, result.get("valid"));
        String errors = result.get("errors").toString();
        assertTrue(errors.contains("并行度"));
        assertTrue(errors.contains("缺少必填参数"));
        assertTrue(errors.contains("枚举"));
        assertTrue(errors.contains("重复边"));
        verify(jobs).findByIdForUser(1L, user);
        verifyNoMoreInteractions(jobs);
    }

    private ControlRegistry control(String type, String category, String schema) {
        ControlRegistry c = new ControlRegistry(); c.setType(type); c.setCategory(category); c.setParamSchema(schema); c.setEnabled(true); return c;
    }
}
