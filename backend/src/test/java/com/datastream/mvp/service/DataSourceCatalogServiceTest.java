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

class DataSourceCatalogServiceTest {
    @Test
    void catalogUsesUserScopedJobsAndNeverReturnsCredentials() {
        ControlRegistryService controls = mock(ControlRegistryService.class);
        JobService jobs = mock(JobService.class);
        ControlRegistry connector = new ControlRegistry();
        connector.setType("mysql_input"); connector.setName("MySQL"); connector.setCategory("input"); connector.setEnabled(true);
        CurrentUser user = new CurrentUser(7L, "operator", "Operator", "OPERATOR");
        JobDefinition job = new JobDefinition();
        job.setId(1L); job.setName("owned");
        job.setDagJson("{\"nodes\":[{\"id\":\"db\",\"type\":\"mysql_input\",\"params\":{\"url\":\"jdbc:mysql://user:hidden@db/demo?password=query-secret\",\"table\":\"orders\",\"username\":\"root\",\"password\":\"secret\",\"token\":\"x\",\"apiKey\":\"y\"}}]}");
        when(controls.findAll()).thenReturn(List.of(connector));
        when(jobs.findAllForUser(user)).thenReturn(List.of(job));

        Map<String, Object> result = new DataSourceCatalogService(controls, jobs, new ObjectMapper()).catalog(user);

        String serialized = result.toString();
        assertTrue(serialized.contains("orders"));
        assertTrue(serialized.contains("enabled=true"));
        assertFalse(serialized.contains("secret"));
        assertFalse(serialized.contains("hidden"));
        assertFalse(serialized.contains("username"));
        assertFalse(serialized.contains("token"));
        assertFalse(serialized.contains("apiKey"));
        verify(jobs).findAllForUser(user);
        verify(jobs, never()).findAll();
    }
}
