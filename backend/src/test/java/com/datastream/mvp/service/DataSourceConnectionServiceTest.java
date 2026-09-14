package com.datastream.mvp.service;

import com.datastream.mvp.dto.DataSourceResponse;
import com.datastream.mvp.dto.DataSourceSaveRequest;
import com.datastream.mvp.model.DataSourceConnection;
import com.datastream.mvp.model.DataSourceType;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.DataSourceConnectionRepository;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.ResponseStatusException;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DataSourceConnectionServiceTest {
    @TempDir Path tempDir;
    private DataSourceConnectionRepository repository;
    private JobDefinitionRepository jobRepository;
    private DataSourceConnectionService service;
    private final CurrentUser owner = new CurrentUser(7L, "owner", "Owner", "OPERATOR");
    private final CurrentUser other = new CurrentUser(8L, "other", "Other", "OPERATOR");
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final AtomicLong ids = new AtomicLong(1);

    @BeforeEach
    void setUp() {
        repository = mock(DataSourceConnectionRepository.class);
        jobRepository = mock(JobDefinitionRepository.class);
        DataSourceSecretCipher cipher = new DataSourceSecretCipher(tempDir.resolve("key").toString());
        service = new DataSourceConnectionService(repository, jobRepository, cipher,
                mock(DataSourceConnectionTester.class), mapper);
        ReflectionTestUtils.setField(service, "allowedHosts", "localhost,127.0.0.1,mysql,postgres,oracle,kafka,redis,namenode");
        when(repository.saveAndFlush(any())).thenAnswer(invocation -> {
            DataSourceConnection entity = invocation.getArgument(0);
            if (entity.getId() == null) entity.setId(ids.getAndIncrement());
            if (entity.getVersion() == null) entity.setVersion(0L);
            if (entity.getCreatedAt() == null) entity.setCreatedAt(LocalDateTime.now());
            entity.setUpdatedAt(LocalDateTime.now());
            return entity;
        });
    }

    @Test
    void createForcesCurrentOwnerAndResponseContainsNoSecret() throws Exception {
        DataSourceResponse response = service.create(request("main", credentials("alice", "password"), false, null), owner);
        ArgumentCaptor<DataSourceConnection> captor = ArgumentCaptor.forClass(DataSourceConnection.class);
        verify(repository).saveAndFlush(captor.capture());

        assertEquals(owner.id(), captor.getValue().getOwnerId());
        assertEquals(owner.displayName(), captor.getValue().getOwnerName());
        assertTrue(response.credentialConfigured());
        String json = mapper.writeValueAsString(response);
        assertFalse(json.contains("password"));
        assertFalse(json.contains("alice"));
        assertFalse(json.contains("encryptedCredentials"));
    }

    @Test
    void nonAdminCannotReadAnotherOwnersConnection() {
        DataSourceConnection entity = entity(3L, owner.id(), "main", "cipher", true);
        when(repository.findById(3L)).thenReturn(Optional.of(entity));
        ResponseStatusException error = assertThrows(ResponseStatusException.class, () -> service.findById(3L, other));
        assertEquals(403, error.getStatusCode().value());
    }

    @Test
    void listIsOwnerScopedForNonAdminAndGlobalForAdmin() {
        service.findAll(owner);
        verify(repository).findByOwnerIdOrderByUpdatedAtDesc(owner.id());
        service.findAll(new CurrentUser(1L, "admin", "Admin", "ADMIN"));
        verify(repository).findAllByOrderByUpdatedAtDesc();
    }

    @Test
    void blankCredentialsPreserveStoredCiphertextAndClearIsExplicit() {
        DataSourceConnection entity = entity(3L, owner.id(), "main", "stored-cipher", true);
        when(repository.findById(3L)).thenReturn(Optional.of(entity));

        service.update(3L, request("main", Map.of("username", " ", "password", ""), false, 0L), owner);
        assertEquals("stored-cipher", entity.getEncryptedCredentials());
        assertTrue(entity.isCredentialConfigured());

        service.update(3L, request("main", Map.of(), true, 0L), owner);
        assertNull(entity.getEncryptedCredentials());
        assertFalse(entity.isCredentialConfigured());
    }

    @Test
    void duplicateNameReturnsConflict() {
        when(repository.existsByOwnerIdAndNameIgnoreCase(owner.id(), "main")).thenReturn(true);
        ResponseStatusException error = assertThrows(ResponseStatusException.class,
                () -> service.create(request("main", Map.of(), false, null), owner));
        assertEquals(409, error.getStatusCode().value());
    }

    @Test
    void referencedConnectionCannotBeDeletedWithoutScanningOtherOwnersJobs() {
        DataSourceConnection entity = entity(3L, owner.id(), "main", null, false);
        when(repository.findById(3L)).thenReturn(Optional.of(entity));
        JobDefinition job = new JobDefinition();
        job.setDagJson("{\"nodes\":[{\"params\":{\"dataSourceId\":3}}]}");
        when(jobRepository.findByOwnerIdOrderByUpdatedAtDesc(owner.id())).thenReturn(List.of(job));

        ResponseStatusException error = assertThrows(ResponseStatusException.class, () -> service.delete(3L, owner));
        assertEquals(409, error.getStatusCode().value());
        verify(jobRepository, never()).findAll();
        verify(repository, never()).delete(any());
    }

    @Test
    void rejectsUnknownConfigAndNonWhitelistedHost() {
        DataSourceSaveRequest unknown = new DataSourceSaveRequest("x", DataSourceType.REDIS, "", true,
                Map.of("host", "redis", "port", 6379, "database", 0, "ssl", false, "url", "secret"), Map.of(), false, null);
        assertEquals(400, assertThrows(ResponseStatusException.class, () -> service.create(unknown, owner)).getStatusCode().value());
        DataSourceSaveRequest remote = new DataSourceSaveRequest("x", DataSourceType.REDIS, "", true,
                Map.of("host", "example.com", "port", 6379, "database", 0, "ssl", false), Map.of(), false, null);
        assertEquals(400, assertThrows(ResponseStatusException.class, () -> service.create(remote, owner)).getStatusCode().value());
    }

    private DataSourceSaveRequest request(String name, Map<String, Object> credentials, boolean clear, Long version) {
        return new DataSourceSaveRequest(name, DataSourceType.REDIS, "description", true,
                Map.of("host", "redis", "port", 6379, "database", 0, "ssl", false), credentials, clear, version);
    }

    private Map<String, Object> credentials(String username, String password) {
        return Map.of("username", username, "password", password);
    }

    private DataSourceConnection entity(Long id, Long ownerId, String name, String encrypted, boolean configured) {
        DataSourceConnection entity = new DataSourceConnection();
        entity.setId(id);
        entity.setOwnerId(ownerId);
        entity.setOwnerName("Owner");
        entity.setName(name);
        entity.setType(DataSourceType.REDIS);
        entity.setDescription("");
        entity.setEnabled(true);
        entity.setConfigJson("{\"host\":\"redis\",\"port\":6379,\"database\":0,\"ssl\":false}");
        entity.setEncryptedCredentials(encrypted);
        entity.setCredentialConfigured(configured);
        entity.setCreatedAt(LocalDateTime.now());
        entity.setUpdatedAt(LocalDateTime.now());
        entity.setVersion(0L);
        return entity;
    }
}
