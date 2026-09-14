package com.datastream.mvp.service;

import com.datastream.mvp.dto.DataSourceResponse;
import com.datastream.mvp.dto.DataSourceSaveRequest;
import com.datastream.mvp.dto.DataSourceTestResponse;
import com.datastream.mvp.model.DataSourceConnection;
import com.datastream.mvp.model.DataSourceType;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.DataSourceConnectionRepository;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
public class DataSourceConnectionService {
    private static final Pattern HOST = Pattern.compile("(?i)^(localhost|[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?|(?:\\d{1,3}\\.){3}\\d{1,3})$");
    private static final Pattern KAFKA_ENDPOINT = Pattern.compile("(?i)^(localhost|[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?|(?:\\d{1,3}\\.){3}\\d{1,3}):([1-9]\\d{0,4})$");
    private static final Set<String> JDBC_CONFIG = Set.of("host", "port", "database", "schema", "serviceName");
    private static final Set<String> KAFKA_CONFIG = Set.of("bootstrapServers", "securityProtocol", "saslMechanism");
    private static final Set<String> REDIS_CONFIG = Set.of("host", "port", "database", "ssl");
    private static final Set<String> HDFS_CONFIG = Set.of("uri", "user", "testPath");
    private static final Set<String> CREDENTIAL_FIELDS = Set.of("username", "password");

    private final DataSourceConnectionRepository repository;
    private final JobDefinitionRepository jobRepository;
    private final DataSourceSecretCipher cipher;
    private final DataSourceConnectionTester tester;
    private final ObjectMapper objectMapper;

    @Value("${app.data-source.allowed-hosts:${app.preview.allowed-jdbc-hosts:localhost,127.0.0.1,mysql,postgres,oracle,kafka,redis,namenode}}")
    private String allowedHosts;

    public List<DataSourceResponse> findAll(CurrentUser user) {
        requireUser(user);
        List<DataSourceConnection> entities = user.isAdmin()
                ? repository.findAllByOrderByUpdatedAtDesc()
                : repository.findByOwnerIdOrderByUpdatedAtDesc(user.id());
        return entities.stream().map(this::response).toList();
    }

    public DataSourceResponse findById(Long id, CurrentUser user) {
        return response(requireAccessible(id, user));
    }

    @Transactional
    public DataSourceResponse create(DataSourceSaveRequest request, CurrentUser user) {
        requireUser(user);
        requireSaveRequest(request);
        String name = normalizeName(request.name());
        if (repository.existsByOwnerIdAndNameIgnoreCase(user.id(), name)) conflict("同一所有者下数据源名称已存在");
        DataSourceConnection entity = new DataSourceConnection();
        entity.setName(name);
        entity.setType(requireType(request.type()));
        entity.setDescription(normalizeDescription(request.description()));
        entity.setEnabled(request.enabled() == null || request.enabled());
        entity.setOwnerId(user.id());
        entity.setOwnerName(ownerName(user));
        entity.setConfigJson(writeJson(normalizeConfig(entity.getType(), request.config())));
        applyCredentials(entity, request.credentials(), Boolean.TRUE.equals(request.clearCredentials()), false);
        return response(save(entity));
    }

    @Transactional
    public DataSourceResponse update(Long id, DataSourceSaveRequest request, CurrentUser user) {
        requireSaveRequest(request);
        DataSourceConnection entity = requireAccessible(id, user);
        if (request.version() == null || !Objects.equals(entity.getVersion(), request.version())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "数据源已被其他请求修改，请刷新后重试");
        }
        String name = normalizeName(request.name());
        if (repository.existsByOwnerIdAndNameIgnoreCaseAndIdNot(entity.getOwnerId(), name, id)) conflict("同一所有者下数据源名称已存在");
        DataSourceType type = requireType(request.type());
        entity.setName(name);
        entity.setType(type);
        entity.setDescription(normalizeDescription(request.description()));
        if (request.enabled() != null) entity.setEnabled(request.enabled());
        entity.setConfigJson(writeJson(normalizeConfig(type, request.config())));
        applyCredentials(entity, request.credentials(), Boolean.TRUE.equals(request.clearCredentials()), true);
        return response(save(entity));
    }

    @Transactional
    public void delete(Long id, CurrentUser user) {
        DataSourceConnection entity = requireAccessible(id, user);
        List<JobDefinition> visibleJobs = user.isAdmin() ? jobRepository.findAll() : jobRepository.findByOwnerIdOrderByUpdatedAtDesc(user.id());
        if (visibleJobs.stream().anyMatch(job -> references(job.getDagJson(), id))) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "数据源仍被作业引用，无法删除");
        }
        repository.delete(entity);
    }

    public DataSourceTestResponse testUnsaved(DataSourceSaveRequest request, CurrentUser user) {
        requireUser(user);
        if (request == null) badRequest("测试请求不能为空");
        DataSourceType type = requireType(request.type());
        return tester.test(type, normalizeConfig(type, request.config()), normalizeCredentials(request.credentials()));
    }

    public DataSourceTestResponse testSaved(Long id, DataSourceSaveRequest override, CurrentUser user) {
        DataSourceConnection entity = requireAccessible(id, user);
        DataSourceType type = entity.getType();
        if (override != null && override.type() != null && override.type() != type) badRequest("测试覆盖类型必须与已保存数据源一致");
        Map<String, Object> config = override == null || override.config() == null || override.config().isEmpty()
                ? readMap(entity.getConfigJson()) : normalizeConfig(type, override.config());
        Map<String, Object> credentials = override == null ? Map.of() : normalizeCredentials(override.credentials());
        if (credentials.isEmpty() && entity.isCredentialConfigured()) credentials = readCredentials(entity);
        return tester.test(type, config, credentials);
    }

    private DataSourceConnection requireAccessible(Long id, CurrentUser user) {
        requireUser(user);
        DataSourceConnection entity = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "数据源不存在"));
        if (!user.isAdmin() && !Objects.equals(entity.getOwnerId(), user.id())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "无权访问该数据源");
        }
        return entity;
    }

    private Map<String, Object> normalizeConfig(DataSourceType type, Map<String, Object> raw) {
        if (raw == null) badRequest("config 不能为空");
        Set<String> allowed = switch (type) {
            case MYSQL, POSTGRESQL, ORACLE -> JDBC_CONFIG;
            case KAFKA -> KAFKA_CONFIG;
            case REDIS -> REDIS_CONFIG;
            case HDFS -> HDFS_CONFIG;
        };
        rejectUnknown(raw, allowed, "config");
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        switch (type) {
            case MYSQL, POSTGRESQL, ORACLE -> normalizeJdbc(type, raw, result);
            case KAFKA -> normalizeKafka(raw, result);
            case REDIS -> normalizeRedis(raw, result);
            case HDFS -> normalizeHdfs(raw, result);
        }
        return Collections.unmodifiableMap(result);
    }

    private void normalizeJdbc(DataSourceType type, Map<String, Object> raw, Map<String, Object> out) {
        String host = host(raw, "host");
        assertAllowedHost(host);
        out.put("host", host);
        out.put("port", port(raw, "port"));
        out.put("database", requiredText(raw, "database", 128));
        optionalText(raw, out, "schema", 128);
        if (type == DataSourceType.ORACLE) out.put("serviceName", requiredText(raw, "serviceName", 128));
        else optionalText(raw, out, "serviceName", 128);
    }

    private void normalizeKafka(Map<String, Object> raw, Map<String, Object> out) {
        String servers = requiredText(raw, "bootstrapServers", 1024);
        List<String> normalized = new ArrayList<>();
        for (String endpoint : servers.split(",", -1)) {
            String value = endpoint.trim();
            java.util.regex.Matcher matcher = KAFKA_ENDPOINT.matcher(value);
            if (!matcher.matches()) badRequest("Kafka bootstrapServers 格式非法");
            int port = Integer.parseInt(matcher.group(2));
            if (port > 65535) badRequest("Kafka 端口非法");
            assertAllowedHost(value.substring(0, value.lastIndexOf(':')));
            normalized.add(value);
        }
        out.put("bootstrapServers", String.join(",", normalized));
        String protocol = text(raw, "securityProtocol", "PLAINTEXT").toUpperCase(Locale.ROOT);
        if (!Set.of("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL").contains(protocol)) badRequest("Kafka securityProtocol 非法");
        out.put("securityProtocol", protocol);
        String mechanism = text(raw, "saslMechanism", "").toUpperCase(Locale.ROOT);
        if (!mechanism.isEmpty() && !Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512").contains(mechanism)) badRequest("Kafka saslMechanism 非法");
        out.put("saslMechanism", mechanism);
    }

    private void normalizeRedis(Map<String, Object> raw, Map<String, Object> out) {
        String host = host(raw, "host");
        assertAllowedHost(host);
        out.put("host", host);
        out.put("port", port(raw, "port"));
        int database = integer(raw, "database", 0);
        if (database < 0 || database > 65535) badRequest("Redis database 非法");
        out.put("database", database);
        out.put("ssl", bool(raw, "ssl", false));
    }

    private void normalizeHdfs(Map<String, Object> raw, Map<String, Object> out) {
        String value = requiredText(raw, "uri", 1024);
        try {
            URI uri = URI.create(value);
            if (!"hdfs".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getPort() < 1 || uri.getPort() > 65535) {
                badRequest("HDFS uri 必须是有效的 hdfs://host:port 地址");
            }
            assertAllowedHost(uri.getHost());
            out.put("uri", uri.toString());
        } catch (IllegalArgumentException e) {
            badRequest("HDFS uri 非法");
        }
        optionalText(raw, out, "user", 128);
        String testPath = text(raw, "testPath", "/").trim();
        if (!testPath.startsWith("/") || testPath.contains("..")) badRequest("HDFS testPath 非法");
        out.put("testPath", testPath);
    }

    private void applyCredentials(DataSourceConnection entity, Map<String, Object> raw, boolean clear, boolean preserveBlank) {
        if (clear) {
            entity.setEncryptedCredentials(null);
            entity.setCredentialConfigured(false);
            return;
        }
        Map<String, Object> credentials = normalizeCredentials(raw);
        if (credentials.isEmpty() && preserveBlank) return;
        if (credentials.isEmpty()) {
            entity.setEncryptedCredentials(null);
            entity.setCredentialConfigured(false);
        } else {
            entity.setEncryptedCredentials(cipher.encrypt(writeJson(credentials)));
            entity.setCredentialConfigured(true);
        }
    }

    private Map<String, Object> normalizeCredentials(Map<String, Object> raw) {
        if (raw == null || raw.isEmpty()) return Map.of();
        rejectUnknown(raw, CREDENTIAL_FIELDS, "credentials");
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        for (String field : CREDENTIAL_FIELDS) {
            String value = text(raw, field, "");
            if (!value.isBlank()) result.put(field, value);
        }
        return result;
    }

    private DataSourceResponse response(DataSourceConnection entity) {
        return new DataSourceResponse(entity.getId(), entity.getName(), entity.getType(), entity.getDescription(),
                readMap(entity.getConfigJson()), entity.isCredentialConfigured(), entity.isEnabled(), entity.getOwnerId(),
                entity.getOwnerName(), entity.getCreatedAt(), entity.getUpdatedAt(), entity.getVersion());
    }

    private Map<String, Object> readCredentials(DataSourceConnection entity) {
        return entity.isCredentialConfigured() ? readMap(cipher.decrypt(entity.getEncryptedCredentials())) : Map.of();
    }

    private boolean references(String dagJson, Long id) {
        try { return containsReference(objectMapper.readTree(dagJson), id); }
        catch (Exception ignored) { return false; }
    }

    private boolean containsReference(JsonNode node, Long id) {
        if (node == null) return false;
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if ("dataSourceId".equals(field.getKey()) && (field.getValue().asText().equals(String.valueOf(id)))) return true;
                if (containsReference(field.getValue(), id)) return true;
            }
        } else if (node.isArray()) for (JsonNode child : node) if (containsReference(child, id)) return true;
        return false;
    }

    private DataSourceConnection save(DataSourceConnection entity) {
        try { return repository.saveAndFlush(entity); }
        catch (DataIntegrityViolationException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "同一所有者下数据源名称已存在");
        } catch (ObjectOptimisticLockingFailureException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "数据源已被其他请求修改，请刷新后重试");
        }
    }

    private String writeJson(Object value) {
        try { return objectMapper.writeValueAsString(value); }
        catch (Exception e) { throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "配置无法序列化"); }
    }

    private Map<String, Object> readMap(String json) {
        try { return objectMapper.readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {}); }
        catch (Exception e) { throw new IllegalStateException("数据源配置无法读取", e); }
    }

    private void rejectUnknown(Map<String, Object> raw, Set<String> allowed, String label) {
        for (String key : raw.keySet()) if (!allowed.contains(key)) badRequest(label + " 包含未知字段: " + key);
    }

    private void assertAllowedHost(String host) {
        if (!HOST.matcher(host).matches() || !validIpv4(host)) badRequest("主机格式非法");
        boolean allowed = Arrays.stream(allowedHosts.split(",")).map(String::trim).anyMatch(value -> value.equalsIgnoreCase(host));
        if (!allowed) badRequest("目标主机不在允许白名单中");
    }

    private boolean validIpv4(String host) {
        if (!host.matches("(?:\\d{1,3}\\.){3}\\d{1,3}")) return true;
        return Arrays.stream(host.split("\\.")).allMatch(part -> Integer.parseInt(part) <= 255);
    }

    private String host(Map<String, Object> raw, String key) {
        String host = requiredText(raw, key, 253).toLowerCase(Locale.ROOT);
        if (!HOST.matcher(host).matches()) badRequest("主机格式非法");
        return host;
    }

    private int port(Map<String, Object> raw, String key) {
        int value = integer(raw, key, -1);
        if (value < 1 || value > 65535) badRequest("端口必须在 1-65535 之间");
        return value;
    }

    private int integer(Map<String, Object> raw, String key, int fallback) {
        Object value = raw.get(key);
        if (value == null || String.valueOf(value).isBlank()) return fallback;
        try { return value instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(value)); }
        catch (NumberFormatException e) { badRequest(key + " 必须是整数"); return fallback; }
    }

    private boolean bool(Map<String, Object> raw, String key, boolean fallback) {
        Object value = raw.get(key);
        if (value == null) return fallback;
        if (value instanceof Boolean b) return b;
        if ("true".equalsIgnoreCase(String.valueOf(value))) return true;
        if ("false".equalsIgnoreCase(String.valueOf(value))) return false;
        badRequest(key + " 必须是布尔值"); return fallback;
    }

    private String requiredText(Map<String, Object> raw, String key, int max) {
        String value = text(raw, key, "").trim();
        if (value.isEmpty()) badRequest(key + " 不能为空");
        if (value.length() > max) badRequest(key + " 过长");
        return value;
    }

    private void optionalText(Map<String, Object> raw, Map<String, Object> out, String key, int max) {
        String value = text(raw, key, "").trim();
        if (value.length() > max) badRequest(key + " 过长");
        if (!value.isEmpty()) out.put(key, value);
    }

    private String text(Map<String, Object> raw, String key, String fallback) {
        Object value = raw == null ? null : raw.get(key);
        return value == null ? fallback : String.valueOf(value);
    }

    private String normalizeName(String name) {
        if (name == null || name.trim().isEmpty()) badRequest("name 不能为空");
        String value = name.trim();
        if (value.length() > 128) badRequest("name 过长");
        return value;
    }

    private String normalizeDescription(String description) {
        return description == null ? "" : description.trim();
    }

    private DataSourceType requireType(DataSourceType type) {
        if (type == null) badRequest("type 不能为空");
        return type;
    }

    private void requireSaveRequest(DataSourceSaveRequest request) {
        if (request == null) badRequest("请求体不能为空");
    }

    private void requireUser(CurrentUser user) {
        if (user == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
    }

    private String ownerName(CurrentUser user) {
        return user.displayName() == null || user.displayName().isBlank() ? user.username() : user.displayName();
    }

    private void badRequest(String message) { throw new ResponseStatusException(HttpStatus.BAD_REQUEST, message); }
    private void conflict(String message) { throw new ResponseStatusException(HttpStatus.CONFLICT, message); }
}
