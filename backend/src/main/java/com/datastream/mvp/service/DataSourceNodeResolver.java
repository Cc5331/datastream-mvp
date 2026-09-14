package com.datastream.mvp.service;

import com.datastream.mvp.dag.DagDefinition;
import com.datastream.mvp.model.DataSourceConnection;
import com.datastream.mvp.model.DataSourceType;
import com.datastream.mvp.repository.DataSourceConnectionRepository;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 把节点上的 dataSourceId 引用解析为连接级参数，合并进节点 params。
 * 兼容策略：无 dataSourceId 的旧 DAG 完全走原内联参数，不做任何改写。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DataSourceNodeResolver {
    private final DataSourceConnectionRepository repository;
    private final DataSourceSecretCipher cipher;
    private final ObjectMapper objectMapper;

    /** 返回注入了连接参数的新 DAG；没有任何引用时直接返回原对象。 */
    public DagDefinition resolve(DagDefinition dag, CurrentUser user) {
        if (dag == null || dag.getNodes() == null || dag.getNodes().isEmpty()) return dag;
        for (DagDefinition.DagNode node : dag.getNodes()) {
            if (node.getParams() == null) continue;
            Long dataSourceId = asLong(node.getParams().get("dataSourceId"));
            if (dataSourceId == null) continue;
            DataSourceConnection connection = load(dataSourceId);
            if (user != null && !user.isAdmin() && !user.id().equals(connection.getOwnerId())) {
                throw new IllegalArgumentException("无权使用数据源: " + dataSourceId);
            }
            apply(node, connection, dataSourceId);
        }
        return dag;
    }

    /** 作业 owner 视角解析：提交/预览链路按作业归属校验数据源使用权。 */
    public DagDefinition resolveForOwner(DagDefinition dag, Long ownerId) {
        if (dag == null || dag.getNodes() == null || ownerId == null) return dag;
        for (DagDefinition.DagNode node : dag.getNodes()) {
            if (node.getParams() == null) continue;
            Long dataSourceId = asLong(node.getParams().get("dataSourceId"));
            if (dataSourceId == null) continue;
            DataSourceConnection connection = load(dataSourceId);
            if (!ownerId.equals(connection.getOwnerId())) {
                throw new IllegalArgumentException("作业引用的数据源不属于当前创建者: " + dataSourceId);
            }
            apply(node, connection, dataSourceId);
        }
        return dag;
    }

    private void apply(DagDefinition.DagNode node, DataSourceConnection connection, Long dataSourceId) {
        Map<String, Object> merged = new LinkedHashMap<>(node.getParams());
        merged.putAll(connectionParams(connection, node.getType()));
        merged.putAll(credentialParams(connection));
        merged.put("dataSourceId", dataSourceId);
        node.setParams(merged);
    }

    /** 仅做校验，不修改；用于 preflight 报告数据源问题。 */
    public void validate(DagDefinition dag, CurrentUser user, List<String> errors, String nodeId) {
        if (dag == null || dag.getNodes() == null) return;
        for (DagDefinition.DagNode node : dag.getNodes()) {
            if (node.getParams() == null) continue;
            Long dataSourceId = asLong(node.getParams().get("dataSourceId"));
            if (dataSourceId == null) continue;
            if (nodeId != null && !nodeId.equals(node.getId())) continue;
            try {
                DataSourceConnection connection = load(dataSourceId);
                if (user != null && !user.isAdmin() && !user.id().equals(connection.getOwnerId())) {
                    errors.add("节点 " + node.getId() + " 引用的数据源不属于当前用户");
                    continue;
                }
                if (!supports(connection.getType(), node.getType())) {
                    errors.add("节点 " + node.getId() + " 引用的数据源类型 " + connection.getType()
                            + " 与控件 " + node.getType() + " 不匹配");
                }
            } catch (RuntimeException e) {
                errors.add("节点 " + node.getId() + " 引用的数据源不可用: " + e.getMessage());
            }
        }
    }

    private DataSourceConnection load(Long id) {
        DataSourceConnection connection = repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("数据源不存在: " + id));
        if (!connection.isEnabled()) {
            throw new IllegalArgumentException("数据源已停用: " + connection.getName());
        }
        return connection;
    }

    /** 从加密凭据还原节点级用户名/密码参数。 */
    private Map<String, Object> credentialParams(DataSourceConnection connection) {
        if (!connection.isCredentialConfigured()) return Map.of();
        String json = cipher.decrypt(connection.getEncryptedCredentials());
        Map<String, Object> credentials = readMap(json);
        Map<String, Object> params = new LinkedHashMap<>();
        if (credentials.get("username") != null) params.put("username", credentials.get("username"));
        if (credentials.get("password") != null) params.put("password", credentials.get("password"));
        return params;
    }

    /** 把持久化配置映射为节点级连接参数（不含凭据，凭据在外层单独处理）。 */
    private Map<String, Object> connectionParams(DataSourceConnection connection, String nodeType) {
        Map<String, Object> config = readMap(connection.getConfigJson());
        Map<String, Object> params = new LinkedHashMap<>();
        switch (connection.getType()) {
            case MYSQL, POSTGRESQL -> {
                params.put("url", jdbcUrl(connection.getType(), config));
                if (config.get("schema") != null) params.put("schema", config.get("schema"));
            }
            case ORACLE -> {
                params.put("url", jdbcUrl(DataSourceType.ORACLE, config));
                if (config.get("schema") != null) params.put("schema", config.get("schema"));
            }
            case KAFKA -> {
                if (config.get("bootstrapServers") != null) {
                    params.put("bootstrapServers", config.get("bootstrapServers"));
                }
            }
            case REDIS -> {
                if (config.get("host") != null) params.put("host", config.get("host"));
                if (config.get("port") != null) params.put("port", config.get("port"));
            }
            case HDFS -> {
                // HDFS 数据源只提供 NameNode 地址；具体读写路径属于资产级参数，必须由节点自带，
                // 不能在此臆测目录，否则会生成错误路径。
                String uri = text(config.get("uri"));
                if (!uri.isEmpty()) params.put("hdfsUri", uri);
            }
        }
        return params;
    }

    private String jdbcUrl(DataSourceType type, Map<String, Object> config) {
        String host = text(config.get("host"));
        String port = text(config.get("port"));
        String database = text(config.get("database"));
        if (host.isEmpty() || port.isEmpty() || database.isEmpty()) {
            throw new IllegalArgumentException("数据源缺少 host / port / database 配置");
        }
        return switch (type) {
            case MYSQL -> "jdbc:mysql://" + host + ":" + port + "/" + database
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
            case POSTGRESQL -> "jdbc:postgresql://" + host + ":" + port + "/" + database;
            case ORACLE -> {
                String service = text(config.get("serviceName")).isEmpty() ? database : text(config.get("serviceName"));
                yield "jdbc:oracle:thin:@//" + host + ":" + port + "/" + service;
            }
            default -> throw new IllegalArgumentException("不支持的数据源类型: " + type);
        };
    }

    private boolean supports(DataSourceType type, String nodeType) {
        if (nodeType == null) return true;
        return switch (type) {
            case MYSQL -> nodeType.startsWith("mysql_");
            case POSTGRESQL -> nodeType.startsWith("pg_");
            case ORACLE -> nodeType.startsWith("oracle_");
            case KAFKA -> nodeType.startsWith("kafka_");
            case REDIS -> nodeType.startsWith("redis_");
            case HDFS -> nodeType.startsWith("hdfs_");
        };
    }

    private Map<String, Object> readMap(String json) {
        try { return objectMapper.readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {}); }
        catch (Exception e) { throw new IllegalArgumentException("数据源配置无法解析"); }
    }

    private Long asLong(Object value) {
        if (value == null) return null;
        if (value instanceof Number number) return number.longValue();
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) return null;
        try { return Long.parseLong(text); }
        catch (NumberFormatException e) { return null; }
    }

    private String text(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }
}
