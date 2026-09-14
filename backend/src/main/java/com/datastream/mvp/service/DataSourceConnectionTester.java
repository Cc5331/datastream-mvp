package com.datastream.mvp.service;

import com.datastream.mvp.dto.DataSourceTestResponse;
import com.datastream.mvp.model.DataSourceType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Service
public class DataSourceConnectionTester {
    private static final int TIMEOUT_MS = 4000;

    public DataSourceTestResponse test(DataSourceType type, Map<String, Object> config, Map<String, Object> credentials) {
        long started = System.nanoTime();
        try {
            switch (type) {
                case MYSQL, POSTGRESQL, ORACLE -> testJdbc(type, config, credentials);
                case KAFKA -> testKafka(config, credentials);
                case REDIS -> testRedis(config, credentials);
                case HDFS -> testHdfs(config);
            }
            return response(true, "连接成功", started);
        } catch (Exception ignored) {
            return response(false, "连接失败，请检查地址、凭据和服务状态", started);
        }
    }

    private void testJdbc(DataSourceType type, Map<String, Object> config, Map<String, Object> credentials) throws Exception {
        String host = string(config, "host");
        int port = integer(config, "port");
        String database = string(config, "database");
        String url = switch (type) {
            case MYSQL -> "jdbc:mysql://" + host + ":" + port + "/" + database + "?connectTimeout=" + TIMEOUT_MS + "&socketTimeout=" + TIMEOUT_MS;
            case POSTGRESQL -> "jdbc:postgresql://" + host + ":" + port + "/" + database + "?connectTimeout=4&socketTimeout=4";
            case ORACLE -> "jdbc:oracle:thin:@//" + host + ":" + port + "/" + string(config, "serviceName");
            default -> throw new IllegalArgumentException();
        };
        DriverManager.setLoginTimeout(4);
        try (Connection connection = DriverManager.getConnection(url, string(credentials, "username"), string(credentials, "password"))) {
            if (!connection.isValid(4)) throw new IOException("invalid connection");
        }
    }

    private void testKafka(Map<String, Object> config, Map<String, Object> credentials) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, string(config, "bootstrapServers"));
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        properties.put("security.protocol", string(config, "securityProtocol"));
        String mechanism = string(config, "saslMechanism");
        if (!mechanism.isBlank()) properties.put("sasl.mechanism", mechanism);
        String username = string(credentials, "username");
        String password = string(credentials, "password");
        if (!username.isBlank() || !password.isBlank()) {
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + jaas(username) + "\" password=\"" + jaas(password) + "\";");
        }
        try (AdminClient client = AdminClient.create(properties)) {
            client.describeCluster().clusterId().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void testRedis(Map<String, Object> config, Map<String, Object> credentials) throws Exception {
        boolean ssl = Boolean.parseBoolean(string(config, "ssl"));
        try (Socket socket = ssl ? javax.net.ssl.SSLSocketFactory.getDefault().createSocket() : new Socket()) {
            socket.connect(new InetSocketAddress(string(config, "host"), integer(config, "port")), TIMEOUT_MS);
            socket.setSoTimeout(TIMEOUT_MS);
            InputStream input = new BufferedInputStream(socket.getInputStream());
            OutputStream output = new BufferedOutputStream(socket.getOutputStream());
            String username = string(credentials, "username");
            String password = string(credentials, "password");
            if (!password.isBlank()) {
                writeResp(output, username.isBlank() ? new String[]{"AUTH", password} : new String[]{"AUTH", username, password});
                requireOk(input);
            }
            int database = integer(config, "database");
            if (database != 0) {
                writeResp(output, new String[]{"SELECT", String.valueOf(database)});
                requireOk(input);
            }
            writeResp(output, new String[]{"PING"});
            requireOk(input);
        }
    }

    private void testHdfs(Map<String, Object> config) throws Exception {
        URI uri = URI.create(string(config, "uri"));
        org.apache.hadoop.conf.Configuration hadoop = new org.apache.hadoop.conf.Configuration();
        hadoop.setBoolean("fs.hdfs.impl.disable.cache", true);
        hadoop.setInt("ipc.client.connect.timeout", TIMEOUT_MS);
        String user = string(config, "user");
        try (FileSystem fs = user.isBlank() ? FileSystem.newInstance(uri, hadoop) : FileSystem.newInstance(uri, hadoop, user)) {
            if (!fs.exists(new org.apache.hadoop.fs.Path(string(config, "testPath")))) throw new FileNotFoundException();
        }
    }

    private void writeResp(OutputStream output, String[] parts) throws IOException {
        StringBuilder command = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
            command.append('$').append(bytes.length).append("\r\n").append(part).append("\r\n");
        }
        output.write(command.toString().getBytes(StandardCharsets.UTF_8));
        output.flush();
    }

    private void requireOk(InputStream input) throws IOException {
        int marker = input.read();
        if (marker != '+') throw new IOException("redis error");
        while (input.read() != '\n') { }
    }

    private DataSourceTestResponse response(boolean success, String message, long started) {
        long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        return new DataSourceTestResponse(success, message, latency, LocalDateTime.now());
    }

    private String string(Map<String, Object> map, String key) {
        Object value = map == null ? null : map.get(key);
        return value == null ? "" : String.valueOf(value);
    }

    private int integer(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value instanceof Number number ? number.intValue() : Integer.parseInt(String.valueOf(value));
    }

    private String jaas(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
