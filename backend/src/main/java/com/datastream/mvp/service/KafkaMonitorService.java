package com.datastream.mvp.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Kafka 可视化监控：topic 列表/偏移统计 + 实时消息流（SSE 数据源）。
 */
@Slf4j
@Service
public class KafkaMonitorService {

    @Value("${app.kafka.bootstrap-servers:localhost:29092}")
    private String bootstrapServers;

    /** Topic 概览：分区数、最早/最新偏移、可读消息量 */
    public record TopicInfo(String name, int partitions, long earliest, long latest, long messageCount) {
    }

    public List<TopicInfo> listTopics() {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            Set<String> names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            List<TopicInfo> result = new ArrayList<>();
            for (String name : names) {
                if (name.startsWith("__")) {
                    continue; // 内部 topic 不展示
                }
                try {
                    int partitions = admin.describeTopics(Collections.singleton(name))
                            .allTopicNames().get(10, TimeUnit.SECONDS).get(name).partitions().size();
                    Map<TopicPartition, OffsetSpec> earliestReq = new HashMap<>();
                    Map<TopicPartition, OffsetSpec> latestReq = new HashMap<>();
                    for (int i = 0; i < partitions; i++) {
                        TopicPartition tp = new TopicPartition(name, i);
                        earliestReq.put(tp, OffsetSpec.earliest());
                        latestReq.put(tp, OffsetSpec.latest());
                    }
                    Map<TopicPartition, Long> earliest = offsets(admin, earliestReq);
                    Map<TopicPartition, Long> latest = offsets(admin, latestReq);
                    long e = earliest.values().stream().mapToLong(Long::longValue).sum();
                    long l = latest.values().stream().mapToLong(Long::longValue).sum();
                    result.add(new TopicInfo(name, partitions, e, l, Math.max(0, l - e)));
                } catch (Exception ex) {
                    log.warn("Kafka topic {} describe failed: {}", name, ex.getMessage());
                }
            }
            result.sort((a, b) -> Long.compare(b.messageCount, a.messageCount));
            return result;
        } catch (Exception e) {
            throw new RuntimeException("无法连接 Kafka（" + bootstrapServers + "）: " + e.getMessage(), e);
        }
    }

    private Map<TopicPartition, Long> offsets(AdminClient admin, Map<TopicPartition, OffsetSpec> specs) throws Exception {
        Map<TopicPartition, Long> out = new HashMap<>();
        admin.listOffsets(specs).all().get(10, TimeUnit.SECONDS)
                .forEach((tp, info) -> out.put(tp, info.offset()));
        return out;
    }

    /**
     * 实时订阅指定 topic，把每条消息通过 sink 回调送出（阻塞直至 maxMessages/maxMillis/线程中断）。
     */
    public void streamTopic(String topic, boolean fromBeginning, long maxMessages, long maxMillis,
                            Consumer<Map<String, Object>> sink) {
        Properties props = consumerProps();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromBeginning ? "earliest" : "latest");
        long started = System.currentTimeMillis();
        long count = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (!Thread.currentThread().isInterrupted()) {
                if (maxMillis > 0 && System.currentTimeMillis() - started > maxMillis) {
                    break;
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> r : records) {
                    count++;
                    Map<String, Object> msg = new LinkedHashMap<>();
                    msg.put("topic", r.topic());
                    msg.put("partition", r.partition());
                    msg.put("offset", r.offset());
                    msg.put("key", r.key() == null ? "" : r.key());
                    msg.put("value", r.value() == null ? "" : r.value());
                    sink.accept(msg);
                    if (maxMessages > 0 && count >= maxMessages) {
                        return;
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException ignored) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Kafka stream {} ended: {}", topic, e.getMessage());
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", e.getMessage());
            sink.accept(err);
        }
    }

    private Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "8000");
        return p;
    }

    private Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "mvp-visualizer-" + UUID.randomUUID());
        p.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "12000");
        return p;
    }
}
