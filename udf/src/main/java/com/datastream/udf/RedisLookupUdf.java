package com.datastream.udf;

import org.apache.flink.table.functions.ScalarFunction;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * Flink UDF: Redis 字段富化。
 * 以数据中某字段的值为 key（可选前缀）查询 Redis GET，返回值为扩充字段的字符串；取不到返回 null。
 * 注册为 SQL 函数 redis_lookup；连接池在 open() 初始化，规避序列化；Caffeine 本地缓存 TTL 60s 降低 Redis 压力。
 * 连接参数由控制变量（SQL 端 SET）注入或默认 localhost:6379；多作业共用时可用参数重载 eval。
 */
public class RedisLookupUdf extends ScalarFunction {

    private transient JedisPool pool;
    private transient Cache<String, String> cache;

    /** 可选配置：host/port/password 通过 eval 的三参重载由 SQL 传入（默认重载走单参） */
    public String eval(String key) {
        return lookup("localhost", 6379, null, key);
    }

    public String eval(String host, String port, String password, String key) {
        int p;
        try {
            p = Integer.parseInt(port);
        } catch (Exception e) {
            p = 6379;
        }
        return lookup(host, p, (password == null || password.isBlank()) ? null : password, key);
    }

    private String lookup(String host, int port, String password, String key) {
        if (key == null || key.isBlank()) return null;
        try {
            ensurePool(host, port, password);
            String cached = cache.getIfPresent(key);
            if (cached != null) return cached;
            try (var jedis = pool.getResource()) {
                String value = jedis.get(key);
                if (value != null) cache.put(key, value);
                return value;
            }
        } catch (Exception e) {
            // 容错：单条查询失败不阻断作业，返回 null
            return null;
        }
    }

    private synchronized void ensurePool(String host, int port, String password) {
        if (pool == null) {
            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(16);
            cfg.setMaxIdle(8);
            cfg.setMinIdle(1);
            cfg.setTestOnBorrow(true);
            pool = (password == null)
                    ? new JedisPool(cfg, host, port, 2000)
                    : new JedisPool(cfg, host, port, 2000, password);
            cache = Caffeine.newBuilder()
                    .expireAfterWrite(Duration.ofSeconds(60))
                    .maximumSize(10_000)
                    .build();
        }
    }

    @Override
    public void close() throws Exception {
        if (pool != null) {
            pool.close();
            pool = null;
        }
        super.close();
    }
}
