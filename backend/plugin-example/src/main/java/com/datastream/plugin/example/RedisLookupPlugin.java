package com.datastream.plugin.example;

import com.datastream.plugin.DataStreamPlugin;

/**
 * Redis 异步查询控件示例
 * 演示如何开发一个热加载插件
 */
public class RedisLookupPlugin implements DataStreamPlugin {

    @Override
    public String getType() {
        return "redis_lookup";
    }

    @Override
    public String getName() {
        return "Redis 异步查询";
    }

    @Override
    public String getCategory() {
        return "transform";
    }

    @Override
    public String getDescription() {
        return "根据字段值异步查询 Redis 并补充数据";
    }

    @Override
    public String getParamSchema() {
        return "{" +
            "  \"type\": \"object\"," +
            "  \"properties\": {" +
            "    \"redisHost\": {\"type\": \"string\", \"title\": \"Redis 地址\", \"default\": \"localhost\"}," +
            "    \"redisPort\": {\"type\": \"integer\", \"title\": \"Redis 端口\", \"default\": 6379}," +
            "    \"keyField\": {\"type\": \"string\", \"title\": \"查询键字段\"}," +
            "    \"targetField\": {\"type\": \"string\", \"title\": \"结果写入字段\"}" +
            "  }," +
            "  \"required\": [\"redisHost\", \"keyField\"]" +
            "}";
    }

    @Override
    public String getFlinkTemplate() {
        return "-- Redis AsyncDataStream enrichment\n" +
               "SELECT *, 'redis_enriched' AS ${targetField}\n" +
               "FROM ${source}";
    }

    @Override
    public String getVersion() {
        return "1.0.0";
    }
}
