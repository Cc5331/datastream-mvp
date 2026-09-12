package com.datastream.plugin;

/**
 * 控件插件 SPI 接口（独立 SDK 包）
 * 第三方开发者实现此接口即可开发新控件
 */
public interface DataStreamPlugin {
    /** 控件类型标识 */
    String getType();

    /** 显示名称 */
    String getName();

    /** 分类：input / output / transform */
    String getCategory();

    /** 描述 */
    String getDescription();

    /** 参数 JSON Schema */
    String getParamSchema();

    /** Flink SQL 模板 */
    String getFlinkTemplate();

    /** 版本号 */
    String getVersion();
}
