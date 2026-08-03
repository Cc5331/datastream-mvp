package com.datastream.mvp.plugin;

/**
 * 控件插件 SPI 接口
 * 每个控件实现此接口，使系统能通过 SPI 动态加载
 */
public interface ControlPlugin {
    /** 控件类型标识，如 csv_input */
    String getType();

    /** 控件显示名称 */
    String getName();

    /** 控件分类：input / output / transform */
    String getCategory();

    /** 控件描述 */
    String getDescription();

    /** 参数 JSON Schema */
    String getParamSchema();

    /** Flink SQL/Table API 模板 */
    String getFlinkTemplate();

    /** 版本号 */
    String getVersion();
}
