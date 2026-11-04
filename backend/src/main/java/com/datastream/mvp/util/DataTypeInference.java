package com.datastream.mvp.util;

import java.util.List;

/**
 * 从样本值推断 Flink SQL 列类型（供文件导入分析与 Kafka 消息字段推断共用，避免两处各写一套）。
 *
 * <p><b>空值一律退化为 STRING</b>：CSV/JSON/Kafka 读取时空值会让 INT/DOUBLE 解析失败变成 null，
 * 与其「看起来精确」不如「稳」。其余按 int → bigint → boolean → double → date 的顺序判定，
 * 兜底 STRING。</p>
 */
public final class DataTypeInference {

    private DataTypeInference() {}

    /** @param values 该列的样本值（可为 null 元素）；含空串直接返回 STRING */
    public static String infer(List<String> values) {
        if (values == null || values.isEmpty()) return "STRING";
        boolean any = false;
        boolean intOk = true;
        boolean longOk = true;
        boolean doubleOk = true;
        boolean boolOk = true;
        boolean dateOk = true;
        for (String raw : values) {
            String value = raw == null ? "" : raw.trim();
            if (value.isEmpty()) return "STRING";
            any = true;
            if (!value.matches("[+-]?\\d+")) intOk = false;
            if (!value.matches("[+-]?\\d{1,18}")) longOk = false;
            if (!value.matches("[+-]?\\d+(\\.\\d+)?([eE][+-]?\\d+)?")) doubleOk = false;
            if (!("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) boolOk = false;
            if (!value.matches("\\d{4}-\\d{2}-\\d{2}")) dateOk = false;
        }
        if (!any) return "STRING";
        if (intOk) return "INT";
        if (longOk) return "BIGINT";
        if (boolOk) return "BOOLEAN";
        if (doubleOk) return "DOUBLE";
        if (dateOk) return "DATE";
        return "STRING";
    }
}
