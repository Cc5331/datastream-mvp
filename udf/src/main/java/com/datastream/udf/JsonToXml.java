package com.datastream.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink UDF: convert JSON string to XML string.
 * Registered in Flink SQL as: json2xml
 */
public class JsonToXml extends ScalarFunction {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final XmlMapper XML_MAPPER = new XmlMapper();

    /**
     * @param json JSON document text, e.g. {"id":1,"name":"alice"}
     * @return XML representation with &lt;root&gt; wrapper, or null for null/blank input
     */
    public String eval(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            JsonNode node = JSON_MAPPER.readTree(json);
            return XML_MAPPER.writer().withRootName("root").writeValueAsString(node);
        } catch (Exception e) {
            // Tolerant conversion: bad JSON returns null instead of failing the whole job
            return null;
        }
    }
}