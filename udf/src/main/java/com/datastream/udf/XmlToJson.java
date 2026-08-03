package com.datastream.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink UDF: convert XML string to JSON string.
 * Registered in Flink SQL as: xml2json
 */
public class XmlToJson extends ScalarFunction {

    private static final XmlMapper XML_MAPPER = new XmlMapper();
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    /**
     * @param xml XML document text, e.g. &lt;order&gt;&lt;id&gt;1&lt;/id&gt;&lt;/order&gt;
     * @return JSON representation of the XML root element, or null for null/blank input
     */
    public String eval(String xml) {
        if (xml == null || xml.isBlank()) {
            return null;
        }
        try {
            JsonNode node = XML_MAPPER.readTree(xml);
            return JSON_MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            // Tolerant conversion: bad XML returns null instead of failing the whole job
            return null;
        }
    }
}