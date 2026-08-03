package com.datastream.mvp.dag;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DagDefinition {
    private String jobName;
    private Integer parallelism = 1;
    private List<DagNode> nodes;
    private List<DagEdge> edges;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DagNode {
        private String id;
        private String type;
        private String label;
        private String category;
        private Map<String, Object> params;
        private Object paramSchema;
        private Double x;
        private Double y;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DagEdge {
        private String id;
        private String source;
        private String target;
        private String sourcePort;
        private String targetPort;
    }
}
