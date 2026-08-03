package com.datastream.mvp.dag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
public class FlinkDagExecutor implements DagExecutor {

    private final LocalDagExecutor localExecutor;

    public FlinkDagExecutor(LocalDagExecutor localExecutor) {
        this.localExecutor = localExecutor;
    }

    @Override
    public String execute(DagDefinition dag) throws Exception {
        String flinkSql = localExecutor.execute(dag);
        log.info("[FlinkDagExecutor] Would submit to Flink cluster: {}", dag.getJobName());
        log.info("Flink SQL:\n{}", flinkSql);
        return "flink-job-" + UUID.randomUUID().toString();
    }
}