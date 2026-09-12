package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;

/** 告警触发事件：由 AlertService 在真实故障告警（非 INFO）发生时发布，供自动诊断等监听。 */
public record AlertTriggeredEvent(Long jobId, String jobName, String event, String level, String message) {
    public static AlertTriggeredEvent from(JobDefinition job, String level, String event, String message) {
        return new AlertTriggeredEvent(job == null ? null : job.getId(),
                job == null ? null : job.getName(), event, level, message);
    }
}
