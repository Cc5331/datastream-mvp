package com.datastream.mvp.service;

import com.datastream.mvp.ai.AgentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * 故障告警 -> 自动智能诊断闭环。
 * 监听 AlertTriggeredEvent，对具体作业的故障告警立即异步生成诊断报告（trigger=<告警事件>）。
 * 系统级（jobId=null）告警（如资源/集群心跳）不关联具体作业，跳过自动诊断。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AutoDiagnosisListener {

    private final AgentService agentService;

    @Async
    @EventListener
    public void onAlertTriggered(AlertTriggeredEvent event) {
        if (event.jobId() == null) return;
        agentService.autoDiagnose(event.jobId(), event.event());
    }
}
