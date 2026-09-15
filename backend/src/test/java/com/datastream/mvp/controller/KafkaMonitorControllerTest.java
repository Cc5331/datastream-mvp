package com.datastream.mvp.controller;

import com.datastream.mvp.service.KafkaMonitorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class KafkaMonitorControllerTest {
    @Test
    void topics_delegatesToMonitorService() {
        KafkaMonitorService service = mock(KafkaMonitorService.class);
        List<KafkaMonitorService.TopicInfo> expected = List.of(
                new KafkaMonitorService.TopicInfo("orders", 2, 3, 15, 12));
        when(service.listTopics()).thenReturn(expected);
        KafkaMonitorController controller = new KafkaMonitorController(service, new ObjectMapper());

        assertEquals(expected, controller.topics());
        verify(service).listTopics();
    }

    /**
     * 端点权限回归：Kafka 原始消息与预览文件内容都属于平台级数据，
     * 不允许任意登录用户（含 VIEWER）读取——此前这两个控制器没有任何角色限制。
     */
    @Test
    void kafkaAndPreviewEndpoints_requireOperatorOrAdmin() {
        for (Class<?> type : List.of(KafkaMonitorController.class, PreviewController.class)) {
            var annotation = type.getAnnotation(
                    org.springframework.security.access.prepost.PreAuthorize.class);
            assertEquals(true, annotation != null, type.getSimpleName() + " 必须声明类级 @PreAuthorize");
            assertEquals("hasAnyRole('ADMIN','OPERATOR')", annotation.value(),
                    type.getSimpleName() + " 权限应为 ADMIN/OPERATOR");
        }
    }
}
