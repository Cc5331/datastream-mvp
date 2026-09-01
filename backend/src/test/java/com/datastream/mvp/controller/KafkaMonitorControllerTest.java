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
}
