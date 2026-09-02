package com.datastream.mvp.config;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void handleUnreadableMalformedBodyReturns400() {
        HttpMessageNotReadableException ex =
                new HttpMessageNotReadableException("JSON parse error: Cannot deserialize value of type `java.util.ArrayList<Long>` from Number value (325)");

        ResponseEntity<Map<String, Object>> resp = handler.handleUnreadable(ex);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals(400, resp.getBody().get("status"));
        assertTrue(((String) resp.getBody().get("error")).startsWith("请求体 JSON 格式错误"));
    }

    @Test
    void handleUnreadableTruncatesVeryLongMessage() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 300; i++) sb.append('x');
        HttpMessageNotReadableException ex = new HttpMessageNotReadableException(sb.toString());

        ResponseEntity<Map<String, Object>> resp = handler.handleUnreadable(ex);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertTrue(((String) resp.getBody().get("error")).length() <= 220);
    }
}
