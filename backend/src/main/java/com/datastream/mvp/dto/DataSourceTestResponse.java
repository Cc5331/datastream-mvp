package com.datastream.mvp.dto;

import java.time.LocalDateTime;

public record DataSourceTestResponse(boolean success, String message, long latencyMs, LocalDateTime checkedAt) {}
