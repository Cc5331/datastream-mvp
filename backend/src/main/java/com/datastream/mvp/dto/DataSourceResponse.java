package com.datastream.mvp.dto;

import com.datastream.mvp.model.DataSourceType;

import java.time.LocalDateTime;
import java.util.Map;

public record DataSourceResponse(
        Long id,
        String name,
        DataSourceType type,
        String description,
        Map<String, Object> config,
        boolean credentialConfigured,
        boolean enabled,
        Long ownerId,
        String ownerName,
        LocalDateTime createdAt,
        LocalDateTime updatedAt,
        Long version
) {}
