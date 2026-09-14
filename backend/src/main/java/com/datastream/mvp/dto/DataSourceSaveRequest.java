package com.datastream.mvp.dto;

import com.datastream.mvp.model.DataSourceType;

import java.util.Map;

public record DataSourceSaveRequest(
        String name,
        DataSourceType type,
        String description,
        Boolean enabled,
        Map<String, Object> config,
        Map<String, Object> credentials,
        Boolean clearCredentials,
        Long version
) {}
