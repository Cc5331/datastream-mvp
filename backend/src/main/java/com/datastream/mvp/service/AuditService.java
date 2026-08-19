package com.datastream.mvp.service;

import com.datastream.mvp.model.AuditLog;
import com.datastream.mvp.repository.AuditLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 操作审计：记录 + 分页查询
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

    private final AuditLogRepository repo;

    public void record(Long userId, String username, String action, String targetType, String targetId, String detail, String ip) {
        try {
            AuditLog entry = new AuditLog();
            entry.setUserId(userId);
            entry.setUsername(username == null ? "system" : username);
            entry.setAction(action);
            entry.setTargetType(targetType);
            entry.setTargetId(targetId);
            entry.setDetail(detail);
            entry.setIp(ip);
            entry.setCreatedAt(LocalDateTime.now());
            repo.save(entry);
        } catch (Exception e) {
            log.warn("Failed to save audit log: {}", e.getMessage());
        }
    }

    public Map<String, Object> page(int page, int size, String keyword) {
        int safePage = Math.max(0, page);
        int safeSize = Math.min(100, Math.max(1, size));
        String kw = (keyword == null || keyword.isBlank()) ? null : keyword.trim();
        Page<AuditLog> result = repo.search(kw, PageRequest.of(safePage, safeSize));
        Map<String, Object> map = new HashMap<>();
        map.put("content", result.getContent());
        map.put("total", result.getTotalElements());
        map.put("page", result.getNumber());
        map.put("size", result.getSize());
        return map;
    }
}
