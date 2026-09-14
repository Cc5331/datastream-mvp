package com.datastream.mvp.service;

import com.datastream.mvp.model.AuditLog;
import com.datastream.mvp.repository.AuditLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.ArrayList;
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
        return page(page, size, keyword, null, null, null, null, null, null, null);
    }

    public Map<String, Object> page(int page, int size, String keyword, String username, String action,
                                    String targetType, String targetId, String ip,
                                    LocalDateTime from, LocalDateTime to) {
        if (from != null && to != null && from.isAfter(to)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "from 不能晚于 to");
        }
        int safePage = Math.max(0, page);
        int safeSize = Math.min(100, Math.max(1, size));
        Specification<AuditLog> spec = (root, query, cb) -> {
            var predicates = new ArrayList<jakarta.persistence.criteria.Predicate>();
            String kw = normalize(keyword);
            if (kw != null) {
                String like = "%" + kw.toLowerCase() + "%";
                predicates.add(cb.or(cb.like(cb.lower(root.get("username")), like),
                        cb.like(cb.lower(root.get("action")), like), cb.like(cb.lower(root.get("targetType")), like),
                        cb.like(cb.lower(root.get("targetId")), like), cb.like(cb.lower(root.get("detail")), like),
                        cb.like(cb.lower(root.get("ip")), like)));
            }
            addLike(predicates, cb, root, "username", username);
            addLike(predicates, cb, root, "action", action);
            addLike(predicates, cb, root, "targetType", targetType);
            addLike(predicates, cb, root, "targetId", targetId);
            addLike(predicates, cb, root, "ip", ip);
            if (from != null) predicates.add(cb.greaterThanOrEqualTo(root.get("createdAt"), from));
            if (to != null) predicates.add(cb.lessThanOrEqualTo(root.get("createdAt"), to));
            return cb.and(predicates.toArray(jakarta.persistence.criteria.Predicate[]::new));
        };
        Page<AuditLog> result = repo.findAll(spec, PageRequest.of(safePage, safeSize, Sort.by(Sort.Direction.DESC, "createdAt")));
        Map<String, Object> map = new HashMap<>();
        map.put("content", result.getContent());
        map.put("total", result.getTotalElements());
        map.put("page", result.getNumber());
        map.put("size", result.getSize());
        return map;
    }

    private String normalize(String value) { return value == null || value.isBlank() ? null : value.trim(); }

    private void addLike(java.util.List<jakarta.persistence.criteria.Predicate> predicates,
                         jakarta.persistence.criteria.CriteriaBuilder cb,
                         jakarta.persistence.criteria.Root<AuditLog> root, String field, String value) {
        String normalized = normalize(value);
        if (normalized != null) predicates.add(cb.like(cb.lower(root.get(field)), "%" + normalized.toLowerCase() + "%"));
    }
}
