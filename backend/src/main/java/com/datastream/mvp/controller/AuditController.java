package com.datastream.mvp.controller;

import com.datastream.mvp.service.AuditService;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 操作审计查询 API（ADMIN / OPERATOR 可见）
 */
@RestController
@RequestMapping("/api/audit")
@RequiredArgsConstructor
@PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
public class AuditController {

    private final AuditService auditService;

    @GetMapping
    public Map<String, Object> list(@RequestParam(defaultValue = "0") int page,
                                    @RequestParam(defaultValue = "20") int size,
                                    @RequestParam(required = false) String keyword,
                                    @RequestParam(required = false) String username,
                                    @RequestParam(required = false) String action,
                                    @RequestParam(required = false) String targetType,
                                    @RequestParam(required = false) String targetId,
                                    @RequestParam(required = false) String ip,
                                    @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
                                    @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {
        return auditService.page(page, size, keyword, username, action, targetType, targetId, ip, from, to);
    }
}
