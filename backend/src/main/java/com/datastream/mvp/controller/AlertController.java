package com.datastream.mvp.controller;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.repository.AlertRecordRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 告警中心 REST API：告警列表 / 未读数 / 标记已读
 */
@RestController
@RequestMapping("/api/alerts")
@RequiredArgsConstructor
public class AlertController {

    private final AlertRecordRepository alertRepo;

    @GetMapping
    public List<AlertRecord> list(@RequestParam(required = false) Boolean read) {
        List<AlertRecord> all = alertRepo.findTop100ByOrderByCreatedAtDesc();
        if (read == null) return all;
        return all.stream().filter(a -> Boolean.valueOf(read).equals(a.getReadFlag())).toList();
    }

    @GetMapping("/unread-count")
    public Map<String, Object> unreadCount() {
        return Map.of("unread", alertRepo.countByReadFlagFalse());
    }

    @PostMapping("/{id}/read")
    public ResponseEntity<Void> markRead(@PathVariable Long id) {
        alertRepo.findById(id).ifPresent(a -> {
            a.setReadFlag(true);
            alertRepo.save(a);
        });
        return ResponseEntity.ok().build();
    }

    @PostMapping("/read-all")
    public ResponseEntity<Void> markAllRead() {
        for (AlertRecord a : alertRepo.findTop100ByOrderByCreatedAtDesc()) {
            if (!Boolean.TRUE.equals(a.getReadFlag())) {
                a.setReadFlag(true);
                alertRepo.save(a);
            }
        }
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        alertRepo.findById(id).ifPresent(alertRepo::delete);
        return ResponseEntity.ok().build();
    }
}
