package com.datastream.mvp.controller;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@RestController
@RequestMapping("/api/alerts")
@RequiredArgsConstructor
public class AlertController {

    private final AlertRecordRepository alertRepo;

    @GetMapping
    public Page<AlertRecord> list(@RequestParam(defaultValue = "0") int page,
                                  @RequestParam(defaultValue = "20") int size,
                                  @RequestParam(required = false) Boolean read) {
        CurrentUser user = currentUser();
        PageRequest pageable = PageRequest.of(Math.max(0, page), Math.min(100, Math.max(1, size)));
        if (user.isAdmin()) {
            return read == null
                    ? alertRepo.findAllByOrderByCreatedAtDesc(pageable)
                    : alertRepo.findByReadFlagOrderByCreatedAtDesc(read, pageable);
        }
        return read == null
                ? alertRepo.findByOwnerIdOrderByCreatedAtDesc(user.id(), pageable)
                : alertRepo.findByOwnerIdAndReadFlagOrderByCreatedAtDesc(user.id(), read, pageable);
    }

    @GetMapping("/unread-count")
    public Map<String, Object> unreadCount() {
        CurrentUser user = currentUser();
        long unread = user.isAdmin()
                ? alertRepo.countByReadFlagFalse()
                : alertRepo.countByOwnerIdAndReadFlagFalse(user.id());
        return Map.of("unread", unread);
    }

    @PostMapping("/{id}/read")
    public ResponseEntity<Void> markRead(@PathVariable Long id) {
        AlertRecord alert = findAccessible(id, currentUser());
        alert.setReadFlag(true);
        alertRepo.save(alert);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/read-all")
    public ResponseEntity<Void> markAllRead() {
        CurrentUser user = currentUser();
        if (user.isAdmin()) alertRepo.markAllRead();
        else alertRepo.markAllReadByOwnerId(user.id());
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        AlertRecord alert = findAccessible(id, currentUser());
        alertRepo.delete(alert);
        return ResponseEntity.ok().build();
    }

    private AlertRecord findAccessible(Long id, CurrentUser user) {
        return (user.isAdmin() ? alertRepo.findById(id) : alertRepo.findByIdAndOwnerId(id, user.id()))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "告警不存在"));
    }

    private CurrentUser currentUser() {
        CurrentUser user = SecurityUtils.currentUser();
        if (user == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录或登录已过期");
        return user;
    }
}
