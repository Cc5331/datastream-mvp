package com.datastream.mvp.controller;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.security.CurrentUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class AlertControllerTest {

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void list_nonAdminOnlyReadsOwnedAlerts() {
        AlertRecordRepository repo = mock(AlertRecordRepository.class);
        AlertRecord alert = new AlertRecord();
        alert.setOwnerId(7L);
        when(repo.findByOwnerIdOrderByCreatedAtDesc(eq(7L), any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(alert)));
        authenticate(new CurrentUser(7L, "operator", "操作员", "OPERATOR"));

        Page<AlertRecord> result = new AlertController(repo).list(0, 20, null);

        assertEquals(1, result.getTotalElements());
        verify(repo).findByOwnerIdOrderByCreatedAtDesc(eq(7L), any(Pageable.class));
        verify(repo, never()).findAllByOrderByCreatedAtDesc(any());
    }

    @Test
    void unreadCount_nonAdminOnlyCountsOwnedAlerts() {
        AlertRecordRepository repo = mock(AlertRecordRepository.class);
        when(repo.countByOwnerIdAndReadFlagFalse(7L)).thenReturn(3L);
        authenticate(new CurrentUser(7L, "viewer", "查看者", "VIEWER"));

        Object unread = new AlertController(repo).unreadCount().get("unread");

        assertEquals(3L, unread);
        verify(repo).countByOwnerIdAndReadFlagFalse(7L);
        verify(repo, never()).countByReadFlagFalse();
    }

    @Test
    void markAllRead_adminUpdatesAllRows() {
        AlertRecordRepository repo = mock(AlertRecordRepository.class);
        authenticate(new CurrentUser(1L, "admin", "管理员", "ADMIN"));

        new AlertController(repo).markAllRead();

        verify(repo).markAllRead();
        verify(repo, never()).markAllReadByOwnerId(any());
    }

    @Test
    void batchMarkRead_nonAdminOnlyUpdatesOwnedRows() {
        AlertRecordRepository repo = mock(AlertRecordRepository.class);
        when(repo.markReadByIdsAndOwnerId(eq(List.of(10L, 11L)), eq(7L))).thenReturn(2);
        authenticate(new CurrentUser(7L, "operator", "操作员", "OPERATOR"));

        var res = new AlertController(repo).batchMarkRead(List.of(10L, 10L, 11L));

        assertEquals(2, res.getBody().get("updated"));
        verify(repo).markReadByIdsAndOwnerId(List.of(10L, 11L), 7L);
        verify(repo, never()).markReadByIds(any());
    }

    @Test
    void batchDelete_adminDeletesGivenRows() {
        AlertRecordRepository repo = mock(AlertRecordRepository.class);
        when(repo.deleteByIds(eq(List.of(5L, 6L)))).thenReturn(2);
        authenticate(new CurrentUser(1L, "admin", "管理员", "ADMIN"));

        var res = new AlertController(repo).batchDelete(List.of(5L, 6L));

        assertEquals(2, res.getBody().get("deleted"));
        verify(repo).deleteByIds(List.of(5L, 6L));
        verify(repo, never()).deleteByIdsAndOwnerId(any(), any());
    }

    @Test
    void batchMarkRead_emptySelectionRejected() {
        AlertRecordRepository repo = mock(AlertRecordRepository.class);
        authenticate(new CurrentUser(1L, "admin", "管理员", "ADMIN"));

        org.junit.jupiter.api.Assertions.assertThrows(
                org.springframework.web.server.ResponseStatusException.class,
                () -> new AlertController(repo).batchMarkRead(List.of()));
        verify(repo, never()).markReadByIds(any());
    }

    private void authenticate(CurrentUser user) {
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(user, null, List.of()));
    }
}
