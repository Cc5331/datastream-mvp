package com.datastream.mvp.repository;

import com.datastream.mvp.model.AuditLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AuditLogRepository extends JpaRepository<AuditLog, Long> {

    @Query("select a from AuditLog a where (:kw is null or a.username like %:kw% or a.action like %:kw% or a.targetType like %:kw% or a.targetId like %:kw% or a.detail like %:kw%) order by a.createdAt desc")
    Page<AuditLog> search(@Param("kw") String kw, Pageable pageable);
}
