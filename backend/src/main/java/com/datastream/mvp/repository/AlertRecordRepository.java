package com.datastream.mvp.repository;

import com.datastream.mvp.model.AlertRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface AlertRecordRepository extends JpaRepository<AlertRecord, Long> {
    List<AlertRecord> findTop100ByOrderByCreatedAtDesc();
    long countByReadFlagFalse();
    Optional<AlertRecord> findFirstByJobIdAndEventOrderByCreatedAtDesc(Long jobId, String event);
    List<AlertRecord> findByJobIdAndEventAndCreatedAtAfterOrderByCreatedAtDesc(Long jobId, String event, LocalDateTime after);
}
