package com.datastream.mvp.repository;

import com.datastream.mvp.model.AlertRecord;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface AlertRecordRepository extends JpaRepository<AlertRecord, Long> {
    Page<AlertRecord> findAllByOrderByCreatedAtDesc(Pageable pageable);
    Page<AlertRecord> findByReadFlagOrderByCreatedAtDesc(Boolean readFlag, Pageable pageable);
    Page<AlertRecord> findByOwnerIdOrderByCreatedAtDesc(Long ownerId, Pageable pageable);
    Page<AlertRecord> findByOwnerIdAndReadFlagOrderByCreatedAtDesc(Long ownerId, Boolean readFlag, Pageable pageable);
    long countByReadFlagFalse();
    long countByOwnerIdAndReadFlagFalse(Long ownerId);
    Optional<AlertRecord> findByIdAndOwnerId(Long id, Long ownerId);
    Optional<AlertRecord> findFirstByJobIdAndEventOrderByCreatedAtDesc(Long jobId, String event);

    @Modifying
    @Transactional
    @Query("update AlertRecord a set a.readFlag = true where a.readFlag = false")
    int markAllRead();

    @Modifying
    @Transactional
    @Query("update AlertRecord a set a.readFlag = true where a.ownerId = :ownerId and a.readFlag = false")
    int markAllReadByOwnerId(@Param("ownerId") Long ownerId);
}
