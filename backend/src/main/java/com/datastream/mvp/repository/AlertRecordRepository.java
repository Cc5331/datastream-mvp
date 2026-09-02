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

import java.util.Collection;
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

    // ===== 批量操作 =====
    @Modifying
    @Transactional
    @Query("update AlertRecord a set a.readFlag = true where a.id in :ids")
    int markReadByIds(@Param("ids") Collection<Long> ids);

    @Modifying
    @Transactional
    @Query("update AlertRecord a set a.readFlag = true where a.id in :ids and a.ownerId = :ownerId")
    int markReadByIdsAndOwnerId(@Param("ids") Collection<Long> ids, @Param("ownerId") Long ownerId);

    @Modifying
    @Transactional
    @Query("delete from AlertRecord a where a.id in :ids")
    int deleteByIds(@Param("ids") Collection<Long> ids);

    @Modifying
    @Transactional
    @Query("delete from AlertRecord a where a.id in :ids and a.ownerId = :ownerId")
    int deleteByIdsAndOwnerId(@Param("ids") Collection<Long> ids, @Param("ownerId") Long ownerId);
}
