package com.datastream.mvp.repository;

import com.datastream.mvp.model.ScheduleHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ScheduleHistoryRepository extends JpaRepository<ScheduleHistory, Long> {
    List<ScheduleHistory> findByJobIdOrderByTriggerTimeDesc(Long jobId);
}
