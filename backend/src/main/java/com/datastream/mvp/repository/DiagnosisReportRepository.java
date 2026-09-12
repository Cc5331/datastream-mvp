package com.datastream.mvp.repository;

import com.datastream.mvp.model.DiagnosisReport;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DiagnosisReportRepository extends JpaRepository<DiagnosisReport, Long> {
    List<DiagnosisReport> findByJobIdOrderByCreatedAtDesc(Long jobId);
    List<DiagnosisReport> findByJobIdAndTriggerOrderByCreatedAtDesc(Long jobId, String trigger);
}
