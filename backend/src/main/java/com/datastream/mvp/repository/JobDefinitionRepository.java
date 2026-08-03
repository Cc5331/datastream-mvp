package com.datastream.mvp.repository;

import com.datastream.mvp.model.JobDefinition;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobDefinitionRepository extends JpaRepository<JobDefinition, Long> {
    List<JobDefinition> findByStatusOrderByUpdatedAtDesc(JobDefinition.JobStatus status);
    List<JobDefinition> findAllByOrderByUpdatedAtDesc();
    List<JobDefinition> findByStatusIn(List<JobDefinition.JobStatus> statuses);
}
