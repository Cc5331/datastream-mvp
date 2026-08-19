package com.datastream.mvp.repository;

import com.datastream.mvp.model.JobVersion;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobVersionRepository extends JpaRepository<JobVersion, Long> {
    List<JobVersion> findByJobIdOrderByVersionNoDesc(Long jobId);
    @Transactional
    void deleteByJobId(Long jobId);
}
