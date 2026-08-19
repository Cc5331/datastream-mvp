package com.datastream.mvp.repository;

import com.datastream.mvp.model.JobDependency;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobDependencyRepository extends JpaRepository<JobDependency, Long> {
    List<JobDependency> findByDownstreamJobId(Long downstreamJobId);
    List<JobDependency> findByUpstreamJobId(Long upstreamJobId);
    List<JobDependency> findAllByOrderByCreatedAtAsc();
    boolean existsByUpstreamJobIdAndDownstreamJobId(Long upstreamJobId, Long downstreamJobId);
    void deleteByDownstreamJobId(Long downstreamJobId);
    void deleteByUpstreamJobId(Long upstreamJobId);
    void deleteByUpstreamJobIdOrDownstreamJobId(Long upstreamJobId, Long downstreamJobId);
}
