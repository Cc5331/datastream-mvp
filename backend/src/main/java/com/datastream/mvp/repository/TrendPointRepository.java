package com.datastream.mvp.repository;

import com.datastream.mvp.model.TrendPoint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface TrendPointRepository extends JpaRepository<TrendPoint, Long> {
    /** 指定运行批次的趋势点，按时间升序 */
    List<TrendPoint> findByJobIdAndRunKeyOrderByTAsc(Long jobId, String runKey);

    /** 取某作业最近一批趋势点（倒序，最新在前） */
    List<TrendPoint> findTop300ByJobIdOrderByTDesc(Long jobId);

    /** 新一次运行开始时清掉该作业全部旧趋势点 */
    @Modifying
    @Transactional
    void deleteByJobId(Long jobId);
}
