package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobDependency;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobDependencyRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.datastream.mvp.security.CurrentUser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.*;

/**
 * 作业依赖编排：依赖关系维护、环检测、工作流图、上游完成自动触发下游
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DependencyService {

    private final JobDependencyRepository depRepo;
    private final JobDefinitionRepository jobRepo;
    private final JobLogRepository logRepo;
    private final JobService jobService;
    private final AlertService alertService;

    /** 查询某个作业的全部上游依赖（含作业名与状态） */
    public List<Map<String, Object>> getDependencies(Long downstreamJobId) {
        List<JobDependency> deps = depRepo.findByDownstreamJobId(downstreamJobId);
        List<Map<String, Object>> result = new ArrayList<>();
        for (JobDependency dep : deps) {
            JobDefinition up = jobRepo.findById(dep.getUpstreamJobId()).orElse(null);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("upstreamJobId", dep.getUpstreamJobId());
            m.put("upstreamName", up == null ? "(已删除)" : up.getName());
            m.put("upstreamStatus", up == null ? "DELETED" : up.getStatus().name());
            result.add(m);
        }
        return result;
    }

    /** 查询某个作业的全部下游作业 ID（用于工作流图） */
    public List<Long> getDownstreamJobIds(Long upstreamJobId) {
        List<JobDependency> deps = depRepo.findByUpstreamJobId(upstreamJobId);
        return deps.stream().map(JobDependency::getDownstreamJobId).toList();
    }

    /**
     * 保存依赖：全量覆盖。校验作业归属 + 环检测 + 状态联动（WAITING / BLOCKED）
     */
    @Transactional
    public List<Map<String, Object>> saveDependencies(Long downstreamJobId, List<Long> upstreamIds, CurrentUser cu) {
        JobDefinition downstream = jobRepo.findById(downstreamJobId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "作业不存在: " + downstreamJobId));
        assertAccess(downstream, cu);

        List<Long> ups = upstreamIds == null ? List.of() : upstreamIds.stream().distinct().toList();
        if (ups.contains(downstreamJobId)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "作业不能依赖自身");
        }

        // 校验上游作业存在 + 访问权限
        List<JobDefinition> upstreamJobs = new ArrayList<>();
        for (Long upId : ups) {
            JobDefinition up = jobRepo.findById(upId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "上游作业不存在: " + upId));
            assertAccess(up, cu);
            upstreamJobs.add(up);
        }

        // 环检测：新增边 (up -> down) 后，若从 up 沿上游链能回到 down，则成环
        if (createsCycle(downstreamJobId, upstreamJobs, ups)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "依赖关系存在环，已拒绝保存");
        }

        depRepo.deleteByDownstreamJobId(downstreamJobId);
        for (Long upId : ups) {
            JobDependency dep = new JobDependency();
            dep.setUpstreamJobId(upId);
            dep.setDownstreamJobId(downstreamJobId);
            dep.setCreatedAt(LocalDateTime.now());
            depRepo.save(dep);
        }

        // 状态联动：未运行过的作业进入 WAITING / BLOCKED
        JobDefinition.JobStatus cur = downstream.getStatus();
        if (cur != JobDefinition.JobStatus.RUNNING && cur != JobDefinition.JobStatus.SUBMITTED
                && cur != JobDefinition.JobStatus.COMPLETED && cur != JobDefinition.JobStatus.CANCELLED) {
            if (ups.isEmpty()) {
                if (downstream.getStatus() == JobDefinition.JobStatus.WAITING
                        || downstream.getStatus() == JobDefinition.JobStatus.BLOCKED) {
                    downstream.setStatus(JobDefinition.JobStatus.DRAFT);
                }
            } else {
                boolean hasFailed = upstreamJobs.stream().anyMatch(u -> u.getStatus() == JobDefinition.JobStatus.FAILED
                        || u.getStatus() == JobDefinition.JobStatus.CANCELLED);
                boolean allDone = upstreamJobs.stream().allMatch(u -> u.getStatus() == JobDefinition.JobStatus.COMPLETED);
                if (hasFailed) {
                    downstream.setStatus(JobDefinition.JobStatus.BLOCKED);
                } else if (!allDone) {
                    downstream.setStatus(JobDefinition.JobStatus.WAITING);
                } else {
                    downstream.setStatus(JobDefinition.JobStatus.DRAFT);
                }
            }
            downstream.setUpdatedAt(LocalDateTime.now());
            jobRepo.save(downstream);
        }

        addLog(downstreamJobId, "INFO", "依赖关系已更新：上游 " + ups.size() + " 个作业");
        return getDependencies(downstreamJobId);
    }

    /** 删除作业时清理其依赖边 */
    @Transactional
    public void deleteJobDependencies(Long jobId) {
        depRepo.deleteByUpstreamJobIdOrDownstreamJobId(jobId, jobId);
    }

    /**
     * 全工作流图（用户可见范围内）
     */
    public Map<String, Object> getWorkflow(CurrentUser cu) {
        List<JobDefinition> jobs = cu == null ? List.of()
                : (cu.isAdmin() ? jobRepo.findAllByOrderByUpdatedAtDesc()
                                : jobRepo.findByOwnerIdOrderByUpdatedAtDesc(cu.id()));
        Set<Long> visible = new HashSet<>();
        List<Map<String, Object>> nodes = new ArrayList<>();
        for (JobDefinition j : jobs) {
            visible.add(j.getId());
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", j.getId());
            m.put("name", j.getName());
            m.put("status", j.getStatus().name());
            m.put("ownerName", j.getOwnerName());
            nodes.add(m);
        }
        List<Map<String, Object>> edges = new ArrayList<>();
        for (JobDependency dep : depRepo.findAllByOrderByCreatedAtAsc()) {
            if (visible.contains(dep.getUpstreamJobId()) && visible.contains(dep.getDownstreamJobId())) {
                Map<String, Object> e = new LinkedHashMap<>();
                e.put("source", dep.getUpstreamJobId());
                e.put("target", dep.getDownstreamJobId());
                edges.add(e);
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("jobs", nodes);
        result.put("edges", edges);
        return result;
    }

    /**
     * 扫描 WAITING / BLOCKED 作业：上游全部 COMPLETED → 自动提交下游；
     * 上游出现 FAILED / CANCELLED → 下游 BLOCKED（仅状态迁移时告警一次）
     */
    public void scanAndTrigger() {
        List<JobDefinition.JobStatus> activeStatuses = List.of(
                JobDefinition.JobStatus.WAITING, JobDefinition.JobStatus.BLOCKED);
        List<JobDefinition> waitingJobs = jobRepo.findByStatusIn(activeStatuses);
        for (JobDefinition job : waitingJobs) {
            try {
                List<JobDependency> deps = depRepo.findByDownstreamJobId(job.getId());
                if (deps.isEmpty()) {
                    if (job.getStatus() == JobDefinition.JobStatus.WAITING
                            || job.getStatus() == JobDefinition.JobStatus.BLOCKED) {
                        job.setStatus(JobDefinition.JobStatus.DRAFT);
                        jobRepo.save(job);
                    }
                    continue;
                }
                List<JobDefinition> upstreams = new ArrayList<>();
                for (JobDependency dep : deps) {
                    jobRepo.findById(dep.getUpstreamJobId()).ifPresent(upstreams::add);
                }
                boolean anyFailed = upstreams.stream().anyMatch(u -> u.getStatus() == JobDefinition.JobStatus.FAILED
                        || u.getStatus() == JobDefinition.JobStatus.CANCELLED);
                boolean allDone = upstreams.stream().allMatch(u -> u.getStatus() == JobDefinition.JobStatus.COMPLETED);

                if (allDone) {
                    JobDefinition submitted = jobService.submit(job.getId());
                    addLog(job.getId(), "INFO", "上游作业全部完成，已自动触发下游作业（Flink Job ID: " + submitted.getFlinkJobId() + "）");
                    log.info("Dependency trigger: job {} auto-submitted after upstreams completed", job.getId());
                } else if (anyFailed) {
                    if (job.getStatus() != JobDefinition.JobStatus.BLOCKED) {
                        job.setStatus(JobDefinition.JobStatus.BLOCKED);
                        jobRepo.save(job);
                        addLog(job.getId(), "ERROR", "上游作业失败，下游作业已阻塞（BLOCKED）");
                        alertService.sendAlert(job, "CRITICAL", "DEPENDENCY_BLOCKED", "上游作业失败，下游作业 " + job.getName() + " 已阻塞");
                    }
                } else {
                    if (job.getStatus() != JobDefinition.JobStatus.WAITING) {
                        job.setStatus(JobDefinition.JobStatus.WAITING);
                        jobRepo.save(job);
                    }
                }
            } catch (Exception e) {
                addLog(job.getId(), "WARN", "依赖自动触发失败: " + e.getMessage());
                log.warn("Dependency scan failed for job {}: {}", job.getId(), e.getMessage());
            }
        }
    }

    private boolean createsCycle(Long downstreamId, List<JobDefinition> upstreamJobs, List<Long> upstreamIds) {
        // 全量依赖图（已有 + 新增）
        Map<Long, List<Long>> upstreamMap = new HashMap<>();
        for (JobDependency dep : depRepo.findAll()) {
            upstreamMap.computeIfAbsent(dep.getDownstreamJobId(), k -> new ArrayList<>()).add(dep.getUpstreamJobId());
        }
        for (Long upId : upstreamIds) {
            upstreamMap.computeIfAbsent(downstreamId, k -> new ArrayList<>()).add(upId);
        }
        // 从每个新增上游沿上游链向上走，若能回到 downstreamId → 成环
        for (Long upId : upstreamIds) {
            if (canReach(upId, downstreamId, upstreamMap)) return true;
        }
        return false;
    }

    private boolean canReach(Long start, Long target, Map<Long, List<Long>> upstreamMap) {
        Set<Long> visited = new HashSet<>();
        Deque<Long> stack = new ArrayDeque<>();
        stack.push(start);
        while (!stack.isEmpty()) {
            Long cur = stack.pop();
            if (cur.equals(target)) return true;
            if (!visited.add(cur)) continue;
            for (Long up : upstreamMap.getOrDefault(cur, List.of())) {
                stack.push(up);
            }
        }
        return false;
    }

    private void assertAccess(JobDefinition job, CurrentUser cu) {
        if (cu == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        if (cu.isAdmin()) return;
        if (job.getOwnerId() != null && !job.getOwnerId().equals(cu.id())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "无权访问该作业");
        }
    }

    private void addLog(Long jobId, String level, String message) {
        JobLog entry = new JobLog();
        entry.setJobId(jobId);
        entry.setLevel(level);
        entry.setMessage(message);
        entry.setTimestamp(LocalDateTime.now());
        logRepo.save(entry);
    }
}
