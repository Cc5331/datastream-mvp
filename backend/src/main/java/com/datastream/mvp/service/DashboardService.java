package com.datastream.mvp.service;

import com.datastream.mvp.model.AppUser;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.AppUserRepository;
import com.datastream.mvp.security.CurrentUser;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class DashboardService {
    private final JobService jobService;
    private final AlertRecordRepository alertRepo;
    private final AppUserRepository userRepo;

    public Map<String, Object> workbench(CurrentUser user) {
        List<JobDefinition> jobs = jobService.findAllForUser(user);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("jobStatus", statusCounts(jobs));
        result.put("totalJobs", jobs.size());
        result.put("onlineJobs", jobs.stream().filter(j -> Boolean.TRUE.equals(j.getOnline())).count());
        result.put("unreadAlerts", user.isAdmin() ? alertRepo.countByReadFlagFalse()
                : alertRepo.countByOwnerIdAndReadFlagFalse(user.id()));
        result.put("recentJobs", jobs.stream().limit(10).map(this::jobSummary).toList());
        return result;
    }

    public Map<String, Object> adminOverview(CurrentUser user) {
        Map<String, Object> result = new LinkedHashMap<>(workbench(user));
        Map<String, Long> roles = new LinkedHashMap<>();
        for (AppUser.UserRole role : AppUser.UserRole.values()) {
            roles.put(role.name(), userRepo.countByRoleAndEnabledTrue(role));
        }
        result.put("enabledUsersByRole", roles);
        result.put("totalUsers", userRepo.count());
        result.put("recentAlerts", alertRepo.findAllByOrderByCreatedAtDesc(PageRequest.of(0, 10)).getContent());
        return result;
    }

    private Map<String, Long> statusCounts(List<JobDefinition> jobs) {
        Map<JobDefinition.JobStatus, Long> counts = jobs.stream()
                .collect(Collectors.groupingBy(JobDefinition::getStatus,
                        () -> new EnumMap<>(JobDefinition.JobStatus.class), Collectors.counting()));
        Map<String, Long> result = new LinkedHashMap<>();
        for (JobDefinition.JobStatus status : JobDefinition.JobStatus.values()) {
            result.put(status.name(), counts.getOrDefault(status, 0L));
        }
        return result;
    }

    private Map<String, Object> jobSummary(JobDefinition job) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("id", job.getId());
        summary.put("name", job.getName());
        summary.put("status", job.getStatus());
        summary.put("online", job.getOnline());
        summary.put("ownerName", job.getOwnerName());
        summary.put("updatedAt", job.getUpdatedAt());
        summary.put("submittedAt", job.getSubmittedAt());
        return summary;
    }
}
