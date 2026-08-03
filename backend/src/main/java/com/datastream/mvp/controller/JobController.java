package com.datastream.mvp.controller;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 作业管理 REST API
 */
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;

    @GetMapping
    public List<JobDefinition> list() {
        return jobService.findAll();
    }

    @GetMapping("/{id}")
    public JobDefinition get(@PathVariable Long id) {
        return jobService.findById(id);
    }

    @PostMapping
    public JobDefinition create(@RequestBody JobDefinition job) {
        return jobService.create(job);
    }

    @PutMapping("/{id}")
    public JobDefinition update(@PathVariable Long id, @RequestBody JobDefinition job) {
        return jobService.update(id, job);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        jobService.delete(id);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{id}/submit")
    public JobDefinition submit(@PathVariable Long id) {
        return jobService.submit(id);
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<Void> cancel(@PathVariable Long id) {
        jobService.cancel(id);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{id}/logs")
    public List<JobLog> logs(@PathVariable Long id) {
        return jobService.getLogs(id);
    }

    @GetMapping("/{id}/preview")
    public List<String> preview(@PathVariable Long id) {
        return jobService.preview(id);
    }
}