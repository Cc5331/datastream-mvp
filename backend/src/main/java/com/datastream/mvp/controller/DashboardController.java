package com.datastream.mvp.controller;

import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.DashboardService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@RestController
@RequestMapping("/api/dashboard")
@RequiredArgsConstructor
public class DashboardController {
    private final DashboardService dashboardService;

    @GetMapping("/workbench")
    public Map<String, Object> workbench() {
        return dashboardService.workbench(currentUser());
    }

    @GetMapping("/admin-overview")
    @PreAuthorize("hasRole('ADMIN')")
    public Map<String, Object> adminOverview() {
        return dashboardService.adminOverview(currentUser());
    }

    private CurrentUser currentUser() {
        CurrentUser user = SecurityUtils.currentUser();
        if (user == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return user;
    }
}
