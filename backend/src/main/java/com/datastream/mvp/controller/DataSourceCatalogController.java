package com.datastream.mvp.controller;

import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.DataSourceCatalogService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@RestController
@RequestMapping("/api/data-sources")
@RequiredArgsConstructor
public class DataSourceCatalogController {
    private final DataSourceCatalogService catalogService;

    @GetMapping("/catalog")
    public Map<String, Object> catalog() {
        CurrentUser user = SecurityUtils.currentUser();
        if (user == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return catalogService.catalog(user);
    }
}
