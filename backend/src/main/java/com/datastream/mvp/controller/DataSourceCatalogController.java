package com.datastream.mvp.controller;

import com.datastream.mvp.audit.Audit;
import com.datastream.mvp.dto.DataSourceResponse;
import com.datastream.mvp.dto.DataSourceSaveRequest;
import com.datastream.mvp.dto.DataSourceTestResponse;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.DataSourceCatalogService;
import com.datastream.mvp.service.DataSourceConnectionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/data-sources")
@RequiredArgsConstructor
public class DataSourceCatalogController {
    private final DataSourceCatalogService catalogService;
    private final DataSourceConnectionService connectionService;

    @GetMapping("/catalog")
    public Map<String, Object> catalog() {
        return catalogService.catalog(currentUser());
    }

    @GetMapping
    public List<DataSourceResponse> list() {
        return connectionService.findAll(currentUser());
    }

    @GetMapping("/{id}")
    public DataSourceResponse get(@PathVariable Long id) {
        return connectionService.findById(id, currentUser());
    }

    @PostMapping
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "DATA_SOURCE_CREATE", targetType = "DATA_SOURCE", targetId = "#result.id", detail = "'创建数据源 ' + #result.name")
    public DataSourceResponse create(@RequestBody DataSourceSaveRequest request) {
        return connectionService.create(request, currentUser());
    }

    @PutMapping("/{id}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "DATA_SOURCE_UPDATE", targetType = "DATA_SOURCE", targetId = "#id", detail = "'更新数据源 ' + #result.name")
    public DataSourceResponse update(@PathVariable Long id, @RequestBody DataSourceSaveRequest request) {
        return connectionService.update(id, request, currentUser());
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "DATA_SOURCE_DELETE", targetType = "DATA_SOURCE", targetId = "#id", detail = "'删除数据源'")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        connectionService.delete(id, currentUser());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/test")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "DATA_SOURCE_TEST_UNSAVED", targetType = "DATA_SOURCE", detail = "'测试未保存数据源'")
    public DataSourceTestResponse testUnsaved(@RequestBody DataSourceSaveRequest request) {
        return connectionService.testUnsaved(request, currentUser());
    }

    @PostMapping("/{id}/test")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "DATA_SOURCE_TEST", targetType = "DATA_SOURCE", targetId = "#id", detail = "'测试已保存数据源'")
    public DataSourceTestResponse testSaved(@PathVariable Long id,
                                            @RequestBody(required = false) DataSourceSaveRequest override) {
        return connectionService.testSaved(id, override, currentUser());
    }

    private CurrentUser currentUser() {
        CurrentUser user = SecurityUtils.currentUser();
        if (user == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return user;
    }
}
