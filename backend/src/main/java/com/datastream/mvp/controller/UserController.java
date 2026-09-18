package com.datastream.mvp.controller;

import com.datastream.mvp.audit.Audit;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
@PreAuthorize("hasRole('ADMIN')")
public class UserController {
    private final UserService userService;

    public record ResetPasswordRequest(String password) {}

    /** 批量请求：ids 为待操作用户 id；enabled 仅批量状态接口使用（true=启用，false=禁用） */
    public record BatchUsersRequest(List<Long> ids, Boolean enabled) {}

    @GetMapping
    public List<UserService.UserView> list() {
        return userService.findAll();
    }

    @PostMapping
    @Audit(action = "USER_CREATE", targetType = "USER", targetId = "#result.id")
    public UserService.UserView create(@RequestBody UserService.CreateUserRequest request) {
        return userService.create(request);
    }

    @PutMapping("/{id}")
    @Audit(action = "USER_UPDATE", targetType = "USER", targetId = "#id")
    public UserService.UserView update(@PathVariable Long id,
                                       @RequestBody UserService.UpdateUserRequest request) {
        return userService.update(id, request);
    }

    @PutMapping("/{id}/password")
    @Audit(action = "USER_PASSWORD_RESET", targetType = "USER", targetId = "#id")
    public ResponseEntity<Void> resetPassword(@PathVariable Long id,
                                              @RequestBody ResetPasswordRequest request) {
        userService.resetPassword(id, request == null ? null : request.password());
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{id}")
    @Audit(action = "USER_DELETE", targetType = "USER", targetId = "#id")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        userService.delete(id, currentUser().id());
        return ResponseEntity.noContent().build();
    }

    /**
     * 批量启用/禁用。逐项独立处理，返回 {succeeded:[id], failed:[{id,message}], total}，
     * 前端据此提示"成功 N 失败 M + 原因"（与作业批量上线/下线同一套交互）。
     */
    @PostMapping("/batch-status")
    @Audit(action = "USER_BATCH_STATUS", targetType = "USER",
            detail = "'批量更新用户状态 ' + (#request.ids() == null ? 0 : #request.ids().size()) + ' 个'")
    public Map<String, Object> batchStatus(@RequestBody BatchUsersRequest request) {
        return userService.batchUpdateStatus(request.ids(), Boolean.TRUE.equals(request.enabled()), currentUser().id());
    }

    @PostMapping("/batch-delete")
    @Audit(action = "USER_BATCH_DELETE", targetType = "USER",
            detail = "'批量删除用户 ' + (#request.ids() == null ? 0 : #request.ids().size()) + ' 个'")
    public Map<String, Object> batchDelete(@RequestBody BatchUsersRequest request) {
        return userService.batchDelete(request.ids(), currentUser().id());
    }

    private CurrentUser currentUser() {
        CurrentUser user = SecurityUtils.currentUser();
        if (user == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return user;
    }
}
