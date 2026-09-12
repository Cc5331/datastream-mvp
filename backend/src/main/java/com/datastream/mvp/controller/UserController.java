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

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
@PreAuthorize("hasRole('ADMIN')")
public class UserController {
    private final UserService userService;

    public record ResetPasswordRequest(String password) {}

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

    private CurrentUser currentUser() {
        CurrentUser user = SecurityUtils.currentUser();
        if (user == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return user;
    }
}
