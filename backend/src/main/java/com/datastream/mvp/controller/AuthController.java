package com.datastream.mvp.controller;

import com.datastream.mvp.model.AppUser;
import com.datastream.mvp.repository.AppUserRepository;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.JwtUtil;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.AuditService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/**
 * 登录 / 当前用户 / 登出
 */
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AppUserRepository userRepo;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;
    private final AuditService auditService;

    public record LoginRequest(String username, String password) {}
    public record RegisterRequest(String username, String displayName, String password) {}
    public record UserView(Long id, String username, String displayName, String role) {}
    public record LoginResponse(String token, UserView user) {}

    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    public UserView register(@RequestBody RegisterRequest req, HttpServletRequest httpReq) {
        String username = req.username() == null ? "" : req.username().trim();
        String displayName = req.displayName() == null ? "" : req.displayName().trim();
        String password = req.password() == null ? "" : req.password();
        if (!username.matches("[A-Za-z0-9_]{3,32}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "用户名须为 3-32 位字母、数字或下划线");
        }
        if (displayName.isEmpty() || displayName.length() > 50) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "显示名称须为 1-50 个字符");
        }
        if (password.length() < 8 || password.length() > 128
                || !password.matches(".*[A-Za-z].*") || !password.matches(".*\\d.*")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "密码须为 8-128 位且同时包含字母和数字");
        }
        if (userRepo.existsByUsername(username)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "用户名已存在");
        }

        AppUser user = new AppUser();
        user.setUsername(username);
        user.setDisplayName(displayName);
        user.setPasswordHash(passwordEncoder.encode(password));
        user.setRole(AppUser.UserRole.VIEWER);
        user.setEnabled(true);
        try {
            user = userRepo.save(user);
        } catch (DataIntegrityViolationException ex) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "用户名已存在");
        }
        auditService.record(user.getId(), user.getUsername(), "REGISTER", "USER", user.getUsername(),
                "自助注册成功，默认角色 VIEWER", httpReq.getRemoteAddr());
        return new UserView(user.getId(), user.getUsername(), user.getDisplayName(), user.getRole().name());
    }

    @PostMapping("/login")
    public LoginResponse login(@RequestBody LoginRequest req, HttpServletRequest httpReq) {
        String username = req.username() == null ? "" : req.username().trim();
        AppUser user = userRepo.findByUsername(username).orElse(null);
        if (user == null || !user.isEnabled()
                || !passwordEncoder.matches(req.password() == null ? "" : req.password(), user.getPasswordHash())) {
            auditService.record(null, username, "LOGIN_FAILED", "USER", username, "登录失败：用户名或密码错误", httpReq.getRemoteAddr());
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "用户名或密码错误");
        }
        String token = jwtUtil.generateToken(user.getId(), user.getUsername(), user.getDisplayName(), user.getRole().name());
        auditService.record(user.getId(), user.getUsername(), "LOGIN", "USER", user.getUsername(), "登录成功", httpReq.getRemoteAddr());
        return new LoginResponse(token, new UserView(user.getId(), user.getUsername(), user.getDisplayName(), user.getRole().name()));
    }

    @GetMapping("/me")
    public UserView me() {
        CurrentUser cu = SecurityUtils.currentUser();
        if (cu == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        AppUser user = userRepo.findById(cu.id())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.UNAUTHORIZED, "用户不存在"));
        return new UserView(user.getId(), user.getUsername(), user.getDisplayName(), user.getRole().name());
    }

    @PostMapping("/logout")
    public ResponseEntity<Void> logout(HttpServletRequest httpReq) {
        CurrentUser cu = SecurityUtils.currentUser();
        if (cu != null) {
            auditService.record(cu.id(), cu.username(), "LOGOUT", "USER", cu.username(), "登出", httpReq.getRemoteAddr());
        }
        return ResponseEntity.ok().build();
    }
}
