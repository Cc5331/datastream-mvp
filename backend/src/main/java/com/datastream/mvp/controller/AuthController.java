package com.datastream.mvp.controller;

import com.datastream.mvp.model.AppUser;
import com.datastream.mvp.repository.AppUserRepository;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.JwtUtil;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.AuditService;
import com.datastream.mvp.service.AvatarStorageService;
import com.datastream.mvp.service.UserService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.TimeUnit;

/**
 * 登录 / 当前用户资料 / 在线状态 / 登出
 */
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AppUserRepository userRepo;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;
    private final AuditService auditService;
    private final UserService userService;
    private final AvatarStorageService avatarStorageService;

    public record LoginRequest(String username, String password) {}
    public record RegisterRequest(String username, String displayName, String password) {}
    public record LoginResponse(String token, UserService.ProfileView user) {}

    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    public UserService.ProfileView register(@RequestBody RegisterRequest req, HttpServletRequest httpReq) {
        String username = req.username() == null ? "" : req.username().trim();
        String displayName = req.displayName() == null ? "" : req.displayName().trim();
        String password = req.password() == null ? "" : req.password();
        if (!username.matches("[A-Za-z0-9_]{3,32}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "用户名须为 3-32 位字母、数字或下划线");
        }
        if (displayName.isEmpty() || displayName.length() > 64) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "显示名称须为 1-64 个字符");
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
        return userService.profile(user.getId());
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
        String token = jwtUtil.generateToken(user.getId(), user.getUsername(), user.getDisplayName(), user.getRole().name(),
                user.getTokenVersion());
        UserService.ProfileView profile = userService.markLogin(user.getId());
        auditService.record(user.getId(), user.getUsername(), "LOGIN", "USER", user.getUsername(), "登录成功", httpReq.getRemoteAddr());
        return new LoginResponse(token, profile);
    }

    @GetMapping("/me")
    public UserService.ProfileView me() {
        return userService.profile(currentUser().id());
    }

    @PutMapping("/me")
    public UserService.ProfileView updateProfile(@RequestBody UserService.ProfileUpdateRequest request,
                                                  HttpServletRequest httpReq) {
        CurrentUser current = currentUser();
        UserService.ProfileView profile = userService.updateProfile(current.id(), request);
        auditService.record(current.id(), current.username(), "PROFILE_UPDATE", "USER", current.username(),
                "更新个人资料", httpReq.getRemoteAddr());
        return profile;
    }

    @PutMapping("/me/username")
    public UserService.ProfileView updateUsername(@RequestBody UserService.UsernameUpdateRequest request,
                                                   HttpServletRequest httpReq) {
        CurrentUser current = currentUser();
        UserService.ProfileView profile = userService.updateUsername(current.id(), request);
        auditService.record(current.id(), profile.username(), "USERNAME_UPDATE", "USER", profile.username(),
                "修改登录用户名", httpReq.getRemoteAddr());
        return profile;
    }

    @PutMapping("/me/password")
    public ResponseEntity<Void> updatePassword(@RequestBody UserService.PasswordUpdateRequest request,
                                                HttpServletRequest httpReq) {
        CurrentUser current = currentUser();
        userService.updateOwnPassword(current.id(), request);
        auditService.record(current.id(), current.username(), "PASSWORD_UPDATE", "USER", current.username(),
                "修改登录密码", httpReq.getRemoteAddr());
        return ResponseEntity.noContent().build();
    }

    @PostMapping(value = "/me/avatar", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public UserService.ProfileView uploadAvatar(@RequestPart("file") MultipartFile file,
                                                 HttpServletRequest httpReq) {
        CurrentUser current = currentUser();
        String newKey = avatarStorageService.store(file);
        UserService.AvatarChange change;
        try {
            change = userService.replaceAvatar(current.id(), newKey);
        } catch (RuntimeException e) {
            avatarStorageService.delete(newKey);
            throw e;
        }
        avatarStorageService.delete(change.oldKey());
        auditService.record(current.id(), current.username(), "AVATAR_UPDATE", "USER", current.username(),
                "更新头像", httpReq.getRemoteAddr());
        return change.profile();
    }

    @GetMapping(value = "/me/avatar", produces = MediaType.IMAGE_PNG_VALUE)
    public ResponseEntity<byte[]> avatar() {
        AppUser user = userService.requireUser(currentUser().id());
        return ResponseEntity.ok()
                .cacheControl(CacheControl.maxAge(1, TimeUnit.DAYS).cachePrivate())
                .contentType(MediaType.IMAGE_PNG)
                .body(avatarStorageService.read(user.getAvatarKey()));
    }

    @DeleteMapping("/me/avatar")
    public UserService.ProfileView deleteAvatar(HttpServletRequest httpReq) {
        CurrentUser current = currentUser();
        UserService.AvatarChange change = userService.replaceAvatar(current.id(), null);
        avatarStorageService.delete(change.oldKey());
        auditService.record(current.id(), current.username(), "AVATAR_DELETE", "USER", current.username(),
                "删除头像", httpReq.getRemoteAddr());
        return change.profile();
    }

    @PostMapping("/presence/heartbeat")
    public ResponseEntity<Void> heartbeat() {
        userService.heartbeat(currentUser().id());
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/logout")
    public ResponseEntity<Void> logout(HttpServletRequest httpReq) {
        CurrentUser current = SecurityUtils.currentUser();
        if (current != null) {
            // 递增令牌版本：无状态 JWT 无法逐个吊销，登出即让该账号此前签发的所有 token 失效
            userService.revokeTokens(current.id());
            userService.markOffline(current.id());
            auditService.record(current.id(), current.username(), "LOGOUT", "USER", current.username(),
                    "登出（已吊销已签发令牌）", httpReq.getRemoteAddr());
        }
        return ResponseEntity.ok().build();
    }

    private CurrentUser currentUser() {
        CurrentUser current = SecurityUtils.currentUser();
        if (current == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return current;
    }
}
