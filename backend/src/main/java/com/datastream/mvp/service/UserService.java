package com.datastream.mvp.service;

import com.datastream.mvp.model.AppUser;
import com.datastream.mvp.repository.AppUserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class UserService {
    private final AppUserRepository userRepo;
    private final PasswordEncoder passwordEncoder;

    @Value("${app.user-presence.timeout-ms:90000}")
    private long presenceTimeoutMs = 90000;

    @Value("${app.user-presence.minimum-write-interval-ms:20000}")
    private long minimumPresenceWriteIntervalMs = 20000;

    public record UserView(Long id, String username, String displayName, String role,
                           boolean enabled, LocalDateTime createdAt) {}

    public record ProfileView(Long id, String username, String displayName, String role,
                              String signature, AppUser.UserStatus statusPreference,
                              boolean present, AppUser.UserStatus publicStatus, LocalDateTime lastLoginAt,
                              LocalDateTime lastSeenAt, LocalDateTime updatedAt,
                              boolean avatarConfigured, long avatarVersion) {}

    public record ProfileUpdateRequest(String displayName, String signature,
                                       AppUser.UserStatus statusPreference) {}

    public record UsernameUpdateRequest(String username, String currentPassword) {}
    public record PasswordUpdateRequest(String currentPassword, String newPassword) {}

    public record CreateUserRequest(String username, String password, String displayName,
                                    AppUser.UserRole role, Boolean enabled) {}

    public record UpdateUserRequest(String displayName, AppUser.UserRole role, Boolean enabled) {}

    public List<UserView> findAll() {
        return userRepo.findAll().stream().map(this::view).toList();
    }

    @Transactional
    public UserView create(CreateUserRequest request) {
        String username = normalizeUsername(request == null ? null : request.username());
        validatePassword(request == null ? null : request.password());
        if (userRepo.existsByUsername(username)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "用户名已存在");
        }
        AppUser user = new AppUser();
        user.setUsername(username);
        user.setPasswordHash(passwordEncoder.encode(request.password()));
        user.setDisplayName(normalizeDisplayName(request.displayName(), username));
        user.setRole(request.role() == null ? AppUser.UserRole.VIEWER : request.role());
        user.setEnabled(request.enabled() == null || request.enabled());
        user.setCreatedAt(LocalDateTime.now());
        return view(userRepo.save(user));
    }

    @Transactional
    public UserView update(Long id, UpdateUserRequest request) {
        AppUser user = find(id);
        AppUser.UserRole nextRole = request != null && request.role() != null ? request.role() : user.getRole();
        boolean nextEnabled = request == null || request.enabled() == null ? user.isEnabled() : request.enabled();
        protectLastAdmin(user, nextRole, nextEnabled);
        if (request != null && request.displayName() != null) {
            user.setDisplayName(normalizeDisplayName(request.displayName(), user.getUsername()));
        }
        user.setRole(nextRole);
        user.setEnabled(nextEnabled);
        return view(userRepo.save(user));
    }

    public ProfileView profile(Long id) {
        return profileView(find(id));
    }

    @Transactional
    public ProfileView updateProfile(Long id, ProfileUpdateRequest request) {
        if (request == null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "资料不能为空");
        AppUser user = find(id);
        user.setDisplayName(normalizeDisplayName(request.displayName(), user.getUsername()));
        String signature = request.signature() == null ? "" : request.signature().trim();
        if (signature.length() > 200) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "个性签名不能超过 200 个字符");
        }
        user.setSignature(signature);
        user.setStatusPreference(request.statusPreference() == null ? status(user) : request.statusPreference());
        user.setUpdatedAt(LocalDateTime.now());
        return profileView(userRepo.save(user));
    }

    @Transactional
    public ProfileView updateUsername(Long id, UsernameUpdateRequest request) {
        if (request == null || request.currentPassword() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "请输入当前密码");
        }
        AppUser user = find(id);
        if (!passwordEncoder.matches(request.currentPassword(), user.getPasswordHash())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "当前密码错误");
        }
        String username = normalizeUsername(request.username());
        if (!username.equals(user.getUsername()) && userRepo.existsByUsername(username)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "用户名已存在");
        }
        user.setUsername(username);
        user.setUpdatedAt(LocalDateTime.now());
        return profileView(userRepo.save(user));
    }

    @Transactional
    public void updateOwnPassword(Long id, PasswordUpdateRequest request) {
        if (request == null || request.currentPassword() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "请输入当前密码");
        }
        AppUser user = find(id);
        if (!passwordEncoder.matches(request.currentPassword(), user.getPasswordHash())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "当前密码错误");
        }
        validateStrongPassword(request.newPassword());
        if (passwordEncoder.matches(request.newPassword(), user.getPasswordHash())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "新密码不能与当前密码相同");
        }
        user.setPasswordHash(passwordEncoder.encode(request.newPassword()));
        user.setUpdatedAt(LocalDateTime.now());
        bumpTokenVersion(user);
        userRepo.save(user);
    }

    /**
     * 递增令牌版本：使该账号此前签发的所有 JWT 立即失效。
     * 改密码、管理员重置密码、主动登出都走这里。
     */
    @Transactional
    public void revokeTokens(Long id) {
        AppUser user = find(id);
        bumpTokenVersion(user);
        user.setUpdatedAt(LocalDateTime.now());
        userRepo.save(user);
    }

    private void bumpTokenVersion(AppUser user) {
        int current = user.getTokenVersion() == null ? 0 : user.getTokenVersion();
        user.setTokenVersion(current + 1);
    }

    @Transactional
    public ProfileView markLogin(Long id) {
        AppUser user = find(id);
        LocalDateTime now = LocalDateTime.now();
        user.setLastLoginAt(now);
        user.setLastSeenAt(now);
        return profileView(userRepo.save(user));
    }

    @Transactional
    public void heartbeat(Long id) {
        LocalDateTime now = LocalDateTime.now();
        userRepo.touchPresence(id, now, now.minusNanos(minimumPresenceWriteIntervalMs * 1_000_000));
    }

    @Transactional
    public void markOffline(Long id) {
        userRepo.clearPresence(id);
    }

    public record AvatarChange(String oldKey, ProfileView profile) {}

    @Transactional
    public synchronized AvatarChange replaceAvatar(Long id, String avatarKey) {
        AppUser user = find(id);
        String oldKey = user.getAvatarKey();
        user.setAvatarKey(avatarKey);
        user.setUpdatedAt(LocalDateTime.now());
        userRepo.save(user);
        return new AvatarChange(oldKey, profileView(user));
    }

    public AppUser requireUser(Long id) {
        return find(id);
    }

    @Transactional
    public void resetPassword(Long id, String password) {
        validatePassword(password);
        AppUser user = find(id);
        user.setPasswordHash(passwordEncoder.encode(password));
        // 管理员重置密码后，被重置账号的旧 token 必须失效
        bumpTokenVersion(user);
        user.setUpdatedAt(LocalDateTime.now());
        userRepo.save(user);
    }

    @Transactional
    public void delete(Long id, Long currentUserId) {
        AppUser user = find(id);
        if (id.equals(currentUserId)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "不能删除当前登录账号");
        }
        protectLastAdmin(user, null, false);
        userRepo.delete(user);
    }

    private AppUser find(Long id) {
        return userRepo.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "用户不存在"));
    }

    private void protectLastAdmin(AppUser user, AppUser.UserRole nextRole, boolean nextEnabled) {
        boolean removingActiveAdmin = user.getRole() == AppUser.UserRole.ADMIN && user.isEnabled()
                && (nextRole != AppUser.UserRole.ADMIN || !nextEnabled);
        if (removingActiveAdmin && userRepo.countByRoleAndEnabledTrue(AppUser.UserRole.ADMIN) <= 1) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "系统必须保留至少一个启用的管理员");
        }
    }

    private String normalizeUsername(String username) {
        String value = username == null ? "" : username.trim();
        if (!value.matches("[A-Za-z0-9_]{3,32}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "用户名只能包含字母、数字、下划线，长度 3-32 位");
        }
        return value;
    }

    private void validatePassword(String password) {
        if (password == null || password.length() < 8 || password.length() > 128) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "密码长度必须为 8-128 位");
        }
    }

    private void validateStrongPassword(String password) {
        validatePassword(password);
        if (!password.matches(".*[A-Za-z].*") || !password.matches(".*\\d.*")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "新密码必须同时包含字母和数字");
        }
    }

    private String normalizeDisplayName(String displayName, String username) {
        String value = displayName == null ? "" : displayName.trim();
        if (value.length() > 64) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "显示名称不能超过 64 个字符");
        }
        return value.isEmpty() ? username : value;
    }

    private AppUser.UserStatus status(AppUser user) {
        return user.getStatusPreference() == null ? AppUser.UserStatus.ONLINE : user.getStatusPreference();
    }

    private ProfileView profileView(AppUser user) {
        boolean present = user.isEnabled() && user.getLastSeenAt() != null
                && user.getLastSeenAt().isAfter(LocalDateTime.now().minusNanos(presenceTimeoutMs * 1_000_000));
        AppUser.UserStatus preference = status(user);
        AppUser.UserStatus publicStatus = present && preference != AppUser.UserStatus.INVISIBLE
                && preference != AppUser.UserStatus.OFFLINE ? preference : AppUser.UserStatus.OFFLINE;
        long avatarVersion = user.getUpdatedAt() == null ? 0
                : user.getUpdatedAt().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        return new ProfileView(user.getId(), user.getUsername(), user.getDisplayName(), user.getRole().name(),
                user.getSignature() == null ? "" : user.getSignature(), preference, present, publicStatus,
                user.getLastLoginAt(), user.getLastSeenAt(), user.getUpdatedAt(),
                user.getAvatarKey() != null && !user.getAvatarKey().isBlank(), avatarVersion);
    }

    private UserView view(AppUser user) {
        return new UserView(user.getId(), user.getUsername(), user.getDisplayName(), user.getRole().name(),
                user.isEnabled(), user.getCreatedAt());
    }
}
