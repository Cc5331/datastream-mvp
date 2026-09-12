package com.datastream.mvp.service;

import com.datastream.mvp.model.AppUser;
import com.datastream.mvp.repository.AppUserRepository;
import lombok.RequiredArgsConstructor;
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

    public record UserView(Long id, String username, String displayName, String role,
                           boolean enabled, LocalDateTime createdAt) {}

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

    @Transactional
    public void resetPassword(Long id, String password) {
        validatePassword(password);
        AppUser user = find(id);
        user.setPasswordHash(passwordEncoder.encode(password));
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

    private String normalizeDisplayName(String displayName, String username) {
        String value = displayName == null ? "" : displayName.trim();
        if (value.length() > 64) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "显示名称不能超过 64 个字符");
        }
        return value.isEmpty() ? username : value;
    }

    private UserView view(AppUser user) {
        return new UserView(user.getId(), user.getUsername(), user.getDisplayName(), user.getRole().name(),
                user.isEnabled(), user.getCreatedAt());
    }
}
