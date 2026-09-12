package com.datastream.mvp.controller;

import com.datastream.mvp.model.AppUser;
import com.datastream.mvp.repository.AppUserRepository;
import com.datastream.mvp.security.JwtUtil;
import com.datastream.mvp.service.AuditService;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.server.ResponseStatusException;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AuthControllerTest {
    private AppUserRepository userRepo;
    private PasswordEncoder encoder;
    private JwtUtil jwtUtil;
    private AuditService auditService;
    private HttpServletRequest request;
    private AuthController controller;

    @BeforeEach
    void setUp() {
        userRepo = mock(AppUserRepository.class);
        encoder = mock(PasswordEncoder.class);
        jwtUtil = mock(JwtUtil.class);
        auditService = mock(AuditService.class);
        request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        controller = new AuthController(userRepo, encoder, jwtUtil, auditService);
    }

    @Test
    void register_createsEnabledViewerWithEncodedPassword() {
        when(userRepo.existsByUsername("new_user")).thenReturn(false);
        when(encoder.encode("secure123")).thenReturn("encoded");
        when(userRepo.save(any(AppUser.class))).thenAnswer(invocation -> {
            AppUser saved = invocation.getArgument(0);
            saved.setId(9L);
            return saved;
        });

        AuthController.UserView response = controller.register(
                new AuthController.RegisterRequest(" new_user ", " 新用户 ", "secure123"), request);

        assertEquals(9L, response.id());
        assertEquals("new_user", response.username());
        assertEquals("新用户", response.displayName());
        assertEquals("VIEWER", response.role());
        verify(userRepo).save(argThat(user -> user.isEnabled()
                && user.getRole() == AppUser.UserRole.VIEWER
                && "encoded".equals(user.getPasswordHash())));
        verify(auditService).record(9L, "new_user", "REGISTER", "USER", "new_user",
                "自助注册成功，默认角色 VIEWER", "127.0.0.1");
    }

    @Test
    void register_rejectsDuplicateUsername() {
        when(userRepo.existsByUsername("existing")).thenReturn(true);

        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> controller.register(new AuthController.RegisterRequest("existing", "用户", "secure123"), request));

        assertEquals(409, ex.getStatusCode().value());
        verify(userRepo, never()).save(any());
    }

    @Test
    void register_rejectsWeakPassword() {
        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> controller.register(new AuthController.RegisterRequest("new_user", "用户", "password"), request));

        assertEquals(400, ex.getStatusCode().value());
        verifyNoInteractions(encoder);
    }

    @Test
    void login_returnsTokenForEnabledUser() {
        AppUser user = user();
        when(userRepo.findByUsername("admin")).thenReturn(Optional.of(user));
        when(encoder.matches("secret", user.getPasswordHash())).thenReturn(true);
        when(jwtUtil.generateToken(1L, "admin", "管理员", "ADMIN")).thenReturn("jwt-token");

        AuthController.LoginResponse response = controller.login(
                new AuthController.LoginRequest(" admin ", "secret"), request);

        assertEquals("jwt-token", response.token());
        assertEquals("ADMIN", response.user().role());
        verify(auditService).record(1L, "admin", "LOGIN", "USER", "admin", "登录成功", "127.0.0.1");
    }

    @Test
    void login_rejectsInvalidPasswordAndAuditsFailure() {
        AppUser user = user();
        when(userRepo.findByUsername("admin")).thenReturn(Optional.of(user));
        when(encoder.matches("wrong", user.getPasswordHash())).thenReturn(false);

        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> controller.login(new AuthController.LoginRequest("admin", "wrong"), request));

        assertEquals(401, ex.getStatusCode().value());
        verify(auditService).record(null, "admin", "LOGIN_FAILED", "USER", "admin",
                "登录失败：用户名或密码错误", "127.0.0.1");
    }

    @Test
    void login_rejectsDisabledUserWithoutPasswordCheck() {
        AppUser user = user();
        user.setEnabled(false);
        when(userRepo.findByUsername("admin")).thenReturn(Optional.of(user));

        assertThrows(ResponseStatusException.class,
                () -> controller.login(new AuthController.LoginRequest("admin", "secret"), request));
        verifyNoInteractions(encoder, jwtUtil);
    }

    private AppUser user() {
        AppUser user = new AppUser();
        user.setId(1L);
        user.setUsername("admin");
        user.setDisplayName("管理员");
        user.setPasswordHash("hash");
        user.setRole(AppUser.UserRole.ADMIN);
        user.setEnabled(true);
        return user;
    }
}
