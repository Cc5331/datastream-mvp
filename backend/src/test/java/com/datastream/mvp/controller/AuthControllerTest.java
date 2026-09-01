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
