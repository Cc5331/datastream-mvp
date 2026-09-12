package com.datastream.mvp.service;

import com.datastream.mvp.model.AppUser;
import com.datastream.mvp.repository.AppUserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.server.ResponseStatusException;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class UserServiceTest {
    private AppUserRepository userRepo;
    private PasswordEncoder passwordEncoder;
    private UserService service;

    @BeforeEach
    void setUp() {
        userRepo = mock(AppUserRepository.class);
        passwordEncoder = mock(PasswordEncoder.class);
        service = new UserService(userRepo, passwordEncoder);
    }

    @Test
    void createHashesPasswordAndDoesNotExposeHash() {
        when(userRepo.existsByUsername("new_user")).thenReturn(false);
        when(passwordEncoder.encode("strongPass1")).thenReturn("encoded");
        when(userRepo.save(any())).thenAnswer(invocation -> {
            AppUser user = invocation.getArgument(0);
            user.setId(10L);
            return user;
        });

        UserService.UserView result = service.create(new UserService.CreateUserRequest(
                " new_user ", "strongPass1", "新用户", AppUser.UserRole.OPERATOR, true));

        assertEquals("new_user", result.username());
        assertEquals("OPERATOR", result.role());
        verify(passwordEncoder).encode("strongPass1");
    }

    @Test
    void createRejectsDuplicateUsername() {
        when(userRepo.existsByUsername("existing")).thenReturn(true);
        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> service.create(new UserService.CreateUserRequest(
                        "existing", "strongPass1", null, null, null)));
        assertEquals(409, ex.getStatusCode().value());
        verify(userRepo, never()).save(any());
    }

    @Test
    void updateRejectsDisablingLastAdmin() {
        AppUser admin = user(1L, AppUser.UserRole.ADMIN, true);
        when(userRepo.findById(1L)).thenReturn(Optional.of(admin));
        when(userRepo.countByRoleAndEnabledTrue(AppUser.UserRole.ADMIN)).thenReturn(1L);

        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> service.update(1L, new UserService.UpdateUserRequest(null, null, false)));
        assertEquals(400, ex.getStatusCode().value());
    }

    @Test
    void deleteRejectsCurrentUser() {
        when(userRepo.findById(1L)).thenReturn(Optional.of(user(1L, AppUser.UserRole.ADMIN, true)));
        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> service.delete(1L, 1L));
        assertEquals(400, ex.getStatusCode().value());
        verify(userRepo, never()).delete(any());
    }

    @Test
    void resetPasswordValidatesAndHashes() {
        AppUser viewer = user(2L, AppUser.UserRole.VIEWER, true);
        when(userRepo.findById(2L)).thenReturn(Optional.of(viewer));
        when(passwordEncoder.encode("newStrong9")).thenReturn("newHash");

        service.resetPassword(2L, "newStrong9");

        assertEquals("newHash", viewer.getPasswordHash());
        verify(userRepo).save(viewer);
    }

    private AppUser user(Long id, AppUser.UserRole role, boolean enabled) {
        AppUser user = new AppUser();
        user.setId(id);
        user.setUsername("user" + id);
        user.setDisplayName("用户" + id);
        user.setPasswordHash("hash");
        user.setRole(role);
        user.setEnabled(enabled);
        return user;
    }
}
