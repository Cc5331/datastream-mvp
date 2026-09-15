package com.datastream.mvp.repository;

import com.datastream.mvp.model.AppUser;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.Optional;

public interface AppUserRepository extends JpaRepository<AppUser, Long> {
    Optional<AppUser> findByUsername(String username);
    boolean existsByUsername(String username);
    long countByRoleAndEnabledTrue(AppUser.UserRole role);

    @Modifying
    @Query("update AppUser u set u.lastSeenAt = :now where u.id = :id and (u.lastSeenAt is null or u.lastSeenAt < :threshold)")
    int touchPresence(@Param("id") Long id, @Param("now") LocalDateTime now,
                      @Param("threshold") LocalDateTime threshold);

    @Modifying
    @Query("update AppUser u set u.lastSeenAt = null where u.id = :id")
    int clearPresence(@Param("id") Long id);
}
