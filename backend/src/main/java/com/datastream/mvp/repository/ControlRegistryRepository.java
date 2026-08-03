package com.datastream.mvp.repository;

import com.datastream.mvp.model.ControlRegistry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ControlRegistryRepository extends JpaRepository<ControlRegistry, Long> {
    Optional<ControlRegistry> findByType(String type);
    List<ControlRegistry> findByCategory(String category);
    List<ControlRegistry> findByEnabledTrue();
}
