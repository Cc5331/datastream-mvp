package com.datastream.mvp.repository;

import com.datastream.mvp.model.DataSourceConnection;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DataSourceConnectionRepository extends JpaRepository<DataSourceConnection, Long> {
    List<DataSourceConnection> findAllByOrderByUpdatedAtDesc();
    List<DataSourceConnection> findByOwnerIdOrderByUpdatedAtDesc(Long ownerId);
    boolean existsByOwnerIdAndNameIgnoreCase(Long ownerId, String name);
    boolean existsByOwnerIdAndNameIgnoreCaseAndIdNot(Long ownerId, String name, Long id);
}
