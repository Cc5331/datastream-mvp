package com.datastream.mvp.service;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.repository.ControlRegistryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 控件注册表服务
 */
@Service
@RequiredArgsConstructor
public class ControlRegistryService {

    private final ControlRegistryRepository repository;

    public List<ControlRegistry> findAll() {
        return repository.findByEnabledTrue();
    }

    public List<ControlRegistry> findByCategory(String category) {
        return repository.findByCategory(category);
    }

    public ControlRegistry findById(Long id) {
        return repository.findById(id).orElseThrow(() ->
                new RuntimeException("Control not found: id=" + id));
    }

    public ControlRegistry findByType(String type) {
        return repository.findByType(type).orElseThrow(() ->
                new RuntimeException("Control not found: type=" + type));
    }

    public ControlRegistry create(ControlRegistry control) {
        control.setCreatedAt(LocalDateTime.now());
        control.setUpdatedAt(LocalDateTime.now());
        return repository.save(control);
    }

    public ControlRegistry update(Long id, ControlRegistry update) {
        ControlRegistry existing = findById(id);
        existing.setName(update.getName());
        existing.setCategory(update.getCategory());
        existing.setDescription(update.getDescription());
        existing.setParamSchema(update.getParamSchema());
        existing.setFlinkTemplate(update.getFlinkTemplate());
        existing.setVersion(update.getVersion());
        existing.setJarPath(update.getJarPath());
        existing.setEnabled(update.getEnabled());
        existing.setUpdatedAt(LocalDateTime.now());
        return repository.save(existing);
    }

    public void delete(Long id) {
        repository.deleteById(id);
    }
}
