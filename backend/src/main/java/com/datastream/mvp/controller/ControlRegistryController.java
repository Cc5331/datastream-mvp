package com.datastream.mvp.controller;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.service.ControlRegistryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 控件注册表 REST API
 */
@RestController
@RequestMapping("/api/controls")
@RequiredArgsConstructor
public class ControlRegistryController {

    private final ControlRegistryService service;

    @GetMapping
    public List<ControlRegistry> list(@RequestParam(required = false) String category) {
        if (category != null && !category.isBlank()) {
            return service.findByCategory(category);
        }
        return service.findAll();
    }

    @GetMapping("/{id}")
    public ControlRegistry get(@PathVariable Long id) {
        return service.findById(id);
    }

    @GetMapping("/type/{type}")
    public ControlRegistry getByType(@PathVariable String type) {
        return service.findByType(type);
    }

    @PostMapping
    public ControlRegistry create(@RequestBody ControlRegistry control) {
        return service.create(control);
    }

    @PutMapping("/{id}")
    public ControlRegistry update(@PathVariable Long id, @RequestBody ControlRegistry control) {
        return service.update(id, control);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        service.delete(id);
        return ResponseEntity.ok().build();
    }
}
