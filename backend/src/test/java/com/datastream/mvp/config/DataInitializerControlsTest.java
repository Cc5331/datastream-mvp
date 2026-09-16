package com.datastream.mvp.config;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.AppUserRepository;
import com.datastream.mvp.repository.ControlRegistryRepository;
import com.datastream.mvp.repository.JobDefinitionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 内置控件播种的两条硬约束（2026-09-16 修复）：
 * 1) **不能清库重建**——插件加载器是 @PostConstruct（早于本 CommandLineRunner），
 *    历史上的 deleteAll 会把插件控件每次启动都抹掉；
 * 2) 按 type upsert——重复启动不产生重复行，且不重置管理员设置的 enabled。
 */
class DataInitializerControlsTest {

    private ControlRegistryRepository controlRepo;
    private DataInitializer initializer;
    /** 模拟库中现存数据：type -> 实体 */
    private Map<String, ControlRegistry> table;

    @BeforeEach
    void setUp() throws Exception {
        controlRepo = mock(ControlRegistryRepository.class);
        table = new LinkedHashMap<>();

        when(controlRepo.findByType(anyString()))
                .thenAnswer(inv -> Optional.ofNullable(table.get((String) inv.getArgument(0))));
        when(controlRepo.save(any())).thenAnswer(inv -> {
            ControlRegistry c = inv.getArgument(0);
            table.put(c.getType(), c);
            return c;
        });
        when(controlRepo.findAll()).thenAnswer(inv -> new ArrayList<>(table.values()));
        when(controlRepo.count()).thenAnswer(inv -> (long) table.size());
        doAnswer(inv -> {
            table.remove(((ControlRegistry) inv.getArgument(0)).getType());
            return null;
        }).when(controlRepo).delete(any());
        doAnswer(inv -> {
            table.clear();
            return null;
        }).when(controlRepo).deleteAll();

        initializer = new DataInitializer(controlRepo,
                mock(AppUserRepository.class), mock(JobDefinitionRepository.class),
                mock(AlertRecordRepository.class), mock(PasswordEncoder.class));
        // seedBuiltInControls 会用到路径前缀（默认值来自 application.yml，单测里手工注入）
        set("dataRoot", "../data");
        set("testResourcesRoot", "../test-resources");
        set("outputRoot", "../output");
    }

    private void set(String field, Object value) throws Exception {
        Field f = DataInitializer.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(initializer, value);
    }

    private ControlRegistry row(String type, String jarPath) {
        ControlRegistry c = new ControlRegistry();
        c.setType(type);
        c.setName("旧名称");
        c.setCategory("transform");
        c.setJarPath(jarPath);
        c.setEnabled(true);
        c.setCreatedAt(LocalDateTime.now().minusDays(3));
        return c;
    }

    @Test
    void seedUpdatesExistingRowInPlaceAndKeepsAdminEnabledState() {
        ControlRegistry existing = row("csv_input", "built-in");
        existing.setId(42L);
        existing.setEnabled(false);   // 管理员手工停用过
        LocalDateTime createdAt = existing.getCreatedAt();
        table.put("csv_input", existing);

        initializer.seedBuiltInControls();

        ControlRegistry after = table.get("csv_input");
        assertSame(existing, after, "同 type 必须原地更新，不能新建一行");
        assertEquals(42L, after.getId(), "主键不能变");
        assertEquals(createdAt, after.getCreatedAt(), "createdAt 必须保留");
        assertEquals(Boolean.FALSE, after.getEnabled(), "管理员设置的 enabled 不能被重启重置");
        assertNotEquals("旧名称", after.getName(), "内置定义（名称等）应被代码更新");
        assertTrue(after.getParamSchema().contains("path"), "paramSchema 应被刷新");
    }

    @Test
    void seedNeverDeletesPluginControls() {
        ControlRegistry plugin = row("demo_probe_plugin", "probe-plugin-1.0.0.jar");
        table.put(plugin.getType(), plugin);

        initializer.seedBuiltInControls();

        assertTrue(table.containsKey("demo_probe_plugin"), "插件控件在播种后必须仍然存在");
        verify(controlRepo, never()).delete(plugin);
        verify(controlRepo, never()).deleteAll();
    }

    @Test
    void seedRemovesOnlyStaleBuiltInControls() {
        ControlRegistry retired = row("retired_builtin_control", "built-in");
        table.put(retired.getType(), retired);
        ControlRegistry plugin = row("demo_probe_plugin", "probe.jar");
        table.put(plugin.getType(), plugin);

        initializer.seedBuiltInControls();

        assertFalse(table.containsKey("retired_builtin_control"), "已从代码移除的内置控件应被清理");
        assertTrue(table.containsKey("demo_probe_plugin"), "插件控件不受清理影响");
        verify(controlRepo).delete(retired);
    }

    @Test
    void seedIsIdempotentAcrossRestarts() {
        initializer.seedBuiltInControls();
        int firstRun = table.size();

        initializer.seedBuiltInControls();

        assertEquals(firstRun, table.size(), "重复播种不应产生重复控件");
        assertTrue(firstRun >= 31, "内置控件数量不少于 31（当前 31 个），实际 " + firstRun);
        // 每次播种都要把内置定义写回去（保证代码里的改动生效）
        verify(controlRepo, atLeast(firstRun)).save(any());
    }
}
