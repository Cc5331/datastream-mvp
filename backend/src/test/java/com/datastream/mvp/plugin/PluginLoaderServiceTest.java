package com.datastream.mvp.plugin;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.repository.ControlRegistryRepository;
import com.datastream.plugin.DataStreamPlugin;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * 插件 SPI 识别与注册（2026-09-16 修复）：
 * 历史上加载器只认内置的 ControlPlugin，而 README 与 plugin-sdk 让第三方实现 DataStreamPlugin
 * （backend/plugin-example 也是后者），导致插件被静默跳过、从来加载不了。
 */
class PluginLoaderServiceTest {

    /** 按 SDK 接口实现的插件（对外文档推荐写法） */
    static class SdkPlugin implements DataStreamPlugin {
        public String getType() { return "demo_probe_plugin"; }
        public String getName() { return "探针插件"; }
        public String getCategory() { return "transform"; }
        public String getDescription() { return "用于验证插件链路"; }
        public String getParamSchema() { return "{\"type\":\"object\",\"properties\":{}}"; }
        public String getFlinkTemplate() { return "SELECT * FROM ${source}"; }
        public String getVersion() { return "1.0.0"; }
    }

    /** 按内置接口实现的插件（历史写法，需继续兼容） */
    static class LegacyPlugin implements ControlPlugin {
        public String getType() { return "legacy_probe_plugin"; }
        public String getName() { return "老式插件"; }
        public String getCategory() { return "transform"; }
        public String getDescription() { return "兼容内置 SPI"; }
        public String getParamSchema() { return "{\"type\":\"object\",\"properties\":{}}"; }
        public String getFlinkTemplate() { return "SELECT * FROM ${source}"; }
        public String getVersion() { return "0.9.0"; }
    }

    static class NotAPlugin {
        public String getType() { return "nope"; }
    }

    @Test
    void isPluginClass_acceptsBothSpiInterfaces() {
        assertTrue(PluginLoaderService.isPluginClass(SdkPlugin.class), "SDK 接口（DataStreamPlugin）必须被识别");
        assertTrue(PluginLoaderService.isPluginClass(LegacyPlugin.class), "内置接口（ControlPlugin）必须继续兼容");
        assertFalse(PluginLoaderService.isPluginClass(NotAPlugin.class));
        assertFalse(PluginLoaderService.isPluginClass(DataStreamPlugin.class), "接口自身不是插件实现");
        assertFalse(PluginLoaderService.isPluginClass(null));
    }

    @Test
    void registerPlugin_fromSdkInterface_writesRegistryRow() {
        ControlRegistryRepository repo = mock(ControlRegistryRepository.class);
        when(repo.findByType("demo_probe_plugin")).thenReturn(Optional.empty());
        when(repo.save(any())).thenAnswer(i -> i.getArgument(0));
        PluginLoaderService service = new PluginLoaderService(repo);
        ArgumentCaptor<ControlRegistry> captor = ArgumentCaptor.forClass(ControlRegistry.class);

        service.registerPlugin(new SdkPlugin(), "probe-plugin-1.0.0.jar");

        verify(repo).save(captor.capture());
        ControlRegistry saved = captor.getValue();
        assertEquals("demo_probe_plugin", saved.getType());
        assertEquals("探针插件", saved.getName());
        assertEquals("probe-plugin-1.0.0.jar", saved.getJarPath(), "jar 来源必须记录，供与内置控件区分");
        assertEquals("1.0.0", saved.getVersion());
        assertEquals(Boolean.TRUE, saved.getEnabled());
        assertNotNull(saved.getCreatedAt());
    }

    @Test
    void registerPlugin_keepsBuiltInDefinitionOnTypeCollision() {
        ControlRegistryRepository repo = mock(ControlRegistryRepository.class);
        ControlRegistry builtIn = new ControlRegistry();
        builtIn.setType("demo_probe_plugin");
        builtIn.setName("内置定义");
        builtIn.setJarPath("built-in");
        when(repo.findByType("demo_probe_plugin")).thenReturn(Optional.of(builtIn));
        PluginLoaderService service = new PluginLoaderService(repo);

        service.registerPlugin(new SdkPlugin(), "probe-plugin-1.0.0.jar");

        verify(repo, never()).save(any());
        assertEquals("内置定义", builtIn.getName(), "同 type 冲突时保留内置定义");
        assertEquals("built-in", builtIn.getJarPath());
    }

    /**
     * 插件 jar 被移出目录后，其控件行必须清理，否则控件库里会留下一个永远加载不出来的孤儿控件；
     * 内置控件（jarPath=built-in）绝不能因此被删。
     */
    @Test
    void removeOrphanPluginControls_deletesOnlyMissingPluginRows() throws Exception {
        ControlRegistryRepository repo = mock(ControlRegistryRepository.class);
        ControlRegistry builtIn = new ControlRegistry();
        builtIn.setType("csv_input");
        builtIn.setJarPath("built-in");
        ControlRegistry kept = new ControlRegistry();
        kept.setType("alive_plugin");
        kept.setJarPath("alive-plugin-1.0.0.jar");
        ControlRegistry orphan = new ControlRegistry();
        orphan.setType("removed_plugin");
        orphan.setJarPath("gone-plugin-1.0.0.jar");
        when(repo.findAll()).thenReturn(java.util.List.of(builtIn, kept, orphan));
        PluginLoaderService service = new PluginLoaderService(repo);

        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("plugin-dir");
        java.nio.file.Files.write(dir.resolve("alive-plugin-1.0.0.jar"), new byte[]{1, 2, 3});

        service.removeOrphanPluginControls(dir);

        verify(repo).delete(orphan);
        verify(repo, never()).delete(builtIn);
        verify(repo, never()).delete(kept);
    }
}
