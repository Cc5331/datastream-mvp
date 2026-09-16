package com.datastream.mvp.plugin;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.repository.ControlRegistryRepository;
import com.datastream.plugin.DataStreamPlugin;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

@Slf4j
@Service
public class PluginLoaderService {

    @Value("${plugin.dir:./plugins}")
    private String pluginDir;

    private final ControlRegistryRepository controlRepo;
    private final Map<String, URLClassLoader> classLoaders = new HashMap<>();
    private WatchService watchService;

    public PluginLoaderService(ControlRegistryRepository controlRepo) {
        this.controlRepo = controlRepo;
    }

    @PostConstruct
    public void init() throws Exception {
        Path dir = Paths.get(pluginDir);
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }

        loadAllPlugins(dir);
        removeOrphanPluginControls(dir);

        watchService = FileSystems.getDefault().newWatchService();
        dir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY);

        new Thread(() -> {
            try {
                while (true) {
                    WatchKey key = watchService.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        Path changed = (Path) event.context();
                        if (changed.toString().endsWith(".jar")) {
                            log.info("Detected plugin change: {}, reloading...", changed);
                            loadPlugin(dir.resolve(changed));
                        }
                    }
                    key.reset();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "plugin-watcher").start();

        log.info("PluginLoader initialized, watching directory: {}", pluginDir);
    }

    private void loadAllPlugins(Path dir) {
        File[] jars = dir.toFile().listFiles((d, name) -> name.endsWith(".jar"));
        if (jars != null) {
            for (File jar : jars) {
                loadPlugin(jar.toPath());
            }
        }
    }

    /**
     * 清理「jar 已不在插件目录」的插件控件行，保证注册表与插件目录一致。
     * 只处理 jarPath != built-in 的行，内置控件永不触碰。
     */
    void removeOrphanPluginControls(Path dir) {
        Set<String> presentJars = new HashSet<>();
        File[] jars = dir.toFile().listFiles((d, name) -> name.endsWith(".jar"));
        if (jars != null) {
            for (File jar : jars) presentJars.add(jar.getName());
        }
        int removed = 0;
        for (ControlRegistry c : controlRepo.findAll()) {
            String jar = c.getJarPath();
            if (jar == null || jar.isBlank() || "built-in".equals(jar)) continue;
            if (!presentJars.contains(jar)) {
                controlRepo.delete(c);
                removed++;
                log.info("Removed control of missing plugin: {} (jar={})", c.getType(), jar);
            }
        }
        if (removed > 0) {
            log.info("Cleaned {} orphan plugin control(s)", removed);
        }
    }

    private void loadPlugin(Path jarPath) {
        try {
            String jarName = jarPath.getFileName().toString();

            if (classLoaders.containsKey(jarName)) {
                classLoaders.get(jarName).close();
            }

            URLClassLoader classLoader = new URLClassLoader(
                    new URL[]{jarPath.toUri().toURL()},
                    Thread.currentThread().getContextClassLoader()
            );

            try (JarFile jar = new JarFile(jarPath.toFile())) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    if (entry.getName().endsWith(".class") && !entry.getName().contains("$")) {
                        String className = entry.getName().replace("/", ".").replace(".class", "");
                        try {
                            Class<?> clazz = classLoader.loadClass(className);
                            if (isPluginClass(clazz)) {
                                registerPlugin(clazz.getDeclaredConstructor().newInstance(), jarName);
                            }
                        } catch (NoClassDefFoundError | Exception e) {
                            // skip non-plugin classes
                        }
                    }
                }
            }

            classLoaders.put(jarName, classLoader);
            log.info("Plugin loaded successfully: {}", jarName);

        } catch (Exception e) {
            log.error("Failed to load plugin: {}", jarPath, e);
        }
    }

    /**
     * 是否为可加载的控件插件类。
     * 同时接受两种 SPI：内置的 {@link ControlPlugin} 与对外发布的 SDK 接口
     * {@link DataStreamPlugin}（plugin-sdk）。历史实现只认前者，
     * 导致按文档实现了 DataStreamPlugin 的插件（含 backend/plugin-example）被静默跳过。
     */
    static boolean isPluginClass(Class<?> clazz) {
        if (clazz == null || clazz.isInterface()) return false;
        return ControlPlugin.class.isAssignableFrom(clazz) || DataStreamPlugin.class.isAssignableFrom(clazz);
    }

    /** 从任意一种 SPI 实现上取字段（两种接口方法签名一致，只是包不同） */
    private static String field(Object plugin, String what) {
        if (plugin instanceof ControlPlugin p) {
            return switch (what) {
                case "type" -> p.getType();
                case "name" -> p.getName();
                case "category" -> p.getCategory();
                case "description" -> p.getDescription();
                case "paramSchema" -> p.getParamSchema();
                case "flinkTemplate" -> p.getFlinkTemplate();
                default -> p.getVersion();
            };
        }
        DataStreamPlugin p = (DataStreamPlugin) plugin;
        return switch (what) {
            case "type" -> p.getType();
            case "name" -> p.getName();
            case "category" -> p.getCategory();
            case "description" -> p.getDescription();
            case "paramSchema" -> p.getParamSchema();
            case "flinkTemplate" -> p.getFlinkTemplate();
            default -> p.getVersion();
        };
    }

    /**
     * 注册/更新插件控件。
     * - 与内置控件同 type 时**保留内置定义**（内置控件在翻译层有专门分支，插件覆盖会产生"注册表说是插件、实际按内置跑"的错觉）；
     * - 已存在的行保留管理员设置的 enabled 与 createdAt。
     */
    void registerPlugin(Object plugin, String jarName) {
        String type = field(plugin, "type");
        if (type == null || type.isBlank()) {
            log.warn("Skip plugin without type: jar={}", jarName);
            return;
        }
        Optional<ControlRegistry> existing = controlRepo.findByType(type);
        if (existing.isPresent() && "built-in".equals(existing.get().getJarPath())) {
            log.warn("Plugin type '{}' collides with a built-in control, keeping built-in definition (plugin jar={})。"
                    + "请为插件使用独立 type。", type, jarName);
            return;
        }

        ControlRegistry control = existing.orElse(new ControlRegistry());
        control.setType(type);
        control.setName(field(plugin, "name"));
        control.setCategory(field(plugin, "category"));
        control.setDescription(field(plugin, "description"));
        control.setParamSchema(field(plugin, "paramSchema"));
        control.setFlinkTemplate(field(plugin, "flinkTemplate"));
        control.setVersion(field(plugin, "version"));
        control.setJarPath(jarName);
        control.setUpdatedAt(LocalDateTime.now());

        if (existing.isEmpty()) {
            control.setEnabled(true);
            control.setCreatedAt(LocalDateTime.now());
        }

        controlRepo.save(control);
        log.info("Plugin registered: {} (type={}, jar={})", control.getName(), type, jarName);
    }
}