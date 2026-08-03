package com.datastream.mvp.plugin;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.repository.ControlRegistryRepository;
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
                            if (ControlPlugin.class.isAssignableFrom(clazz) && !clazz.isInterface()) {
                                ControlPlugin plugin = (ControlPlugin) clazz.getDeclaredConstructor().newInstance();
                                registerPlugin(plugin, jarName);
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

    private void registerPlugin(ControlPlugin plugin, String jarName) {
        Optional<ControlRegistry> existing = controlRepo.findByType(plugin.getType());

        ControlRegistry control = existing.orElse(new ControlRegistry());
        control.setType(plugin.getType());
        control.setName(plugin.getName());
        control.setCategory(plugin.getCategory());
        control.setDescription(plugin.getDescription());
        control.setParamSchema(plugin.getParamSchema());
        control.setFlinkTemplate(plugin.getFlinkTemplate());
        control.setVersion(plugin.getVersion());
        control.setJarPath(jarName);
        control.setEnabled(true);
        control.setUpdatedAt(LocalDateTime.now());

        if (existing.isEmpty()) {
            control.setCreatedAt(LocalDateTime.now());
        }

        controlRepo.save(control);
        log.info("Plugin registered: {} (type={}, jar={})", plugin.getName(), plugin.getType(), jarName);
    }
}