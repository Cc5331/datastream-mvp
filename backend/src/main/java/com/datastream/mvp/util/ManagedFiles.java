package com.datastream.mvp.util;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public final class ManagedFiles {
    private ManagedFiles() {}

    public static boolean deleteRecursively(Path candidate, String configuredRoot) throws IOException {
        if (candidate == null || configuredRoot == null || configuredRoot.isBlank()) return false;
        Path root = Path.of(configuredRoot).toAbsolutePath().normalize();
        Path target = candidate.toAbsolutePath().normalize();
        if (!target.startsWith(root) || target.equals(root)) return false;
        if (!Files.exists(target)) return true;

        Files.walkFileTree(target, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) throw exc;
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
        return true;
    }
}
