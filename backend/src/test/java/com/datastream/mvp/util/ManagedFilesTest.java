package com.datastream.mvp.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagedFilesTest {
    @TempDir
    Path tempDir;

    @Test
    void deletesOnlyChildrenOfManagedRoot() throws Exception {
        Path root = Files.createDirectory(tempDir.resolve("output"));
        Path target = Files.createDirectories(root.resolve("job.tmp"));
        Files.writeString(target.resolve("part-0"), "data");
        Path outside = Files.writeString(tempDir.resolve("outside.txt"), "keep");

        assertTrue(ManagedFiles.deleteRecursively(target, root.toString()));
        assertFalse(Files.exists(target));
        assertFalse(ManagedFiles.deleteRecursively(outside, root.toString()));
        assertTrue(Files.exists(outside));
        assertFalse(ManagedFiles.deleteRecursively(root, root.toString()));
    }
}
