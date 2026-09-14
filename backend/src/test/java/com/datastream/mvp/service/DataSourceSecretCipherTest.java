package com.datastream.mvp.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DataSourceSecretCipherTest {
    @TempDir Path tempDir;

    @Test
    void encryptionUsesRandomIvAndRoundTrips() {
        DataSourceSecretCipher cipher = new DataSourceSecretCipher(tempDir.resolve("key").toString());
        String first = cipher.encrypt("secret-value");
        String second = cipher.encrypt("secret-value");

        assertNotEquals(first, second);
        assertEquals("secret-value", cipher.decrypt(first));
        assertTrue(tempDir.resolve("key").toFile().isFile());
    }

    @Test
    void decryptWithDifferentKeyFails() throws Exception {
        Path firstDir = java.nio.file.Files.createDirectory(tempDir.resolve("first"));
        Path secondDir = java.nio.file.Files.createDirectory(tempDir.resolve("second"));
        DataSourceSecretCipher first = new DataSourceSecretCipher(firstDir.resolve("key").toString());
        DataSourceSecretCipher second = new DataSourceSecretCipher(secondDir.resolve("key").toString());

        String encrypted = first.encrypt("secret-value");
        assertThrows(IllegalStateException.class, () -> second.decrypt(encrypted));
    }
}
