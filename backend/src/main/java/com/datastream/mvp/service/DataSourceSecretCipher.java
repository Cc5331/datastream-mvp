package com.datastream.mvp.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

@Component
public class DataSourceSecretCipher {
    private static final int IV_LENGTH = 12;
    private static final SecureRandom RANDOM = new SecureRandom();
    private final Path keyFile;
    private volatile byte[] key;

    public DataSourceSecretCipher(@Value("${app.data-source.key-file:./data/.data_source_key}") String keyFile) {
        this.keyFile = Path.of(keyFile).toAbsolutePath().normalize();
    }

    public String encrypt(String plainText) {
        if (plainText == null || plainText.isEmpty()) return "";
        try {
            byte[] iv = new byte[IV_LENGTH];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key(), "AES"), new GCMParameterSpec(128, iv));
            byte[] encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            byte[] payload = new byte[iv.length + encrypted.length];
            System.arraycopy(iv, 0, payload, 0, iv.length);
            System.arraycopy(encrypted, 0, payload, iv.length, encrypted.length);
            return Base64.getEncoder().encodeToString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("数据源凭据加密失败", e);
        }
    }

    public String decrypt(String encoded) {
        if (encoded == null || encoded.isBlank()) return "";
        try {
            byte[] payload = Base64.getDecoder().decode(encoded);
            if (payload.length <= IV_LENGTH) throw new IllegalArgumentException("invalid payload");
            byte[] iv = java.util.Arrays.copyOfRange(payload, 0, IV_LENGTH);
            byte[] encrypted = java.util.Arrays.copyOfRange(payload, IV_LENGTH, payload.length);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key(), "AES"), new GCMParameterSpec(128, iv));
            return new String(cipher.doFinal(encrypted), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("数据源凭据解密失败，请检查加密密钥", e);
        }
    }

    private byte[] key() throws Exception {
        byte[] cached = key;
        if (cached != null) return cached;
        synchronized (this) {
            if (key == null) key = MessageDigest.getInstance("SHA-256").digest(secret().getBytes(StandardCharsets.UTF_8));
            return key;
        }
    }

    private String secret() throws Exception {
        String configured = firstNonBlank(System.getenv("DATA_SOURCE_ENCRYPTION_KEY"),
                System.getenv("APP_CONFIG_ENCRYPTION_KEY"), System.getenv("AI_CONFIG_ENCRYPTION_KEY"));
        if (configured != null) return configured;
        if (Files.isRegularFile(keyFile)) {
            String existing = Files.readString(keyFile, StandardCharsets.UTF_8).trim();
            if (!existing.isEmpty()) return existing;
        }
        Path parent = keyFile.getParent();
        if (parent != null) Files.createDirectories(parent);
        byte[] generated = new byte[32];
        RANDOM.nextBytes(generated);
        String value = Base64.getEncoder().encodeToString(generated);
        try {
            Files.writeString(keyFile, value, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            return value;
        } catch (java.nio.file.FileAlreadyExistsException e) {
            String existing = Files.readString(keyFile, StandardCharsets.UTF_8).trim();
            if (existing.isEmpty()) throw new IllegalStateException("数据源密钥文件为空");
            return existing;
        }
    }

    private String firstNonBlank(String... values) {
        for (String value : values) if (value != null && !value.isBlank()) return value.trim();
        return null;
    }
}
