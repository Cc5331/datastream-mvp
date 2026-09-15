package com.datastream.mvp.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.server.ResponseStatusException;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class AvatarStorageServiceTest {
    @TempDir
    Path tempDir;

    @Test
    void storesNormalizedPngAndDeletesIt() throws Exception {
        AvatarStorageService service = new AvatarStorageService(tempDir.toString());
        BufferedImage image = new BufferedImage(800, 400, BufferedImage.TYPE_INT_RGB);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ImageIO.write(image, "jpeg", output);
        MockMultipartFile file = new MockMultipartFile("file", "../../avatar.jpg", "image/jpeg", output.toByteArray());

        String key = service.store(file);
        byte[] stored = service.read(key);

        assertTrue(key.matches("[0-9a-f-]{36}\\.png"));
        BufferedImage normalized = ImageIO.read(new java.io.ByteArrayInputStream(stored));
        assertTrue(normalized.getWidth() <= 512);
        assertTrue(normalized.getHeight() <= 512);
        assertEquals(2.0, (double) normalized.getWidth() / normalized.getHeight(), 0.01);
        service.delete(key);
        assertThrows(ResponseStatusException.class, () -> service.read(key));
    }

    @Test
    void rejectsUnsupportedOrOversizedFiles() {
        AvatarStorageService service = new AvatarStorageService(tempDir.toString());
        assertEquals(400, assertThrows(ResponseStatusException.class,
                () -> service.store(new MockMultipartFile("file", "x.svg", "image/svg+xml", "<svg/>".getBytes())))
                .getStatusCode().value());
        assertEquals(413, assertThrows(ResponseStatusException.class,
                () -> service.store(new MockMultipartFile("file", "x.png", "image/png", new byte[2 * 1024 * 1024 + 1])))
                .getStatusCode().value());
    }

    @Test
    void rejectsFakeImageContent() {
        AvatarStorageService service = new AvatarStorageService(tempDir.toString());
        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> service.store(new MockMultipartFile("file", "x.png", "image/png", "not-image".getBytes())));
        assertEquals(400, ex.getStatusCode().value());
    }
}
