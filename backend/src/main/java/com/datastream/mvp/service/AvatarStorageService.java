package com.datastream.mvp.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.Locale;
import java.util.UUID;

@Service
public class AvatarStorageService {
    private static final long MAX_BYTES = 2L * 1024 * 1024;
    private static final int MAX_SOURCE_EDGE = 4096;
    private static final long MAX_PIXELS = 4096L * 4096L;
    private static final int OUTPUT_EDGE = 512;
    private final Path root;

    public AvatarStorageService(@Value("${app.profile.avatar-dir:./data/avatars}") String avatarDir) {
        this.root = Path.of(avatarDir).toAbsolutePath().normalize();
    }

    public String store(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "请选择头像文件");
        }
        if (file.getSize() > MAX_BYTES) {
            throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "头像不能超过 2MB");
        }
        String contentType = file.getContentType();
        if (!("image/jpeg".equals(contentType) || "image/png".equals(contentType))) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "头像仅支持 JPEG、PNG 格式");
        }
        try (InputStream input = file.getInputStream(); ImageInputStream imageInput = ImageIO.createImageInputStream(input)) {
            if (imageInput == null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "头像文件不是有效图片");
            Iterator<ImageReader> readers = ImageIO.getImageReaders(imageInput);
            if (!readers.hasNext()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "头像文件不是有效图片");
            ImageReader reader = readers.next();
            BufferedImage source;
            try {
                reader.setInput(imageInput, true, true);
                String format = reader.getFormatName().toLowerCase(Locale.ROOT);
                if (!("jpeg".equals(format) || "jpg".equals(format) || "png".equals(format))) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "头像仅支持 JPEG、PNG 格式");
                }
                int width = reader.getWidth(0);
                int height = reader.getHeight(0);
                if (width < 1 || height < 1 || width > MAX_SOURCE_EDGE || height > MAX_SOURCE_EDGE
                        || (long) width * height > MAX_PIXELS) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "头像像素尺寸无效或超过 4096px");
                }
                ImageReadParam param = reader.getDefaultReadParam();
                int subsampling = Math.max(1, (int) Math.ceil((double) Math.max(width, height) / OUTPUT_EDGE));
                param.setSourceSubsampling(subsampling, subsampling, 0, 0);
                source = reader.read(0, param);
            } finally {
                reader.dispose();
            }
            BufferedImage normalized = normalize(source);
            Files.createDirectories(root);
            String key = UUID.randomUUID() + ".png";
            Path temp = Files.createTempFile(root, "avatar-", ".tmp");
            try {
                if (!ImageIO.write(normalized, "png", temp.toFile())) throw new IOException("PNG encoder unavailable");
                Files.move(temp, resolve(key), StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(temp);
            }
            return key;
        } catch (ResponseStatusException e) {
            throw e;
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "头像保存失败");
        }
    }

    public byte[] read(String key) {
        if (key == null || key.isBlank()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "未设置头像");
        try {
            Path file = resolve(key);
            if (!Files.isRegularFile(file)) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "头像不存在");
            return Files.readAllBytes(file);
        } catch (ResponseStatusException e) {
            throw e;
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "头像读取失败");
        }
    }

    public void delete(String key) {
        if (key == null || key.isBlank()) return;
        try {
            Files.deleteIfExists(resolve(key));
        } catch (IOException ignored) {
            // 数据库状态优先；删除失败的旧文件不可被 API 访问
        }
    }

    private Path resolve(String key) {
        if (!key.matches("[0-9a-fA-F-]{36}\\.png")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "头像标识非法");
        }
        Path result = root.resolve(key).normalize();
        if (!result.startsWith(root)) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "头像路径非法");
        return result;
    }

    private BufferedImage normalize(BufferedImage source) {
        double scale = Math.min(1D, (double) OUTPUT_EDGE / Math.max(source.getWidth(), source.getHeight()));
        int width = Math.max(1, (int) Math.round(source.getWidth() * scale));
        int height = Math.max(1, (int) Math.round(source.getHeight() * scale));
        BufferedImage output = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D graphics = output.createGraphics();
        try {
            graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
            graphics.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
            graphics.drawImage(source, 0, 0, width, height, null);
        } finally {
            graphics.dispose();
        }
        return output;
    }
}
