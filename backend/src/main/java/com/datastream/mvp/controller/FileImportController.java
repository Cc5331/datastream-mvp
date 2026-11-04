package com.datastream.mvp.controller;

import com.datastream.mvp.service.FileImportService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

/**
 * 画布「拖拽文件导入」接口：文件暂存到 output/uploads 并返回可直接落成输入控件的建议参数。
 *
 * <p>权限与数据预览保持一致（ADMIN/OPERATOR）：上传会写入服务器磁盘、分析结果会回显文件内容
 * （列名 + 前 N 行），VIEWER 只读角色不开放。</p>
 */
@RestController
@RequestMapping("/api/files")
@RequiredArgsConstructor
@PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
public class FileImportController {

    private final FileImportService fileImportService;

    /** 上传并分析文件（画布拖拽文件后调用），返回确认对话框所需的识别结果。 */
    @PostMapping("/analyze")
    public Map<String, Object> analyze(@RequestParam("file") MultipartFile file) {
        return fileImportService.analyze(file);
    }

    /** 用户在确认框点「取消」时丢弃暂存文件（仅限 uploads 目录内）。 */
    @DeleteMapping("/uploaded")
    public Map<String, Object> discard(@RequestParam("path") String path) {
        return fileImportService.discard(path);
    }
}
