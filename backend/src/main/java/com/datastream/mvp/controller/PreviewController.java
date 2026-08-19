package com.datastream.mvp.controller;

import com.datastream.mvp.service.PreviewService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 数据预览接口：按文件类型读取前 N 行，供前端画布节点“预览数据”。
 */
@RestController
@RequestMapping("/api/preview")
@RequiredArgsConstructor
public class PreviewController {

    private final PreviewService previewService;

    @GetMapping("/file")
    public Map<String, Object> previewFile(@RequestParam String path,
                                           @RequestParam(defaultValue = "20") int limit) {
        return previewService.previewFile(path, limit);
    }
}
