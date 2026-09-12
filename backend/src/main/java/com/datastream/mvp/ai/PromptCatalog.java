package com.datastream.mvp.ai;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Prompt 模板目录：从 classpath:ai-prompts/ 加载双语 NL2Pipeline 提示词。
 * 模板使用 {{registry}} 占位符，运行时替换为实际控件注册表摘要。
 * 资源缺失时回退为内置兜底提示词（保证离线/未打包运行时仍可用）。
 */
@Slf4j
@Component
public class PromptCatalog {

    private String zhTemplate;
    private String enTemplate;

    @PostConstruct
    public void load() {
        zhTemplate = loadOrDefault("ai-prompts/nl2pipeline.zh.txt", null);
        enTemplate = loadOrDefault("ai-prompts/nl2pipeline.en.txt", null);
    }

    public String nl2PipelineChinese(String registry) {
        return (zhTemplate == null ? "" : zhTemplate).replace("{{registry}}", registry);
    }

    public String nl2PipelineEnglish(String registry) {
        return (enTemplate == null ? "" : enTemplate).replace("{{registry}}", registry);
    }

    private String loadOrDefault(String path, String fallback) {
        try {
            var res = new ClassPathResource(path);
            if (res.exists()) {
                return new String(res.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.warn("load prompt template {} failed: {}", path, e.getMessage());
        }
        return fallback;
    }
}
