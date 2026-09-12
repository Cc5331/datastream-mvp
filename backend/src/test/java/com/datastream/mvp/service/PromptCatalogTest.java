package com.datastream.mvp.service;

import com.datastream.mvp.ai.PromptCatalog;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PromptCatalogTest {

    @Test
    void loadsChineseTemplate_andSubstitutesRegistry() {
        PromptCatalog catalog = new PromptCatalog();
        catalog.load();
        String prompt = catalog.nl2PipelineChinese("- type=csv_input\n");
        assertTrue(prompt.contains("可视化 DAG"));
        assertTrue(prompt.contains("- type=csv_input"));
        assertTrue(!prompt.contains("{{registry}}"));
    }

    @Test
    void loadsEnglishTemplate_andSubstitutesRegistry() {
        PromptCatalog catalog = new PromptCatalog();
        catalog.load();
        String prompt = catalog.nl2PipelineEnglish("- type=csv_input\n");
        assertTrue(prompt.contains("visual DAG generator"));
        assertTrue(prompt.contains("- type=csv_input"));
    }
}
