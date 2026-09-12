package com.datastream.mvp.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanguageDetectorTest {

    @Test
    void chineseInput_isNotEnglish() {
        assertFalse(LanguageDetector.isEnglish("读取 sales.csv，把 product_category 和 channel 拼接成新列 category_channel，写出到 MySQL 表 ai_demo"));
    }

    @Test
    void englishInput_isEnglish() {
        assertTrue(LanguageDetector.isEnglish("Read sales.csv, concatenate product_category and channel into category_channel, write to MySQL table ai_demo"));
    }

    @Test
    void blankOrNull_isNotEnglish() {
        assertFalse(LanguageDetector.isEnglish(null));
        assertFalse(LanguageDetector.isEnglish("   "));
    }

    @Test
    void mixedHeavilyChinese_isNotEnglish() {
        assertFalse(LanguageDetector.isEnglish("把 data1.csv 和 data2.csv 合并"));
    }
}
