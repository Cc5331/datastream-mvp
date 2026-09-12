package com.datastream.mvp.util;

import java.util.regex.Pattern;

/**
 * 简单语言检测：判断用户输入以中文还是英文为主导，用于选择 NL2Pipeline 的双语 Prompt。
 * 判定规则：统计文本中 CJK 字符与 ASCII 拉丁字母的占比，CJK 占比更高判为 zh，否则 en。
 */
public final class LanguageDetector {

    private static final Pattern CJK = Pattern.compile("[\\u4E00-\\u9FFF\\u3400-\\u4DBF]");
    private static final Pattern LATIN = Pattern.compile("[A-Za-z]");

    private LanguageDetector() {}

    public static boolean isEnglish(String text) {
        if (text == null || text.isBlank()) return false;
        int latin = 0;
        for (char c : text.toCharArray()) {
            if (CJK.matcher(String.valueOf(c)).find()) {
                // 出现任何 CJK 字符即视为中文输入（技术领域中文必然混大量英文标识符，看 CJK 有无更可靠）
                return false;
            }
            if (LATIN.matcher(String.valueOf(c)).find()) latin++;
        }
        return latin > 0;
    }
}
