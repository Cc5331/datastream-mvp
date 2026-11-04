package com.datastream.mvp.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 文本编码探测与乱码修复（文件导入分析、数据预览共用一套口径）。
 *
 * <p><b>为什么需要它（2026-11-04 真实事故）</b>：编码探测只读文件头部 64KB 判断是不是 UTF-8，
 * 而采样窗口的最后一个字节**可能正好切在多字节汉字中间**（实测 orders.csv 第 65536 字节是
 * {@code e8 ae b0 …} 的前半个字），严格 UTF-8 解码报 {@code unexpected end of data} 后
 * 被误判成「不是 UTF-8」→ 回退 GB18030 → UTF-8 字节被当 GBK 解读，
 * 中文列变成 {@code 鐠虫満} 并夹杂 {@code \uFFFD} 替换符。用户看到的就是「分析结果乱码」。</p>
 *
 * <p><b>三条防错</b>：</p>
 * <ol>
 *   <li>采样解码用 {@code endOfInput=false}（{@link CharsetDecoder#decode(ByteBuffer, CharBuffer, boolean)}），
 *       缓冲区末尾被截断的半个字符不算错误 —— 这是本事故的根因修复；</li>
 *   <li>UTF-8 与 GB18030 都能「勉强」解出结果时，按**替换字符（\uFFFD）更少**的一方取胜，
 *       而不是无脑相信某一种；</li>
 *   <li>{@link #repairMojibake} 兜底：内容本身已经是「UTF-8 被按 GBK 解」的历史乱码时（比如别人
 *       用错编码导出过的文件），按 GB18030 编码回字节再用 UTF-8 严格解码即可还原。</li>
 * </ol>
 */
public final class TextEncoding {

    /** 探测窗口：只读头部，避免大文件整读 */
    public static final int SAMPLE_BYTES = 64 * 1024;

    private static final Charset GB18030 = Charset.forName("GB18030");
    private static final char REPLACEMENT = '\uFFFD';

    private TextEncoding() {}

    /** 探测文件编码（BOM 优先 → 严格 UTF-8 → GB18030 → 谁替换符少用谁）。 */
    public static Charset detect(Path file) {
        byte[] head = readHead(file, SAMPLE_BYTES);
        return detect(head);
    }

    /** 探测字节样本的编码。包级可见以便单测直接喂字节。 */
    public static Charset detect(byte[] head) {
        if (head == null || head.length == 0) return StandardCharsets.UTF_8;
        if (head.length >= 3 && (head[0] & 0xFF) == 0xEF && (head[1] & 0xFF) == 0xBB && (head[2] & 0xFF) == 0xBF) {
            return StandardCharsets.UTF_8;
        }
        if (head.length >= 2 && (head[0] & 0xFF) == 0xFF && (head[1] & 0xFF) == 0xFE) {
            return StandardCharsets.UTF_16LE;
        }
        if (head.length >= 2 && (head[0] & 0xFF) == 0xFE && (head[1] & 0xFF) == 0xFF) {
            return StandardCharsets.UTF_16BE;
        }
        if (decodesStrictly(head, StandardCharsets.UTF_8)) return StandardCharsets.UTF_8;
        if (decodesStrictly(head, GB18030)) return GB18030;
        // 两种都不严格可解（文件混了坏字节）：用替换符更少的那种，至少把能读的读出来
        int utf8Bad = replacementCount(head, StandardCharsets.UTF_8);
        int gbBad = replacementCount(head, GB18030);
        return gbBad < utf8Bad ? GB18030 : StandardCharsets.UTF_8;
    }

    /** 采样解码是否严格通过。**末尾被截断的多字节序列不算错误**（本事故的根因）。 */
    static boolean decodesStrictly(byte[] data, Charset charset) {
        CharsetDecoder decoder = charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        CharBuffer out = CharBuffer.allocate(Math.max(data.length, 1) + 16);
        // endOfInput=false：缓冲区末尾不完整的序列按 UNDERFLOW 处理，不报错 —— 这是根因修复点
        java.nio.charset.CoderResult result = decoder.decode(ByteBuffer.wrap(data), out, false);
        return !result.isError();
    }

    /** 宽松解码后统计替换符数量，用于两种编码都解不干净时择优。 */
    static int replacementCount(byte[] data, Charset charset) {
        String text = new String(data, charset);
        int count = 0;
        for (int i = 0; i < text.length(); i++) if (text.charAt(i) == REPLACEMENT) count++;
        return count;
    }

    /**
     * 乱码还原：内容形如「UTF-8 字节被按 GB18030 解读」时还原成原文，否则原样返回。
     *
     * <p>判定条件（宁可不动也不能改坏正常文本）：原文不含替换符、含中文；
     * 按 GB18030 编码回字节后能被 UTF-8 **严格**解码；结果非空且仍含中文。</p>
     */
    public static String repairMojibake(String value) {
        if (value == null || value.isEmpty()) return value;
        if (value.indexOf(REPLACEMENT) >= 0) return value;      // 字节已丢，还原只会瞎猜
        if (!containsCjk(value)) return value;                  // ASCII/数字/英文无需处理
        try {
            byte[] bytes = value.getBytes(GB18030);
            String repaired = strictUtf8(bytes);
            if (repaired == null || repaired.equals(value)) return value;
            if (!containsCjk(repaired)) return value;
            return repaired;
        } catch (Exception e) {
            return value;
        }
    }

    /** 对整行/整表的每个字段做乱码还原，返回是否有改动（供上层决定要不要提示用户）。 */
    public static boolean repairMojibakeInPlace(java.util.List<String> values) {
        boolean changed = false;
        for (int i = 0; i < values.size(); i++) {
            String original = values.get(i);
            String repaired = repairMojibake(original);
            if (repaired != null && !repaired.equals(original)) {
                values.set(i, repaired);
                changed = true;
            }
        }
        return changed;
    }

    /** 按探测结果一次性读入文本（小文件工具方法，供 JSON/XML 预览等使用）。 */
    public static String readAll(Path file) throws IOException {
        byte[] raw = Files.readAllBytes(file);
        return new String(raw, detect(raw));
    }

    // ===================== 内部工具 =====================

    private static byte[] readHead(Path file, int limit) {
        try (InputStream input = Files.newInputStream(file)) {
            return input.readNBytes(limit);
        } catch (IOException e) {
            return new byte[0];
        }
    }

    private static String strictUtf8(byte[] bytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static boolean containsCjk(String text) {
        for (int i = 0; i < text.length(); i++) {
            Character.UnicodeBlock block = Character.UnicodeBlock.of(text.charAt(i));
            if (block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                    || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                    || block == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS) {
                return true;
            }
        }
        return false;
    }
}
