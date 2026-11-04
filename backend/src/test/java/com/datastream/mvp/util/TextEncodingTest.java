package com.datastream.mvp.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 编码探测与乱码还原回归。
 *
 * <p>背景（2026-11-04 真实事故）：探测只读头部 64KB 做严格 UTF-8 判断，采样窗口最后一个字节
 * 正好切在多字节汉字中间时，严格解码报 "unexpected end of data" → 误判成 GB18030 →
 * UTF-8 字节被当 GBK 解读，中文列变成 {@code 鐠虫満} 并夹杂 {@code \uFFFD}。</p>
 */
class TextEncodingTest {

    private static final Charset GB18030 = Charset.forName("GB18030");

    @TempDir
    Path tempDir;

    @Test
    void sampleEndingInsideMultiByteCharIsStillUtf8() {
        // 构造一个 64KB 采样：末尾只剩一个 3 字节汉字的**前两个字节**（被采样窗口截断）
        byte[] full = "平板".getBytes(StandardCharsets.UTF_8);    // e5 b9 b3 | e6 9d bf
        byte[] sample = new byte[TextEncoding.SAMPLE_BYTES];
        java.util.Arrays.fill(sample, (byte) 'a');
        sample[sample.length - 2] = full[0];                      // e5
        sample[sample.length - 1] = full[1];                      // b9（缺第三字节）

        assertEquals(StandardCharsets.UTF_8, TextEncoding.detect(sample),
                "采样末尾被截断的多字节序列不能当成「不是 UTF-8」");
    }

    @Test
    void detectsRealGbkSample() {
        byte[] gbk = "编号,名称,金额\n1,测试数据,12.5\n".getBytes(GB18030);
        Charset detected = TextEncoding.detect(gbk);
        assertEquals(GB18030, detected, "真 GBK 文件必须识别成 GB18030，才能正确读出中文");
        assertEquals("编号,名称,金额", new String(gbk, detected).split("\n")[0]);
    }

    @Test
    void detectsBomAndPlainAscii() {
        byte[] bom = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF, 'a', 'b'};
        assertEquals(StandardCharsets.UTF_8, TextEncoding.detect(bom));
        assertEquals(StandardCharsets.UTF_8, TextEncoding.detect("id,name\n1,a\n".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void repairsMojibakeFromWrongDoubleDecode() {
        // 模拟「UTF-8 字节被按 GBK 解」的历史乱码内容（选可逆的样本：
        // 有些汉字组合在 GBK 里没有对应字节，属**不可逆**乱码，只能提示用户重新导出）
        String mojibake = new String("平板".getBytes(StandardCharsets.UTF_8), GB18030);
        String mojibake2 = new String("测试数据".getBytes(StandardCharsets.UTF_8), GB18030);
        assertNotEquals("平板", mojibake, "前提：这确实是乱码");
        assertEquals("平板", TextEncoding.repairMojibake(mojibake));

        // 整行多字段也要能还原
        List<String> row = new ArrayList<>(List.of("1", mojibake, mojibake2));
        assertTrue(TextEncoding.repairMojibakeInPlace(row));
        assertEquals("1", row.get(0));
        assertEquals("平板", row.get(1));
        assertEquals("测试数据", row.get(2));
        assertEquals("1", TextEncoding.repairMojibake("1"), "纯数字不应被改动");
    }

    @Test
    void doesNotTouchCorrectText() {
        assertEquals("平板", TextEncoding.repairMojibake("平板"), "正常中文不能被「修复」坏");
        assertEquals("ORD000001", TextEncoding.repairMojibake("ORD000001"));
        assertEquals("", TextEncoding.repairMojibake(""));
        assertNull(TextEncoding.repairMojibake(null));
        // 已含替换符（字节已丢）时不瞎猜
        assertEquals("平\uFFFD板", TextEncoding.repairMojibake("平\uFFFD板"));
    }

    @Test
    void readsGbkFileWithoutReplacementChars() throws Exception {
        Path file = tempDir.resolve("gbk.csv");
        Files.write(file, "编号,名称\n1,测试数据\n".getBytes(GB18030));
        String text = TextEncoding.readAll(file);
        assertEquals("编号,名称", text.split("\n")[0]);
        assertFalse(text.contains("\uFFFD"), "不应出现替换符");
    }

    @Test
    void utf8FileWithChineseAcrossSampleBoundaryStaysUtf8() throws Exception {
        // 让第 64KB 处正好落在一个汉字中间，复现事故现场
        String header = "id,product\n";
        int headerLen = header.getBytes(StandardCharsets.UTF_8).length;
        int padLen = TextEncoding.SAMPLE_BYTES - 1 - headerLen;     // 汉字起始位置 = 65535
        StringBuilder content = new StringBuilder(header);
        content.append("0,").append("x".repeat(Math.max(0, padLen - 3))).append('\n');
        for (int i = 0; i < 20; i++) content.append(i).append(",平板\n");

        Path file = tempDir.resolve("boundary.csv");
        Files.write(file, content.toString().getBytes(StandardCharsets.UTF_8));

        assertEquals(StandardCharsets.UTF_8, TextEncoding.detect(file),
                "64KB 边界切在汉字中间时仍要判定为 UTF-8");
        assertTrue(TextEncoding.readAll(file).contains("平板"));
    }
}
