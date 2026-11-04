package com.datastream.mvp.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 告警聚合器（降噪）：把「同一根因、多个作业」的重复告警合并成一条记录。
 *
 * <p>背景（真实痛点）：后端重启后，几十个历史 FAILED 作业会在一轮扫描里同时触发 JOB_FAILED，
 * 告警中心瞬间刷出几十条同义记录，邮件/钉钉也各发一遍；而这些告警的根因往往是同一个
 * （同一个 SQL Gateway 挂了、同一批文件都不在了……）。</p>
 *
 * <p>做法：给每条告警算一个**签名** = 事件 + 归一化后的根因文本（去掉作业名、ID、路径、
 * 数字、引号内容等易变部分）。窗口内（默认 10 分钟）同签名的第 2..N 条不再新建记录、
 * 也不再发邮件/推 webhook，而是把次数累加到既有记录上并补记涉及的作业。</p>
 *
 * <p>另外提供「自动诊断配额」：一个窗口内同签名最多触发 N 次自动诊断（默认 3），
 * 避免一波故障同时打几十个 LLM 请求。</p>
 *
 * <p>状态保存在内存（重启后从头算）：聚合只影响「提醒次数」，落库字段
 * （occurrences / aggregatedJobs / lastSeenAt）才是用户可见的事实，窗口丢失不会丢数据。</p>
 */
@Slf4j
@Component
public class AlertAggregator {

    /** 与具体作业/文件/数字无关的噪声片段 */
    private static final Pattern WINDOWS_PATH = Pattern.compile("[A-Za-z]:\\\\(?:[^\\s\"']|\\\\)*");
    private static final Pattern UNIX_PATH = Pattern.compile("(?:/[^\\s\"']+){2,}");
    private static final Pattern TIMESTAMP = Pattern.compile("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}(?::\\d{2})?");
    private static final Pattern FLINK_JOB_ID = Pattern.compile("\\b[0-9a-f]{32}\\b");
    private static final Pattern JOB_REF = Pattern.compile("job[=: ]\\s*\\d+", Pattern.CASE_INSENSITIVE);
    private static final Pattern HASH_ID = Pattern.compile("#\\d+");
    private static final Pattern QUOTED = Pattern.compile("[`'\"][^`'\"]{0,64}[`'\"]");
    private static final Pattern NUMBER = Pattern.compile("\\b\\d+(?:\\.\\d+)?\\b");
    private static final Pattern SPACES = Pattern.compile("\\s+");

    @Value("${app.alert.aggregation-window-ms:600000}")
    private long windowMs;

    @Value("${app.alert.auto-diagnosis-per-window:3}")
    private int autoDiagnosisPerWindow;

    /** signature -> 聚合桶（仅在窗口有效期内存在） */
    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    /** 一个签名在窗口内的聚合状态 */
    public static final class Bucket {
        private final String signature;
        private final Long alertId;
        private final Set<String> jobLabels = new LinkedHashSet<>();
        private int occurrences = 1;
        private long lastSeen;
        private int diagnosisCount;

        Bucket(String signature, Long alertId, long now) {
            this.signature = signature;
            this.alertId = alertId;
            this.lastSeen = now;
        }

        public String signature() { return signature; }
        public Long alertId() { return alertId; }
        public int occurrences() { return occurrences; }
        public List<String> jobLabels() { return new ArrayList<>(jobLabels); }
        public long lastSeen() { return lastSeen; }
    }

    /**
     * 计算告警签名：事件 + 归一化根因。归一化只抹掉易变部分，不做语义推断，
     * 因此同一段错误文本在不同作业上会得到同一个签名。
     */
    public String signatureOf(String event, String message) {
        return signatureOf(event, message, java.util.List.of());
    }

    /**
     * 同上，额外抹掉调用方给出的噪声词（通常是**作业名**——它只是文本里的业务标签，
     * 不影响根因判断；不抹掉的话「同一个 Gateway 挂了」会因为作业名不同而聚合不到一起）。
     *
     * @param noise 需要替换为 &lt;job&gt; 的文本片段（空串/null 自动忽略）
     */
    public String signatureOf(String event, String message, java.util.Collection<String> noise) {
        String text = message == null ? "" : message;
        if (noise != null) {
            for (String token : noise) {
                if (token == null || token.isBlank()) continue;
                text = text.replace(token.trim(), "<job>");
            }
        }
        text = WINDOWS_PATH.matcher(text).replaceAll("<path>");
        text = UNIX_PATH.matcher(text).replaceAll("<path>");
        text = TIMESTAMP.matcher(text).replaceAll("<ts>");
        text = FLINK_JOB_ID.matcher(text).replaceAll("<jid>");
        text = JOB_REF.matcher(text).replaceAll("job=<id>");
        text = HASH_ID.matcher(text).replaceAll("#<id>");
        text = QUOTED.matcher(text).replaceAll("<q>");
        text = NUMBER.matcher(text).replaceAll("<n>");
        text = SPACES.matcher(text).replaceAll(" ").trim();
        if (text.length() > 240) text = text.substring(0, 240);
        String signature = (event == null ? "" : event.trim()) + "|" + text;
        return signature.length() > 512 ? signature.substring(0, 512) : signature;
    }

    /**
     * 查窗口内可合并的既有聚合桶。
     *
     * @return 非 null 表示本次告警应**合并**进该桶指向的记录（不新建、不发通知）
     */
    public Bucket findMergeable(String signature) {
        long now = System.currentTimeMillis();
        pruneExpired(now);
        Bucket existing = buckets.get(signature);
        if (existing == null) return null;
        if (now - existing.lastSeen > windowMs) {
            buckets.remove(signature);
            return null;
        }
        return existing;
    }

    /** 合并一次：次数 +1、刷新最近时间、补记涉及的作业 */
    public void merge(Bucket bucket, Long jobId, String jobName) {
        if (bucket == null) return;
        bucket.occurrences++;
        bucket.lastSeen = System.currentTimeMillis();
        addJobLabel(bucket, jobId, jobName);
    }

    /** 登记一条新的（未合并的）告警记录，开一个新桶 */
    public Bucket register(String signature, Long alertId, Long jobId, String jobName) {
        long now = System.currentTimeMillis();
        pruneExpired(now);
        Bucket bucket = new Bucket(signature, alertId, now);
        addJobLabel(bucket, jobId, jobName);
        buckets.put(signature, bucket);
        return bucket;
    }

    private void addJobLabel(Bucket bucket, Long jobId, String jobName) {
        String label = (jobName == null || jobName.isBlank() ? "未命名作业" : jobName.trim())
                + (jobId == null ? "" : "(#" + jobId + ")");
        if (bucket.jobLabels.contains(label)) return;
        if (bucket.jobLabels.size() >= 20) return;
        bucket.jobLabels.add(label);
    }

    /** 该签名在本窗口内是否还允许触发自动诊断（默认前 3 次） */
    public boolean allowAutoDiagnosis(String signature) {
        Bucket bucket = buckets.get(signature);
        if (bucket == null) return true;
        if (bucket.diagnosisCount >= autoDiagnosisPerWindow) return false;
        bucket.diagnosisCount++;
        return true;
    }

    /** 清理过期桶，返回清理数量（供定时任务调用，避免 Map 只增不减） */
    public int prune() {
        long now = System.currentTimeMillis();
        int before = buckets.size();
        pruneExpired(now);
        return before - buckets.size();
    }

    /** 当前窗口内的聚合桶数量（健康检查/测试用） */
    public int activeBucketCount() {
        pruneExpired(System.currentTimeMillis());
        return buckets.size();
    }

    /** 定期清理过期桶（桶按签名聚合，量很小；这里只是防止长期运行时 Map 只增不减） */
    @org.springframework.scheduling.annotation.Scheduled(fixedRateString = "${app.alert.aggregation-prune-ms:300000}")
    public void scheduledPrune() {
        int removed = prune();
        if (removed > 0) log.debug("Alert aggregation buckets pruned: {}", removed);
    }

    private void pruneExpired(long now) {
        buckets.entrySet().removeIf(e -> now - e.getValue().lastSeen > windowMs);
    }
}
