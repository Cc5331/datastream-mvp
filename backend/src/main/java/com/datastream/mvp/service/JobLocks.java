package com.datastream.mvp.service;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 按作业 id 分片的可重入锁池。
 *
 * 用途：让「同一作业」的提交 / 取消 / 输出后处理互斥，避免并发触发导致
 * - 手动提交、JobScheduler 定时触发、DependencyService 自动触发同时 submit → 重复的 Flink 作业；
 * - 取消（或轮询判定完成）与轮询器同时合并同一批 part 文件 → 输出丢失或损坏。
 *
 * 采用固定 64 个分片而不是「每个作业一把锁 + Map」：后者会随作业数量无界增长。
 * 不同作业偶发落在同一分片只会带来极小的串行化代价，不会死锁（每个线程同一时刻只持有一把分片锁）。
 */
final class JobLocks {

    private static final int STRIPES = 64;
    private static final ReentrantLock[] LOCKS = new ReentrantLock[STRIPES];

    static {
        for (int i = 0; i < STRIPES; i++) {
            LOCKS[i] = new ReentrantLock();
        }
    }

    private JobLocks() {
    }

    /** 取该作业所属分片的锁（可重入：提交过程中再调 finalize 不会自锁） */
    static ReentrantLock forJob(Long jobId) {
        int index = (int) Math.floorMod(jobId == null ? 0L : jobId, STRIPES);
        return LOCKS[index];
    }
}
