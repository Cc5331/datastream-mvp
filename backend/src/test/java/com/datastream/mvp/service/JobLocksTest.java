package com.datastream.mvp.service;

import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 作业分片锁池：同一作业必须落在同一把锁上（否则互斥失效），且锁可重入
 * （提交过程中再调 finalizeJobOutputs 不会自锁）。
 */
class JobLocksTest {

    @Test
    void sameJobAlwaysMapsToSameLock() {
        ReentrantLock a = JobLocks.forJob(42L);
        ReentrantLock b = JobLocks.forJob(42L);
        assertSame(a, b, "同一作业必须取到同一把锁");
    }

    @Test
    void differentJobsInSameStripeShareLockButDoNotDeadlock() {
        // 分片数为 64：相差 64 的作业 id 会共用一把锁（代价可接受，换取锁池不随作业数增长）
        assertSame(JobLocks.forJob(1L), JobLocks.forJob(65L));
        assertNotSame(JobLocks.forJob(1L), JobLocks.forJob(2L));
    }

    @Test
    void lockIsReentrantAndNullSafe() {
        ReentrantLock lock = JobLocks.forJob(null);
        assertNotNull(lock);
        lock.lock();
        try {
            assertTrue(lock.isHeldByCurrentThread());
            lock.lock(); // 可重入：submit 内部再调 finalizeJobOutputs 的场景
            lock.unlock();
        } finally {
            lock.unlock();
        }
        assertFalse(lock.isLocked());
    }

    @Test
    void mutualExclusionActuallySerializesSameJob() throws Exception {
        ReentrantLock lock = JobLocks.forJob(7L);
        java.util.concurrent.atomic.AtomicInteger concurrent = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger maxConcurrent = new java.util.concurrent.atomic.AtomicInteger();
        Runnable task = () -> {
            lock.lock();
            try {
                int now = concurrent.incrementAndGet();
                maxConcurrent.accumulateAndGet(now, Math::max);
                Thread.sleep(20);
                concurrent.decrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        };
        Thread t1 = new Thread(task);
        Thread t2 = new Thread(task);
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        assertEquals(1, maxConcurrent.get(), "同一作业的临界区不能并发进入");
    }
}
