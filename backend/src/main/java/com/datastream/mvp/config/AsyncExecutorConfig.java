package com.datastream.mvp.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 告警邮件专用异步线程池。
 *
 * 不使用默认执行器：邮件发送是外部 SMTP 调用，需要与智能诊断等其他异步任务隔离，
 * 且必须是有界队列（避免 SMTP 长时间不可用时无界堆积）。
 */
@Configuration
public class AsyncExecutorConfig {

    @Bean("alertTaskExecutor")
    public ThreadPoolTaskExecutor alertTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("alert-mail-");
        // 队列满时由调用线程执行，保证告警邮件不静默丢失
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(10);
        executor.initialize();
        return executor;
    }
}
