package btree4j.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class ThreadPoolConfig {

    @Bean(name = "canalTaskExecutor")
    public Executor canalTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10); // 核心线程数
        executor.setMaxPoolSize(20);  // 最大线程数
        executor.setQueueCapacity(500); // 队列容量
        executor.setThreadNamePrefix("CanalTaskExecutor-"); // 线程名前缀
        executor.setWaitForTasksToCompleteOnShutdown(true); // 应用关闭时等待任务完成
        executor.setAwaitTerminationSeconds(60); // 等待时间
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());        //rejcet policy
        executor.initialize();
        return executor;
    }
}
