package com.self.kafka.consum.config;

import org.springframework.lang.NonNull;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/**
 * 定义递增重试
 */
public class LinearBackOff implements BackOff {

    private final long initialInterval;
    private final long increment;
    private final long maxAttempts;

    public LinearBackOff(long initialInterval, long increment, long maxAttempts) {
        this.initialInterval = initialInterval;
        this.increment = increment;
        this.maxAttempts = maxAttempts;
    }

    @Override
    @NonNull
    public BackOffExecution start() {
        return new BackOffExecution() {
            private long attempt = 0;

            @Override
            public long nextBackOff() {
                attempt++;
                if (attempt > maxAttempts) {
                    return BackOffExecution.STOP;
                }
                // 第1次重试等 initialInterval，之后每次递增 increment
                return initialInterval + (attempt - 1) * increment;
            }
        };
    }

}
