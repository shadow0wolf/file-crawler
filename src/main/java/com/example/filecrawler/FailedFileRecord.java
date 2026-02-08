package com.example.filecrawler;

import java.time.Instant;
import java.util.List;

public class FailedFileRecord {
    private final String path;
    private final int attempts;
    private final int maxAttempts;
    private final Instant lastAttemptTime;
    private final String lastError;
    private final List<RetryAttempt> retryAttempts;

    public FailedFileRecord(String path,
                            int attempts,
                            int maxAttempts,
                            Instant lastAttemptTime,
                            String lastError,
                            List<RetryAttempt> retryAttempts) {
        this.path = path;
        this.attempts = attempts;
        this.maxAttempts = maxAttempts;
        this.lastAttemptTime = lastAttemptTime;
        this.lastError = lastError;
        this.retryAttempts = retryAttempts;
    }

    public String getPath() {
        return path;
    }

    public int getAttempts() {
        return attempts;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public Instant getLastAttemptTime() {
        return lastAttemptTime;
    }

    public String getLastError() {
        return lastError;
    }

    public List<RetryAttempt> getRetryAttempts() {
        return retryAttempts;
    }
}
