package com.example.filecrawler;

import java.time.Instant;

public class RetryAttempt {
    private final int attempt;
    private final Instant timestamp;
    private final String error;

    public RetryAttempt(int attempt, Instant timestamp, String error) {
        this.attempt = attempt;
        this.timestamp = timestamp;
        this.error = error;
    }

    public int getAttempt() {
        return attempt;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getError() {
        return error;
    }
}
