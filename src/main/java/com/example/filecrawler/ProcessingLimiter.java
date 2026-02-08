package com.example.filecrawler;

public interface ProcessingLimiter {
    /**
     * Returns true if processing should stop at the given processed count.
     */
    boolean shouldStop(long processedCount);

    /**
     * Default limiter used in production runs (never stops early).
     */
    ProcessingLimiter NO_LIMIT = processedCount -> false;
}
