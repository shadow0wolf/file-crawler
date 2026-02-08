package com.example.filecrawler;

public interface ProcessingLimiter {
    boolean shouldStop(long processedCount);

    ProcessingLimiter NO_LIMIT = processedCount -> false;
}
