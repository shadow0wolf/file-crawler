package com.example.filecrawler;

import java.util.concurrent.atomic.AtomicLong;

public final class DirectoryStats {
    private final AtomicLong totalFiles = new AtomicLong();
    private final AtomicLong totalBytes = new AtomicLong();

    public DirectoryStats() {
    }

    public DirectoryStats(long totalFiles, long totalBytes) {
        this.totalFiles.set(totalFiles);
        this.totalBytes.set(totalBytes);
    }

    public void addFile(long size) {
        totalFiles.incrementAndGet();
        totalBytes.addAndGet(size);
    }

    public long totalFiles() {
        return totalFiles.get();
    }

    public long totalBytes() {
        return totalBytes.get();
    }
}
