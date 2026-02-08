package com.example.filecrawler;

public record DirectoryStatsSnapshot(
        long totalFiles,
        long totalBytes
) {
    /**
     * Creates an immutable snapshot of the running directory totals.
     */
    public static DirectoryStatsSnapshot from(DirectoryStats stats) {
        return new DirectoryStatsSnapshot(stats.totalFiles(), stats.totalBytes());
    }
}
