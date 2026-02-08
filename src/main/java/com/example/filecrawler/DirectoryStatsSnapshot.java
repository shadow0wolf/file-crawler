package com.example.filecrawler;

public record DirectoryStatsSnapshot(
        long totalFiles,
        long totalBytes
) {
    public static DirectoryStatsSnapshot from(DirectoryStats stats) {
        return new DirectoryStatsSnapshot(stats.totalFiles(), stats.totalBytes());
    }
}
