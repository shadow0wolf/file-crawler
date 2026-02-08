package com.example.filecrawler;

import java.util.List;
import java.util.Map;

/**
 * Serializable checkpoint payload for resuming a crawl.
 */
public record RunState(
        long nextId,
        int nextSequence,
        List<String> pendingDirectories,
        String inProgressDirectory,
        Map<String, DirectoryStatsSnapshot> directoryStats,
        List<String> processedFiles
) {
}
