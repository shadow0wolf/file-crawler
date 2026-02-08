package com.example.filecrawler;

import java.util.List;
import java.util.Map;

public record RunState(
        long nextId,
        int nextSequence,
        List<String> pendingDirectories,
        String inProgressDirectory,
        Map<String, DirectoryStatsSnapshot> directoryStats
) {
}
