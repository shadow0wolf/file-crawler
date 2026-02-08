package com.example.filecrawler;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public record CrawlerConfig(
        List<Path> roots,
        Path outputDirectory,
        int bufferEntryThreshold,
        int checkpointEntryInterval,
        int threadCount,
        boolean followLinks,
        Optional<Path> checkpointFile,
        Optional<Integer> maxEntries
) {
}
