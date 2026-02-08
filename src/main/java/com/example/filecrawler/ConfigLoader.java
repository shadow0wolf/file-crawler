package com.example.filecrawler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ConfigLoader {
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private ConfigLoader() {
    }

    public static CrawlerConfig load(Path configPath) throws IOException {
        ConfigPayload payload = MAPPER.readValue(Files.newBufferedReader(configPath), ConfigPayload.class);
        List<Path> roots = payload.roots.stream().map(Path::of).collect(Collectors.toList());
        Path outputDirectory = Path.of(payload.outputDirectory);
        int bufferThreshold = payload.bufferEntryThreshold == null ? 1000 : payload.bufferEntryThreshold;
        int checkpointInterval = payload.checkpointEntryInterval == null ? 2000 : payload.checkpointEntryInterval;
        int threadCount = payload.threadCount == null ? Runtime.getRuntime().availableProcessors() : payload.threadCount;
        boolean followLinks = payload.followLinks != null && payload.followLinks;
        Optional<Path> checkpointFile = Optional.ofNullable(payload.checkpointFile)
                .map(Path::of)
                .or(() -> Optional.of(outputDirectory.resolve("checkpoint.json")));
        Optional<Integer> maxEntries = Optional.ofNullable(payload.maxEntries);
        return new CrawlerConfig(
                roots,
                outputDirectory,
                bufferThreshold,
                checkpointInterval,
                threadCount,
                followLinks,
                checkpointFile,
                maxEntries
        );
    }

    private static final class ConfigPayload {
        public List<String> roots;
        public String outputDirectory;
        public Integer bufferEntryThreshold;
        public Integer checkpointEntryInterval;
        public Integer threadCount;
        public Boolean followLinks;
        public String checkpointFile;
        public Integer maxEntries;
    }
}
