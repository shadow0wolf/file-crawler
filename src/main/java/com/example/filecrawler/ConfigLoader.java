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

    /**
     * Loads the crawler configuration from a JSON file and applies defaults for
     * optional fields (buffer thresholds, thread count, checkpoint file).
     */
    public static CrawlerConfig load(Path configPath) throws IOException {
        ConfigPayload payload = MAPPER.readValue(Files.newBufferedReader(configPath), ConfigPayload.class);
        List<Path> roots = payload.roots.stream().map(Path::of).collect(Collectors.toList());
        Path outputDirectory = Path.of(payload.outputDirectory);
        int bufferThreshold = payload.bufferEntryThreshold == null ? 1000 : payload.bufferEntryThreshold;
        int checkpointInterval = payload.checkpointEntryInterval == null ? 2000 : payload.checkpointEntryInterval;
        int threadCount = payload.threadCount == null ? Runtime.getRuntime().availableProcessors() : payload.threadCount;
        boolean followLinks = payload.followLinks != null && payload.followLinks;
        // If omitted, checkpoint defaults to the output directory.
        Optional<Path> checkpointFile = Optional.ofNullable(payload.checkpointFile)
                .map(Path::of)
                .or(() -> Optional.of(outputDirectory.resolve("checkpoint.json")));
        Optional<Integer> maxEntries = Optional.ofNullable(payload.maxEntries);
        boolean s3SyncEnabled = payload.s3SyncEnabled != null && payload.s3SyncEnabled;
        Optional<String> s3Bucket = Optional.ofNullable(payload.s3Bucket).filter(value -> !value.isBlank());
        Optional<String> s3Prefix = Optional.ofNullable(payload.s3Prefix).filter(value -> !value.isBlank());
        Optional<String> s3Region = Optional.ofNullable(payload.s3Region).filter(value -> !value.isBlank());
        if (s3SyncEnabled && s3Bucket.isEmpty()) {
            throw new IllegalArgumentException("s3Bucket is required when s3SyncEnabled is true.");
        }
        return new CrawlerConfig(
                roots,
                outputDirectory,
                bufferThreshold,
                checkpointInterval,
                threadCount,
                followLinks,
                checkpointFile,
                maxEntries,
                s3SyncEnabled,
                s3Bucket,
                s3Prefix,
                s3Region
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
        public Boolean s3SyncEnabled;
        public String s3Bucket;
        public String s3Prefix;
        public String s3Region;
    }
}
