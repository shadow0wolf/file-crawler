package com.example.filecrawler;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConfigLoader {
    private static final int DEFAULT_BUFFER_THRESHOLD = 1000;
    private static final int DEFAULT_CHECKPOINT_INTERVAL = 2000;
    private static final int DEFAULT_FILE_RETRY_ATTEMPTS = 2;

    private final ObjectMapper mapper;

    public ConfigLoader() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
    public CrawlerConfig load(Path path) throws IOException {
        RawConfig raw = mapper.readValue(path.toFile(), RawConfig.class);

        if (raw.roots == null || raw.roots.isEmpty()) {
            throw new IllegalArgumentException("Config must include at least one root path.");
        }

        Path outputDirectory = Path.of(optionalString(raw.outputDirectory, "output"));
        int bufferThreshold = raw.bufferEntryThreshold != null && raw.bufferEntryThreshold > 0
                ? raw.bufferEntryThreshold
                : DEFAULT_BUFFER_THRESHOLD;
        int checkpointInterval = raw.checkpointEntryInterval != null && raw.checkpointEntryInterval > 0
                ? raw.checkpointEntryInterval
                : DEFAULT_CHECKPOINT_INTERVAL;
        int threadCount = raw.threadCount != null && raw.threadCount > 0
                ? raw.threadCount
                : Math.max(1, Runtime.getRuntime().availableProcessors());
        boolean followLinks = raw.followLinks != null && raw.followLinks;
        int fileRetryAttempts = raw.fileRetryAttempts != null && raw.fileRetryAttempts > 0
                ? raw.fileRetryAttempts
                : DEFAULT_FILE_RETRY_ATTEMPTS;

        List<Path> roots = raw.roots.stream().map(Path::of).toList();
        Optional<Path> checkpointFile = Optional.ofNullable(raw.checkpointFile).map(Path::of);
        Optional<Integer> maxEntries = Optional.ofNullable(raw.maxEntries);
        List<String> excludeFilePatterns = raw.excludeFilePatterns == null ? List.of() : List.copyOf(raw.excludeFilePatterns);
        List<String> excludeDirectoryPatterns = raw.excludeDirectoryPatterns == null ? List.of() : List.copyOf(raw.excludeDirectoryPatterns);

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
                fileRetryAttempts,
                excludeFilePatterns,
                excludeDirectoryPatterns
        );
    }

    private String optionalString(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value;
    }

    private static class RawConfig {
        public List<String> roots = new ArrayList<>();
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
        public Integer fileRetryAttempts;
        public List<String> excludeFilePatterns;
        public List<String> excludeDirectoryPatterns;
    }
}
