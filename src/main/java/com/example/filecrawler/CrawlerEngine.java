package com.example.filecrawler;

import com.example.filecrawler.metadata.DirectoryMetadata;
import com.example.filecrawler.metadata.FileMetadata;
import com.example.filecrawler.metadata.MetadataEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class CrawlerEngine {
    private static final Logger logger = LoggerFactory.getLogger(CrawlerEngine.class);

    private final CrawlerConfig config;
    private final CheckpointManager checkpointManager;
    private final ProcessingLimiter limiter;
    private final FileMetadataExtractor extractor;

    public CrawlerEngine(CrawlerConfig config, CheckpointManager checkpointManager, ProcessingLimiter limiter) {
        this.config = config;
        this.checkpointManager = checkpointManager;
        this.limiter = limiter == null ? ProcessingLimiter.NO_LIMIT : limiter;
        this.extractor = new FileMetadataExtractor(new Tika());
    }

    public void run() throws IOException, InterruptedException {
        Path outputDirectory = config.outputDirectory();
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        Optional<RunState> loadedState = checkpointManager == null ? Optional.empty() : checkpointManager.load();
        Deque<Path> directoryQueue = new ArrayDeque<>();
        Set<Path> queuedDirectories = new HashSet<>();
        Map<Path, DirectoryStats> directoryStats = new HashMap<>();
        List<Path> visitedDirectories = new ArrayList<>();
        Set<Path> processedFiles = new HashSet<>();
        List<Path> rootPaths = config.roots().stream()
                .map(path -> path.toAbsolutePath().normalize())
                .toList();

        IdRegistry idRegistry;
        int nextMetadataSequence = 1;
        if (loadedState.isPresent()) {
            RunState state = loadedState.get();
            idRegistry = new IdRegistry(state.nextId());
            nextMetadataSequence = state.nextSequence();
            processedFiles.addAll(loadProcessedFiles(outputDirectory, mapper));
            if (state.processedFiles() != null) {
                state.processedFiles().stream()
                        .filter(path -> path != null && !path.isBlank())
                        .map(Path::of)
                        .map(path -> path.toAbsolutePath().normalize())
                        .forEach(processedFiles::add);
            }
            if (state.inProgressDirectory() != null) {
                Path normalized = Path.of(state.inProgressDirectory()).toAbsolutePath().normalize();
                directoryQueue.add(normalized);
                queuedDirectories.add(normalized);
            }
            state.pendingDirectories().stream()
                    .map(Path::of)
                    .map(path -> path.toAbsolutePath().normalize())
                    .forEach(path -> {
                        if (queuedDirectories.add(path)) {
                            directoryQueue.add(path);
                        }
                    });
            state.directoryStats().forEach((path, snapshot) -> {
                Path dirPath = Path.of(path).toAbsolutePath().normalize();
                directoryStats.put(dirPath, new DirectoryStats(snapshot.totalFiles(), snapshot.totalBytes()));
                visitedDirectories.add(dirPath);
            });
        } else {
            idRegistry = new IdRegistry(1L);
            for (Path root : rootPaths) {
                Path normalized = root.toAbsolutePath().normalize();
                if (queuedDirectories.add(normalized)) {
                    directoryQueue.add(normalized);
                }
            }
        }

        int nextFailureSequence = determineNextSequence(outputDirectory, "failed_");
        JsonFileSyncer syncer = JsonFileSyncer.noop();
        if (config.s3SyncEnabled()) {
            syncer = new S3SyncService(
                    outputDirectory,
                    config.s3Bucket().orElseThrow(),
                    config.s3Prefix().orElse(""),
                    config.s3Region()
            );
        }

        try {
            JsonBuffer<MetadataEnvelope> metadataBuffer = new JsonBuffer<>(mapper, outputDirectory,
                    config.bufferEntryThreshold(), nextMetadataSequence, syncer);
            JsonBuffer<FailedFileRecord> failureBuffer = new JsonBuffer<>(mapper, outputDirectory,
                    config.bufferEntryThreshold(), nextFailureSequence, "failed_", syncer);

            ExecutorService executor = Executors.newFixedThreadPool(config.threadCount());
            CompletionService<FileProcessingResult> completionService = new ExecutorCompletionService<>(executor);

            List<PathMatcher> fileMatchers = buildMatchers(config.excludeFilePatterns());
            List<PathMatcher> directoryMatchers = buildMatchers(config.excludeDirectoryPatterns());

            long processedCount = 0L;
            int inFlight = 0;
            boolean stopRequested = false;
            int maxInFlight = limiter == ProcessingLimiter.NO_LIMIT && config.maxEntries().isEmpty()
                    ? config.threadCount()
                    : 1;

            Path currentDirectory = null;
            while ((!directoryQueue.isEmpty() || inFlight > 0) && !stopRequested) {
                while (!directoryQueue.isEmpty() && !stopRequested) {
                    Path directory = directoryQueue.poll();
                    if (directory == null) {
                        break;
                    }
                    queuedDirectories.remove(directory);
                    currentDirectory = directory;
                    if (matchesAny(directory, directoryMatchers)) {
                        logger.debug("Skipping excluded directory: {}", directory);
                        continue;
                    }
                    if (!config.followLinks() && Files.isSymbolicLink(directory)) {
                        logger.debug("Skipping symlinked directory: {}", directory);
                        continue;
                    }
                    Path normalizedDirectory = directory.toAbsolutePath().normalize();
                    if (!visitedDirectories.contains(normalizedDirectory)) {
                        visitedDirectories.add(normalizedDirectory);
                    }
                    if (checkpointManager != null) {
                        saveCheckpoint(directoryQueue, directory, idRegistry, metadataBuffer, directoryStats, processedFiles);
                    }

                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
                        for (Path entry : stream) {
                            if (stopRequested) {
                                break;
                            }
                            Path normalizedEntry = entry.toAbsolutePath().normalize();
                            if (Files.isDirectory(entry)) {
                                if (matchesAny(normalizedEntry, directoryMatchers)) {
                                    continue;
                                }
                                if (!config.followLinks() && Files.isSymbolicLink(entry)) {
                                    continue;
                                }
                                if (queuedDirectories.add(normalizedEntry)) {
                                    directoryQueue.add(normalizedEntry);
                                }
                            } else if (Files.isRegularFile(entry)) {
                                if (matchesAny(normalizedEntry, fileMatchers)) {
                                    continue;
                                }
                                if (processedFiles.contains(normalizedEntry)) {
                                    continue;
                                }
                                if (config.maxEntries().isPresent()
                                        && processedCount + inFlight >= config.maxEntries().get()) {
                                    stopRequested = true;
                                    break;
                                }
                                completionService.submit(() -> processFile(normalizedEntry, idRegistry));
                                inFlight++;

                                if (inFlight >= maxInFlight) {
                                    Future<FileProcessingResult> future = completionService.take();
                                    processedCount += handleResult(future, metadataBuffer, failureBuffer, directoryStats, rootPaths, processedFiles);
                                    inFlight--;
                                    stopRequested = shouldStop(processedCount);
                                    maybeCheckpoint(processedCount, directoryQueue, directory, idRegistry, metadataBuffer, directoryStats, processedFiles);
                                }
                            }
                        }
                    } catch (IOException ex) {
                        logger.warn("Failed to scan directory {}", directory, ex);
                    }
                }

                while (inFlight > 0 && !stopRequested) {
                    Future<FileProcessingResult> future = completionService.take();
                    processedCount += handleResult(future, metadataBuffer, failureBuffer, directoryStats, rootPaths, processedFiles);
                    inFlight--;
                    stopRequested = shouldStop(processedCount);
                    maybeCheckpoint(processedCount, directoryQueue, null, idRegistry, metadataBuffer, directoryStats, processedFiles);
                }
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);

            if (!stopRequested) {
                emitDirectoryMetadata(metadataBuffer, visitedDirectories, directoryStats, idRegistry);
            }

            metadataBuffer.close();
            failureBuffer.close();

            if (checkpointManager != null) {
                Path inProgress = stopRequested ? currentDirectory : null;
                saveCheckpoint(directoryQueue, inProgress, idRegistry, metadataBuffer, directoryStats, processedFiles);
            }
        } finally {
            syncer.close();
        }
    }

    private boolean shouldStop(long processedCount) {
        if (limiter.shouldStop(processedCount)) {
            return true;
        }
        return config.maxEntries().isPresent() && processedCount >= config.maxEntries().get();
    }

    private int handleResult(Future<FileProcessingResult> future,
                             JsonBuffer<MetadataEnvelope> metadataBuffer,
                             JsonBuffer<FailedFileRecord> failureBuffer,
                             Map<Path, DirectoryStats> directoryStats,
                             List<Path> rootPaths,
                             Set<Path> processedFiles) throws IOException {
        try {
            FileProcessingResult result = future.get();
            if (result.isSuccess()) {
                FileMetadata metadata = result.getMetadata();
                metadataBuffer.add(MetadataEnvelope.file(metadata));
                updateDirectoryStats(Path.of(metadata.path()), metadata.size(), directoryStats, rootPaths);
                processedFiles.add(Path.of(metadata.path()).toAbsolutePath().normalize());
            } else {
                failureBuffer.add(result.getFailure());
            }
            return 1;
        } catch (Exception ex) {
            logger.warn("Failed to process file result", ex);
            FailedFileRecord failure = new FailedFileRecord(
                    "unknown",
                    1,
                    1,
                    Instant.now(),
                    ex.getMessage(),
                    List.of(new RetryAttempt(1, Instant.now(), ex.getMessage())));
            failureBuffer.add(failure);
            return 1;
        }
    }

    private FileProcessingResult processFile(Path path, IdRegistry idRegistry) {
        int maxAttempts = Math.max(1, config.fileRetryAttempts());
        List<RetryAttempt> attempts = new ArrayList<>();
        long parentId = path.getParent() == null ? -1L : idRegistry.idFor(path.getParent());
        long id = idRegistry.idFor(path);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                FileMetadata metadata = extractor.extract(path, id, parentId, config.followLinks());
                return FileProcessingResult.success(metadata);
            } catch (Exception ex) {
                RetryAttempt retryAttempt = new RetryAttempt(attempt, Instant.now(), ex.getMessage());
                attempts.add(retryAttempt);
                logger.debug("Attempt {} failed for {}", attempt, path, ex);
            }
        }

        RetryAttempt lastAttempt = attempts.get(attempts.size() - 1);
        FailedFileRecord failure = new FailedFileRecord(
                path.toString(),
                attempts.size(),
                maxAttempts,
                lastAttempt.getTimestamp(),
                lastAttempt.getError(),
                attempts);
        return FileProcessingResult.failure(failure);
    }

    private void emitDirectoryMetadata(JsonBuffer<MetadataEnvelope> metadataBuffer,
                                       List<Path> visitedDirectories,
                                       Map<Path, DirectoryStats> directoryStats,
                                       IdRegistry idRegistry) throws IOException {
        visitedDirectories.sort(Comparator.comparing(Path::toString));
        LinkOption[] linkOptions = config.followLinks() ? new LinkOption[0] : new LinkOption[]{LinkOption.NOFOLLOW_LINKS};

        for (Path directory : visitedDirectories) {
            DirectoryStats stats = directoryStats.getOrDefault(directory, new DirectoryStats());
            DirectoryMetadata metadata = buildDirectoryMetadata(directory, stats, idRegistry, linkOptions);
            metadataBuffer.add(MetadataEnvelope.directory(metadata));
        }
    }

    private DirectoryMetadata buildDirectoryMetadata(Path directory,
                                                    DirectoryStats stats,
                                                    IdRegistry idRegistry,
                                                    LinkOption[] linkOptions) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(directory, BasicFileAttributes.class, linkOptions);
        long id = idRegistry.idFor(directory);
        long parentId = directory.getParent() == null ? -1L : idRegistry.idFor(directory.getParent());
        String parentPath = directory.getParent() == null ? null : directory.getParent().toString();
        String relativePath = directory.getParent() == null
                ? directory.toString()
                : (directory.getFileName() == null ? directory.toString() : directory.getFileName().toString());
        String fileKey = attributes.fileKey() == null ? null : attributes.fileKey().toString();
        return new DirectoryMetadata(
                id,
                parentId,
                parentPath,
                relativePath,
                directory.getFileName() == null ? directory.toString() : directory.getFileName().toString(),
                stats.totalFiles(),
                stats.totalBytes(),
                attributes.creationTime().toInstant(),
                attributes.lastModifiedTime().toInstant(),
                attributes.lastAccessTime().toInstant(),
                fileKey
        );
    }

    private void updateDirectoryStats(Path filePath,
                                      long size,
                                      Map<Path, DirectoryStats> directoryStats,
                                      List<Path> roots) {
        Path parent = filePath.getParent();
        while (parent != null) {
            Path normalized = parent.toAbsolutePath().normalize();
            boolean withinRoot = roots.stream().anyMatch(normalized::startsWith);
            if (!withinRoot) {
                break;
            }
            directoryStats.computeIfAbsent(normalized, ignored -> new DirectoryStats()).addFile(size);
            if (roots.contains(normalized)) {
                break;
            }
            parent = normalized.getParent();
        }
    }

    private void maybeCheckpoint(long processedCount,
                                 Deque<Path> directoryQueue,
                                 Path inProgressDirectory,
                                 IdRegistry idRegistry,
                                 JsonBuffer<MetadataEnvelope> metadataBuffer,
                                 Map<Path, DirectoryStats> directoryStats,
                                 Set<Path> processedFiles) throws IOException {
        if (checkpointManager == null) {
            return;
        }
        if (processedCount % config.checkpointEntryInterval() == 0) {
            saveCheckpoint(directoryQueue, inProgressDirectory, idRegistry, metadataBuffer, directoryStats, processedFiles);
        }
    }

    private void saveCheckpoint(Deque<Path> directoryQueue,
                                Path inProgressDirectory,
                                IdRegistry idRegistry,
                                JsonBuffer<MetadataEnvelope> metadataBuffer,
                                Map<Path, DirectoryStats> directoryStats,
                                Set<Path> processedFiles) throws IOException {
        if (checkpointManager == null) {
            return;
        }
        List<String> pendingDirectories = directoryQueue.stream().map(Path::toString).toList();
        List<String> processed = processedFiles.stream()
                .map(Path::toString)
                .toList();
        Map<String, DirectoryStatsSnapshot> statsSnapshot = new HashMap<>();
        directoryStats.forEach((path, stats) -> statsSnapshot.put(path.toString(), DirectoryStatsSnapshot.from(stats)));

        RunState state = new RunState(
                idRegistry.nextId(),
                metadataBuffer.nextSequence(),
                pendingDirectories,
                inProgressDirectory == null ? null : inProgressDirectory.toString(),
                statsSnapshot,
                processed
        );
        checkpointManager.save(state);
    }

    private int determineNextSequence(Path outputDirectory, String prefix) throws IOException {
        if (!Files.exists(outputDirectory)) {
            return 1;
        }
        int max = 0;
        try (var stream = Files.list(outputDirectory)) {
            for (Path path : stream.toList()) {
                String name = path.getFileName().toString();
                if (!name.startsWith(prefix) || !name.endsWith(".json")) {
                    continue;
                }
                int start = prefix.length();
                int end = name.length() - ".json".length();
                if (end <= start) {
                    continue;
                }
                try {
                    int value = Integer.parseInt(name.substring(start, end));
                    max = Math.max(max, value);
                } catch (NumberFormatException ignored) {
                    // ignore
                }
            }
        }
        return max + 1;
    }

    private Set<Path> loadProcessedFiles(Path outputDirectory, ObjectMapper mapper) throws IOException {
        Set<Path> processed = new HashSet<>();
        if (!Files.exists(outputDirectory)) {
            return processed;
        }
        Map<Long, DirectoryMetadata> directories = new HashMap<>();
        List<FileMetadata> files = new ArrayList<>();
        try (var stream = Files.list(outputDirectory)) {
            for (Path path : stream.toList()) {
                String name = path.getFileName().toString();
                if (!name.startsWith("metadata_") || !name.endsWith(".json")) {
                    continue;
                }
                try {
                    MetadataEnvelope[] envelopes = mapper.readValue(path.toFile(), MetadataEnvelope[].class);
                    for (MetadataEnvelope envelope : envelopes) {
                        if ("directory".equals(envelope.type())) {
                            DirectoryMetadata directory = mapper.convertValue(envelope.payload(), DirectoryMetadata.class);
                            directories.put(directory.id(), directory);
                        } else if ("file".equals(envelope.type())) {
                            FileMetadata metadata = mapper.convertValue(envelope.payload(), FileMetadata.class);
                            files.add(metadata);
                        }
                    }
                } catch (Exception ex) {
                    logger.warn("Failed to read existing metadata file {}", path, ex);
                }
            }
        }
        Map<Long, Path> resolvedDirectories = new HashMap<>();
        for (FileMetadata metadata : files) {
            Path filePath = resolveFilePath(metadata, directories, resolvedDirectories);
            if (filePath != null) {
                processed.add(filePath.toAbsolutePath().normalize());
            }
        }
        return processed;
    }

    private Path resolveFilePath(FileMetadata metadata,
                                 Map<Long, DirectoryMetadata> directories,
                                 Map<Long, Path> resolvedDirectories) {
        if (metadata.path() != null && !metadata.path().isBlank()) {
            return Path.of(metadata.path());
        }
        Path parentPath = resolveDirectoryPath(metadata.parentId(), directories, resolvedDirectories);
        if (parentPath == null || metadata.name() == null || metadata.name().isBlank()) {
            return null;
        }
        return parentPath.resolve(metadata.name());
    }

    private Path resolveDirectoryPath(long directoryId,
                                      Map<Long, DirectoryMetadata> directories,
                                      Map<Long, Path> resolvedDirectories) {
        if (directoryId < 0) {
            return null;
        }
        Path cached = resolvedDirectories.get(directoryId);
        if (cached != null) {
            return cached;
        }
        DirectoryMetadata metadata = directories.get(directoryId);
        if (metadata == null || metadata.path() == null || metadata.path().isBlank()) {
            return null;
        }
        Path resolved;
        if (metadata.parentId() < 0) {
            resolved = Path.of(metadata.path());
        } else {
            Path parent = resolveDirectoryPath(metadata.parentId(), directories, resolvedDirectories);
            resolved = parent == null ? null : parent.resolve(metadata.path());
        }
        if (resolved != null) {
            resolvedDirectories.put(directoryId, resolved);
        }
        return resolved;
    }

    private List<PathMatcher> buildMatchers(List<String> patterns) {
        List<PathMatcher> matchers = new ArrayList<>();
        for (String pattern : patterns) {
            if (pattern == null || pattern.isBlank()) {
                continue;
            }
            matchers.add(FileSystems.getDefault().getPathMatcher("glob:" + pattern));
        }
        return matchers;
    }

    private boolean matchesAny(Path path, List<PathMatcher> matchers) {
        Path fileName = path.getFileName();
        for (PathMatcher matcher : matchers) {
            if (fileName != null && matcher.matches(fileName)) {
                return true;
            }
            if (matcher.matches(path)) {
                return true;
            }
        }
        return false;
    }
}
