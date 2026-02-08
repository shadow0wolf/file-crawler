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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orchestrates the crawl lifecycle: directory traversal, file metadata extraction,
 * buffering output to JSON, and checkpoint persistence for recovery.
 */
public final class CrawlerEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerEngine.class);
    private static final MetadataEnvelope POISON = new MetadataEnvelope("poison", "poison");

    private final CrawlerConfig config;
    private final CheckpointManager checkpointManager;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final ProcessingLimiter limiter;

    public CrawlerEngine(CrawlerConfig config, CheckpointManager checkpointManager) {
        this(config, checkpointManager, ProcessingLimiter.NO_LIMIT);
    }

    CrawlerEngine(CrawlerConfig config, CheckpointManager checkpointManager, ProcessingLimiter limiter) {
        this.config = config;
        this.checkpointManager = checkpointManager;
        this.limiter = limiter;
    }

    /**
     * Executes a crawl run. The method restores prior checkpoint state (if present),
     * streams file metadata to a background consumer, and emits directory summaries
     * at the end of a successful run.
     */
    public void run() throws IOException, InterruptedException {
        Optional<RunState> existing = checkpointManager.load();
        RunState runState = existing.orElse(null);

        JsonFileSyncer syncer = JsonFileSyncer.noop();
        if (config.s3SyncEnabled()) {
            syncer = new S3SyncService(
                    config.outputDirectory(),
                    config.s3Bucket().orElseThrow(),
                    config.s3Prefix().orElse(""),
                    config.s3Region()
            );
        }

        long startingId = runState == null ? 1L : runState.nextId();
        int startingSequence = runState == null ? 1 : runState.nextSequence();

        IdRegistry idRegistry = new IdRegistry(startingId);
        Map<Path, DirectoryStats> directoryStats = new ConcurrentHashMap<>();
        Set<Path> directories = ConcurrentHashMap.newKeySet();

        if (runState != null) {
            restoreDirectoryStats(directoryStats, runState.directoryStats());
            directories.addAll(directoryStats.keySet());
        }

        // Buffer JSON payloads so we can write them in deterministic batches.
        try {
            JsonBuffer buffer = new JsonBuffer(
                    mapper,
                    config.outputDirectory(),
                    config.bufferEntryThreshold(),
                    startingSequence,
                    syncer
            );
            BlockingQueue<MetadataEnvelope> resultQueue = new LinkedBlockingQueue<>();

            // Dedicated consumer flushes metadata to disk while workers continue extracting.
            Thread consumer = new Thread(() -> consumeResults(resultQueue, buffer));
            consumer.start();

            Deque<Path> pending = new ArrayDeque<>();
            if (runState != null) {
                runState.pendingDirectories().stream().map(Path::of).forEach(pending::addLast);
                if (runState.inProgressDirectory() != null) {
                    pending.addFirst(Path.of(runState.inProgressDirectory()));
                }
            } else {
                config.roots().forEach(pending::addLast);
            }

            ExecutorService fileExecutor = Executors.newFixedThreadPool(config.threadCount());
            FileMetadataExtractor extractor = new FileMetadataExtractor(new Tika());
            AtomicLong processedEntries = new AtomicLong();
            Set<Path> rootSet = config.roots().stream().map(this::normalize).collect(java.util.stream.Collectors.toSet());
            boolean stopRequested = false;

            while (!pending.isEmpty()) {
                Path current = pending.removeFirst();
                if (shouldSkipPath(current)) {
                    continue;
                }

                // Persist progress before we begin scanning a directory so recovery can resume.
                saveCheckpoint(idRegistry, buffer, pending, current, directoryStats);

                BasicFileAttributes attrs;
                try {
                    attrs = Files.readAttributes(current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                } catch (IOException ex) {
                    LOGGER.warn("Failed to read attributes for {}", current, ex);
                    continue;
                }

                if (!attrs.isDirectory()) {
                    continue;
                }

                directories.add(current);
                directoryStats.computeIfAbsent(normalize(current), ignored -> new DirectoryStats());
                idRegistry.idFor(current);

                try (DirectoryStream<Path> stream = Files.newDirectoryStream(current)) {
                    for (Path entry : stream) {
                        if (shouldSkipPath(entry)) {
                            continue;
                        }
                        if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
                            pending.addLast(entry);
                        } else if (Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
                            long size = safeSize(entry);
                            // Update stats for the directory ancestry chain while walking.
                            updateAncestorStats(directoryStats, rootSet, entry, size);
                            // File metadata extraction is parallelized across the executor.
                            submitFileTask(fileExecutor, extractor, resultQueue, idRegistry, entry);
                            long count = processedEntries.incrementAndGet();
                            if (limiter.shouldStop(count)) {
                                LOGGER.info("Stopping early after {} entries for checkpoint testing.", count);
                                stopRequested = true;
                            }
                        }
                    }
                } catch (IOException ex) {
                    LOGGER.warn("Failed to list directory {}", current, ex);
                }

                // Periodically checkpoint based on processed file counts.
                if (processedEntries.get() % config.checkpointEntryInterval() == 0) {
                    saveCheckpoint(idRegistry, buffer, pending, null, directoryStats);
                }
                if (stopRequested || limiter.shouldStop(processedEntries.get())) {
                    break;
                }
            }

            // Drain worker pool and flush the remaining buffered metadata.
            fileExecutor.shutdown();
            fileExecutor.awaitTermination(1, TimeUnit.HOURS);
            resultQueue.add(POISON);
            consumer.join();
            buffer.flush();

            if (stopRequested) {
                saveCheckpoint(idRegistry, buffer, pending, null, directoryStats);
                LOGGER.info("Crawl paused with checkpoint.");
                return;
            }

            // Directory metadata is derived after file processing so totals are complete.
            emitDirectoryMetadata(idRegistry, directories, directoryStats, buffer);
            buffer.flush();
            saveCheckpoint(idRegistry, buffer, new ArrayDeque<>(), null, directoryStats);
            LOGGER.info("Crawl completed.");
        } finally {
            syncer.close();
        }
    }

    private void submitFileTask(ExecutorService executor,
                                FileMetadataExtractor extractor,
                                BlockingQueue<MetadataEnvelope> queue,
                                IdRegistry idRegistry,
                                Path file) {
        executor.submit(() -> {
            long id = idRegistry.idFor(file);
            long parentId = file.getParent() == null ? -1 : idRegistry.idFor(file.getParent());
            try {
                FileMetadata metadata = extractor.extract(file, id, parentId);
                queue.put(MetadataEnvelope.file(metadata));
            } catch (Exception ex) {
                LOGGER.warn("Failed to extract metadata for {}", file, ex);
            }
        });
    }

    private void consumeResults(BlockingQueue<MetadataEnvelope> queue, JsonBuffer buffer) {
        try {
            while (true) {
                MetadataEnvelope envelope = queue.take();
                if (envelope == POISON) {
                    return;
                }
                buffer.add(envelope);
            }
        } catch (InterruptedException | IOException ex) {
            Thread.currentThread().interrupt();
            LOGGER.error("Metadata consumer stopped unexpectedly", ex);
        }
    }

    private void emitDirectoryMetadata(IdRegistry idRegistry,
                                       Set<Path> directories,
                                       Map<Path, DirectoryStats> directoryStats,
                                       JsonBuffer buffer) throws IOException {
        for (Path directory : directories) {
            DirectoryStats stats = directoryStats.getOrDefault(normalize(directory), new DirectoryStats());
            DirectoryMetadata metadata = directoryMetadataFor(idRegistry, directory, stats);
            buffer.add(MetadataEnvelope.directory(metadata));
        }
    }

    private DirectoryMetadata directoryMetadataFor(IdRegistry idRegistry, Path directory, DirectoryStats stats) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(directory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        long id = idRegistry.idFor(directory);
        Path parent = directory.getParent();
        long parentId = parent == null ? -1 : idRegistry.idFor(parent);
        String parentPath = parent == null ? null : parent.toAbsolutePath().toString();
        Instant created = attrs.creationTime().toInstant();
        Instant modified = attrs.lastModifiedTime().toInstant();
        Instant accessed = attrs.lastAccessTime().toInstant();
        return new DirectoryMetadata(
                id,
                parentId,
                parentPath,
                directory.toAbsolutePath().toString(),
                directory.getFileName() == null ? directory.toString() : directory.getFileName().toString(),
                stats.totalFiles(),
                stats.totalBytes(),
                created,
                modified,
                accessed,
                attrs.fileKey() == null ? null : attrs.fileKey().toString()
        );
    }

    private void updateAncestorStats(Map<Path, DirectoryStats> directoryStats,
                                     Set<Path> roots,
                                     Path file,
                                     long size) {
        Path parent = file.getParent();
        while (parent != null) {
            Path normalized = normalize(parent);
            directoryStats.computeIfAbsent(normalized, ignored -> new DirectoryStats()).addFile(size);
            if (roots.contains(normalized)) {
                break;
            }
            parent = parent.getParent();
        }
    }

    private long safeSize(Path file) {
        try {
            return Files.size(file);
        } catch (IOException ex) {
            LOGGER.warn("Failed to read size for {}", file, ex);
            return 0L;
        }
    }

    private boolean shouldSkipPath(Path path) {
        return !config.followLinks() && Files.isSymbolicLink(path);
    }

    private void saveCheckpoint(IdRegistry idRegistry,
                                JsonBuffer buffer,
                                Deque<Path> pending,
                                Path inProgress,
                                Map<Path, DirectoryStats> directoryStats) throws IOException {
        Map<String, DirectoryStatsSnapshot> snapshot = new HashMap<>();
        for (Map.Entry<Path, DirectoryStats> entry : directoryStats.entrySet()) {
            snapshot.put(entry.getKey().toAbsolutePath().toString(), DirectoryStatsSnapshot.from(entry.getValue()));
        }
        List<String> pendingList = new ArrayList<>();
        for (Path path : pending) {
            pendingList.add(path.toAbsolutePath().toString());
        }
        RunState state = new RunState(
                idRegistry.nextId(),
                buffer.currentSequence(),
                pendingList,
                inProgress == null ? null : inProgress.toAbsolutePath().toString(),
                snapshot
        );
        checkpointManager.save(state);
    }

    private void restoreDirectoryStats(Map<Path, DirectoryStats> target,
                                       Map<String, DirectoryStatsSnapshot> snapshot) {
        snapshot.forEach((path, stats) -> {
            target.put(Path.of(path), new DirectoryStats(stats.totalFiles(), stats.totalBytes()));
        });
    }

    private Path normalize(Path path) {
        return path.toAbsolutePath().normalize();
    }
}
