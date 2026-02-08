package com.example.filecrawler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class S3SyncService implements JsonFileSyncer {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SyncService.class);
    private final BlockingQueue<SyncTask> queue = new LinkedBlockingQueue<>();
    private final S3Client s3Client;
    private final Thread worker;
    private final Path outputDirectory;
    private final String bucket;
    private final String prefix;
    private volatile boolean closed;

    public S3SyncService(Path outputDirectory, String bucket, String prefix, Optional<String> region) {
        this.outputDirectory = outputDirectory.toAbsolutePath().normalize();
        this.bucket = bucket;
        this.prefix = normalizePrefix(prefix);
        this.s3Client = region
                .map(Region::of)
                .map(r -> S3Client.builder().region(r).build())
                .orElseGet(() -> S3Client.builder().build());
        this.worker = new Thread(this::run, "s3-sync");
        this.worker.start();
    }

    @Override
    public void enqueue(Path path) {
        if (closed) {
            LOGGER.warn("Skipping S3 sync for {} because the sync service is closed.", path);
            return;
        }
        queue.offer(new SyncTask(path));
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        queue.offer(SyncTask.poisonPill());
        try {
            worker.join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for S3 sync worker to stop.", ex);
        } finally {
            s3Client.close();
        }
    }

    private void run() {
        try {
            while (true) {
                SyncTask task = queue.take();
                if (task.poison()) {
                    return;
                }
                upload(task.path());
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.warn("S3 sync worker interrupted; exiting.", ex);
        }
    }

    private void upload(Path path) {
        try {
            Path relative = outputDirectory.relativize(path.toAbsolutePath().normalize());
            String key = prefix.isEmpty()
                    ? normalizeKey(relative)
                    : prefix + "/" + normalizeKey(relative);
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            s3Client.putObject(request, RequestBody.fromFile(path));
            LOGGER.info("Uploaded {} to s3://{}/{}", path, bucket, key);
        } catch (Exception ex) {
            LOGGER.warn("Failed to upload {} to S3", path, ex);
        }
    }

    private String normalizeKey(Path relative) {
        return relative.toString().replace("\\", "/");
    }

    private String normalizePrefix(String raw) {
        if (raw == null) {
            return "";
        }
        return raw.replaceAll("/+$", "");
    }

    private record SyncTask(Path path, boolean poison) {
        private SyncTask(Path path) {
            this(path, false);
        }

        private static SyncTask poisonPill() {
            return new SyncTask(null, true);
        }
    }
}
