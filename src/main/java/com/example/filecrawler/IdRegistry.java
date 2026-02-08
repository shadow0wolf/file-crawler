package com.example.filecrawler;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class IdRegistry {
    private final ConcurrentHashMap<Path, Long> ids = new ConcurrentHashMap<>();
    private final AtomicLong counter;

    /**
     * Assigns stable numeric identifiers to paths within a crawl run.
     */
    public IdRegistry(long startingId) {
        this.counter = new AtomicLong(startingId);
    }

    /**
     * Returns the ID for a normalized path, allocating a new one if needed.
     */
    public long idFor(Path path) {
        return ids.computeIfAbsent(path.toAbsolutePath().normalize(), ignored -> counter.getAndIncrement());
    }

    /**
     * Returns the next available ID (used for checkpoint recovery).
     */
    public long nextId() {
        return counter.get();
    }

    /**
     * Returns a copy of the current ID mapping for diagnostics or tests.
     */
    public ConcurrentHashMap<Path, Long> snapshot() {
        return new ConcurrentHashMap<>(ids);
    }
}
