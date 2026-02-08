package com.example.filecrawler;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class IdRegistry {
    private final ConcurrentHashMap<Path, Long> ids = new ConcurrentHashMap<>();
    private final AtomicLong counter;

    public IdRegistry(long startingId) {
        this.counter = new AtomicLong(startingId);
    }

    public long idFor(Path path) {
        return ids.computeIfAbsent(path.toAbsolutePath().normalize(), ignored -> counter.getAndIncrement());
    }

    public long nextId() {
        return counter.get();
    }

    public ConcurrentHashMap<Path, Long> snapshot() {
        return new ConcurrentHashMap<>(ids);
    }
}
