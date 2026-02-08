package com.example.filecrawler;

import java.nio.file.Path;

@FunctionalInterface
public interface JsonFileSyncer extends AutoCloseable {
    void enqueue(Path path);

    @Override
    default void close() {
        // no-op
    }

    static JsonFileSyncer noop() {
        return path -> {
        };
    }
}
