package com.example.filecrawler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonBuffer<T> {
    private final ObjectMapper mapper;
    private final Path outputDirectory;
    private final String prefix;
    private final int threshold;
    private final JsonFileSyncer syncer;
    private final List<T> buffer;
    private final AtomicInteger sequence;

    /**
     * Buffers metadata envelopes and writes them to sequential JSON files.
     */
    public JsonBuffer(ObjectMapper mapper, Path outputDirectory, int threshold, int startingSequence, JsonFileSyncer syncer) {
        this(mapper, outputDirectory, threshold, startingSequence, "metadata_", syncer);
    }

    public JsonBuffer(ObjectMapper mapper, Path outputDirectory, int threshold, int startingSequence) {
        this(mapper, outputDirectory, threshold, startingSequence, "metadata_", JsonFileSyncer.noop());
    }

    public JsonBuffer(ObjectMapper mapper,
                      Path outputDirectory,
                      int threshold,
                      int startingSequence,
                      String prefix) {
        this(mapper, outputDirectory, threshold, startingSequence, prefix, JsonFileSyncer.noop());
    }

    public JsonBuffer(ObjectMapper mapper,
                      Path outputDirectory,
                      int threshold,
                      int startingSequence,
                      String prefix,
                      JsonFileSyncer syncer) {
        this.mapper = mapper == null ? new ObjectMapper().registerModule(new JavaTimeModule()) : mapper;
        this.outputDirectory = outputDirectory;
        this.prefix = prefix;
        this.threshold = threshold;
        this.syncer = syncer == null ? JsonFileSyncer.noop() : syncer;
        this.buffer = new ArrayList<>(threshold);
        this.sequence = new AtomicInteger(startingSequence);
    }

    public synchronized void add(T entry) throws IOException {
        buffer.add(entry);
        if (buffer.size() >= threshold) {
            flush();
        }
    }

    public synchronized void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        Files.createDirectories(outputDirectory);
        Path file = outputDirectory.resolve(String.format("%s%06d.json", prefix, sequence.getAndIncrement()));
        mapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), buffer);
        buffer.clear();
        syncer.enqueue(file);
    }

    public synchronized void close() throws IOException {
        flush();
    }

    public synchronized int nextSequence() {
        return sequence.get();
    }
}
