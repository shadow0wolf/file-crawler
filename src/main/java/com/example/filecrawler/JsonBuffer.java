package com.example.filecrawler;

import com.example.filecrawler.metadata.MetadataEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class JsonBuffer {
    private final ObjectMapper mapper;
    private final Path outputDirectory;
    private final int threshold;
    private final List<MetadataEnvelope> buffer = new ArrayList<>();
    private final AtomicInteger sequence;

    /**
     * Buffers metadata envelopes and writes them to sequential JSON files.
     */
    public JsonBuffer(ObjectMapper mapper, Path outputDirectory, int threshold, int startingSequence) {
        this.mapper = mapper;
        this.outputDirectory = outputDirectory;
        this.threshold = threshold;
        this.sequence = new AtomicInteger(startingSequence);
    }

    /**
     * Returns the next sequence number to be used when flushing output.
     */
    public synchronized int currentSequence() {
        return sequence.get();
    }

    /**
     * Adds a metadata envelope to the buffer, flushing when the threshold is met.
     */
    public synchronized void add(MetadataEnvelope envelope) throws IOException {
        buffer.add(envelope);
        if (buffer.size() >= threshold) {
            flush();
        }
    }

    /**
     * Flushes the current buffer to a JSON file and clears the in-memory list.
     */
    public synchronized void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        Files.createDirectories(outputDirectory);
        String fileName = String.format("metadata_%06d.json", sequence.getAndIncrement());
        Path target = outputDirectory.resolve(fileName);
        mapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), buffer);
        buffer.clear();
    }
}
