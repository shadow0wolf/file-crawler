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

    public JsonBuffer(ObjectMapper mapper, Path outputDirectory, int threshold, int startingSequence) {
        this.mapper = mapper;
        this.outputDirectory = outputDirectory;
        this.threshold = threshold;
        this.sequence = new AtomicInteger(startingSequence);
    }

    public synchronized int currentSequence() {
        return sequence.get();
    }

    public synchronized void add(MetadataEnvelope envelope) throws IOException {
        buffer.add(envelope);
        if (buffer.size() >= threshold) {
            flush();
        }
    }

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
