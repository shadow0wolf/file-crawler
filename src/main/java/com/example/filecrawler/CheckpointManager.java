package com.example.filecrawler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public final class CheckpointManager {
    private final ObjectMapper mapper;
    private final Path checkpointPath;

    public CheckpointManager(Path checkpointPath) {
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.checkpointPath = checkpointPath;
    }

    public Optional<RunState> load() throws IOException {
        if (!Files.exists(checkpointPath)) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(Files.newBufferedReader(checkpointPath), RunState.class));
    }

    public void save(RunState state) throws IOException {
        Files.createDirectories(checkpointPath.getParent());
        mapper.writerWithDefaultPrettyPrinter().writeValue(checkpointPath.toFile(), state);
    }

    public Path path() {
        return checkpointPath;
    }
}
