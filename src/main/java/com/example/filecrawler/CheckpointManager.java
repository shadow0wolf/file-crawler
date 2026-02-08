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

    /**
     * Manages persistence of crawl checkpoints to a single JSON file.
     */
    public CheckpointManager(Path checkpointPath) {
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.checkpointPath = checkpointPath;
    }

    /**
     * Returns the last saved run state if the checkpoint file exists.
     */
    public Optional<RunState> load() throws IOException {
        if (!Files.exists(checkpointPath)) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(Files.newBufferedReader(checkpointPath), RunState.class));
    }

    /**
     * Writes the latest run state to disk, creating the parent directories if needed.
     */
    public void save(RunState state) throws IOException {
        Files.createDirectories(checkpointPath.getParent());
        mapper.writerWithDefaultPrettyPrinter().writeValue(checkpointPath.toFile(), state);
    }

    /**
     * Exposes the underlying checkpoint file path.
     */
    public Path path() {
        return checkpointPath;
    }
}
