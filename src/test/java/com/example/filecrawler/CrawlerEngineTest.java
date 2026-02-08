package com.example.filecrawler;

import com.example.filecrawler.metadata.DirectoryMetadata;
import com.example.filecrawler.metadata.MetadataEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrawlerEngineTest {
    @Test
    void resumesFromCheckpoint() throws Exception {
        Path root = Files.createTempDirectory("crawler-root");
        Path output = Files.createTempDirectory("crawler-output");
        Path subDir = Files.createDirectory(root.resolve("sub"));
        Files.writeString(root.resolve("file1.txt"), "alpha");
        Files.writeString(subDir.resolve("file2.txt"), "bravo");

        CrawlerConfig config = new CrawlerConfig(
                List.of(root),
                output,
                2,
                1,
                2,
                false,
                java.util.Optional.of(output.resolve("checkpoint.json")),
                java.util.Optional.empty(),
                2,
                List.of(),
                List.of()
        );

        CheckpointManager checkpointManager = new CheckpointManager(output.resolve("checkpoint.json"));
        CrawlerEngine firstRun = new CrawlerEngine(config, checkpointManager, processed -> processed >= 1);
        firstRun.run();

        assertTrue(Files.exists(output.resolve("checkpoint.json")));

        CrawlerEngine secondRun = new CrawlerEngine(config, checkpointManager, ProcessingLimiter.NO_LIMIT);
        secondRun.run();

        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        List<MetadataEnvelope> envelopes = new ArrayList<>();
        try (var stream = Files.list(output)) {
            stream.filter(path -> path.getFileName().toString().startsWith("metadata_"))
                    .forEach(path -> {
                        try {
                            MetadataEnvelope[] batch = mapper.readValue(path.toFile(), MetadataEnvelope[].class);
                            envelopes.addAll(List.of(batch));
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    });
        }

        long fileCount = envelopes.stream().filter(env -> env.type().equals("file")).count();
        long directoryCount = envelopes.stream().filter(env -> env.type().equals("directory")).count();
        assertEquals(2L, fileCount);
        assertEquals(2L, directoryCount);

        DirectoryMetadata rootMetadata = envelopes.stream()
                .filter(env -> env.type().equals("directory"))
                .map(env -> mapper.convertValue(env.payload(), DirectoryMetadata.class))
                .filter(meta -> meta.name().equals(root.getFileName().toString()))
                .findFirst()
                .orElseThrow();
        assertEquals(2L, rootMetadata.totalFiles());
    }
}
