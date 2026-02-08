package com.example.filecrawler;

import com.example.filecrawler.metadata.FileMetadata;
import com.example.filecrawler.metadata.MetadataEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonBufferTest {
    @Test
    void flushesWhenThresholdReached() throws Exception {
        Path outputDir = Files.createTempDirectory("buffer-test");
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        JsonBuffer buffer = new JsonBuffer(mapper, outputDir, 1, 1);

        FileMetadata fileMetadata = new FileMetadata(
                1L,
                -1L,
                null,
                outputDir.resolve("file.txt").toString(),
                "file.txt",
                10L,
                "text/plain",
                "hash",
                Instant.now(),
                Instant.now(),
                Instant.now(),
                null
        );

        buffer.add(MetadataEnvelope.file(fileMetadata));
        buffer.flush();

        long jsonFiles = Files.list(outputDir).count();
        assertEquals(1L, jsonFiles);
        Path file = outputDir.resolve("metadata_000001.json");
        assertTrue(Files.exists(file));
    }
}
