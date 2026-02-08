package com.example.filecrawler;

import com.example.filecrawler.metadata.FileMetadata;
import org.apache.tika.Tika;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileMetadataExtractorTest {
    @Test
    void detectsMimeTypeAndHash() throws Exception {
        Path tempDir = Files.createTempDirectory("extractor-test");
        Path pngFile = tempDir.resolve("image.png");
        byte[] pngHeader = new byte[] {(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
        Files.write(pngFile, pngHeader);

        FileMetadataExtractor extractor = new FileMetadataExtractor(new Tika());
        FileMetadata metadata = extractor.extract(pngFile, 1L, -1L);

        assertTrue(metadata.mimeType().contains("png"));
        assertEquals("image.png", metadata.name());
        assertTrue(metadata.hashSha256().length() == 64);
    }
}
