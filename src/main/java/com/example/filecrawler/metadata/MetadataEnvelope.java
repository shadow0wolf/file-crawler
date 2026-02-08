package com.example.filecrawler.metadata;

/**
 * Wrapper object used to emit mixed file and directory metadata into one stream.
 */
public record MetadataEnvelope(
        String type,
        Object payload
) {
    /**
     * Creates a file metadata envelope.
     */
    public static MetadataEnvelope file(FileMetadata metadata) {
        return new MetadataEnvelope("file", metadata);
    }

    /**
     * Creates a directory metadata envelope.
     */
    public static MetadataEnvelope directory(DirectoryMetadata metadata) {
        return new MetadataEnvelope("directory", metadata);
    }
}
