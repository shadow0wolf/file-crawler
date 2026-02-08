package com.example.filecrawler.metadata;

public record MetadataEnvelope(
        String type,
        Object payload
) {
    public static MetadataEnvelope file(FileMetadata metadata) {
        return new MetadataEnvelope("file", metadata);
    }

    public static MetadataEnvelope directory(DirectoryMetadata metadata) {
        return new MetadataEnvelope("directory", metadata);
    }
}
