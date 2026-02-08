package com.example.filecrawler.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Serialized metadata describing a directory and its aggregated file stats.
 */
public record DirectoryMetadata(
        long id,
        long parentId,
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) String parentPath,
        String path,
        String name,
        long totalFiles,
        long totalBytes,
        Instant createdTime,
        Instant lastModifiedTime,
        Instant lastAccessTime,
        String fileKey
) {
}
