package com.example.filecrawler.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Serialized metadata describing a single file and its derived attributes.
 */
public record FileMetadata(
        long id,
        long parentId,
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) String parentPath,
        String path,
        String name,
        long size,
        String mimeType,
        String hashSha256,
        Instant createdTime,
        Instant lastModifiedTime,
        Instant lastAccessTime,
        String fileKey
) {
}
