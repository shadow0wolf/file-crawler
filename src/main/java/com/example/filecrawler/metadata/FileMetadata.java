package com.example.filecrawler.metadata;

import java.time.Instant;

public record FileMetadata(
        long id,
        long parentId,
        String parentPath,
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
