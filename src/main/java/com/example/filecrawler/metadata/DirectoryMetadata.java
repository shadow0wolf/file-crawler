package com.example.filecrawler.metadata;

import java.time.Instant;

public record DirectoryMetadata(
        long id,
        long parentId,
        String parentPath,
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
