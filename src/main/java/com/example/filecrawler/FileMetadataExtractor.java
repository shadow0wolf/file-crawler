package com.example.filecrawler;

import com.example.filecrawler.metadata.FileMetadata;
import org.apache.commons.codec.binary.Hex;
import org.apache.tika.Tika;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

public final class FileMetadataExtractor {
    private final Tika tika;

    /**
     * Extracts metadata for files using Apache Tika for MIME detection.
     */
    public FileMetadataExtractor(Tika tika) {
        this.tika = tika;
    }

    /**
     * Reads filesystem metadata and derived fields (MIME type + SHA-256 hash).
     */
    public FileMetadata extract(Path path, long id, long parentId) throws IOException {
        String parentPath = path.getParent() == null ? null : path.getParent().toAbsolutePath().toString();
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        long size = attrs.size();
        String mimeType = detectMimeType(path);
        String hash = hashSha256(path);
        Instant created = attrs.creationTime().toInstant();
        Instant modified = attrs.lastModifiedTime().toInstant();
        Instant accessed = attrs.lastAccessTime().toInstant();
        return new FileMetadata(
                id,
                parentId,
                parentPath,
                path.toAbsolutePath().toString(),
                path.getFileName() == null ? path.toString() : path.getFileName().toString(),
                size,
                mimeType,
                hash,
                created,
                modified,
                accessed,
                attrs.fileKey() == null ? null : attrs.fileKey().toString()
        );
    }

    private String detectMimeType(Path path) throws IOException {
        try (InputStream stream = Files.newInputStream(path)) {
            return tika.detect(stream, path.getFileName() == null ? path.toString() : path.getFileName().toString());
        }
    }

    private String hashSha256(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 not available", ex);
        }
        // Stream file data to avoid loading large files into memory.
        try (InputStream stream = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = stream.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return Hex.encodeHexString(digest.digest());
    }
}
