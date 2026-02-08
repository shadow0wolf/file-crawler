package com.example.filecrawler;

import com.example.filecrawler.metadata.FileMetadata;
import org.apache.tika.Tika;
import org.apache.tika.mime.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

public class FileMetadataExtractor {
    private final Tika tika;

    public FileMetadataExtractor(Tika tika) {
        this.tika = tika;
    }

    public FileMetadata extract(Path path, long id, long parentId) throws IOException {
        return extract(path, id, parentId, true);
    }

    public FileMetadata extract(Path path, long id, long parentId, boolean followLinks) throws IOException {
        LinkOption[] linkOptions = followLinks ? new LinkOption[0] : new LinkOption[]{LinkOption.NOFOLLOW_LINKS};
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, linkOptions);
        long size = attributes.size();
        Instant createdTime = attributes.creationTime().toInstant();
        Instant modifiedTime = attributes.lastModifiedTime().toInstant();
        Instant accessTime = attributes.lastAccessTime().toInstant();
        String mimeType = detectMimeType(path);
        String sha256 = computeSha256(path);
        String parentPath = path.getParent() == null ? null : path.getParent().toString();
        String fileKey = attributes.fileKey() == null ? null : attributes.fileKey().toString();
        return new FileMetadata(
                id,
                parentId,
                parentPath,
                path.toString(),
                path.getFileName().toString(),
                size,
                mimeType,
                sha256,
                createdTime,
                modifiedTime,
                accessTime,
                fileKey
        );
    }

    private String detectMimeType(Path path) {
        try {
            MediaType mediaType = MediaType.parse(tika.detect(path));
            return mediaType == null ? "application/octet-stream" : mediaType.toString();
        } catch (IOException ex) {
            return "application/octet-stream";
        }
    }

    private String computeSha256(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
        try (InputStream inputStream = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        byte[] hash = digest.digest();
        StringBuilder builder = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }
}
