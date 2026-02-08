package com.example.filecrawler;

import com.example.filecrawler.metadata.FileMetadata;

public class FileProcessingResult {
    private final FileMetadata metadata;
    private final FailedFileRecord failure;

    private FileProcessingResult(FileMetadata metadata, FailedFileRecord failure) {
        this.metadata = metadata;
        this.failure = failure;
    }

    public static FileProcessingResult success(FileMetadata metadata) {
        return new FileProcessingResult(metadata, null);
    }

    public static FileProcessingResult failure(FailedFileRecord failure) {
        return new FileProcessingResult(null, failure);
    }

    public boolean isSuccess() {
        return metadata != null;
    }

    public FileMetadata getMetadata() {
        return metadata;
    }

    public FailedFileRecord getFailure() {
        return failure;
    }
}
