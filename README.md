# File Crawler (JDK 17 + Maven)

This project crawls configured folders, extracts file and directory metadata, buffers it as JSON, and flushes batches to disk with checkpoint recovery.

## Philosophy

The crawler prioritizes **deterministic output** and **recoverable long-running scans**. It favors explicit checkpoints over implicit retry logic, uses immutable metadata records for serialization, and separates traversal from extraction so that file system IO can continue while metadata is buffered and written. The design is intentionally pragmatic: prefer simple concurrency patterns (a bounded queue and a fixed thread pool) to keep recovery and output predictable.

## Core abstractions

The codebase is intentionally small and centers around a few abstractions:

- **CrawlerEngine**: orchestrates traversal, parallel file metadata extraction, buffering, and checkpointing.
- **JsonBuffer**: batches metadata envelopes and writes sequential JSON files.
- **CheckpointManager + RunState**: persist and reload crawl progress.
- **IdRegistry**: provides stable numeric IDs for paths during a run.
- **FileMetadataExtractor**: encapsulates filesystem reads, MIME detection, and hashing.
- **DirectoryStats + DirectoryStatsSnapshot**: track and serialize aggregated directory totals.
- **MetadataEnvelope**: normalizes file and directory records into a single output stream.
- **ProcessingLimiter**: allows early stopping for testing and recovery validation.

## Module walkthrough & interactions

1. **App** loads a JSON config and wires the engine, checkpoint manager, and optional limiter.
2. **ConfigLoader** reads the JSON payload and applies defaults (buffer sizes, threads, checkpoint path).
3. **CrawlerEngine** restores the prior **RunState** (if present), seeds the directory queue, and spawns:
   - a fixed worker pool to **FileMetadataExtractor**, and
   - a consumer thread that streams **MetadataEnvelope** objects into **JsonBuffer**.
4. While traversing, **DirectoryStats** are updated for each file and persisted into the checkpoint via **DirectoryStatsSnapshot**.
5. **JsonBuffer** flushes metadata arrays into `metadata_*.json` files when the threshold is met.
6. On completion, **CrawlerEngine** emits directory metadata, flushes, and writes a final checkpoint.

## Build

```bash
mvn test
```

## Run

```bash
mvn -q -DskipTests package
java -jar target/file-crawler-1.0.0.jar path/to/config.json
```

## Sample config

```json
{
  "roots": [
    "C:/data",
    "D:/archive"
  ],
  "outputDirectory": "C:/crawler-output",
  "bufferEntryThreshold": 1000,
  "checkpointEntryInterval": 2000,
  "threadCount": 8,
  "followLinks": false,
  "checkpointFile": "C:/crawler-output/checkpoint.json"
}
```

### Optional testing field

`maxEntries` can be set to limit the number of files processed before a checkpoint is written (useful for testing recovery).

## Output

Each flush writes a JSON array of `MetadataEnvelope` objects to `metadata_000001.json`, `metadata_000002.json`, etc. Each envelope contains a `type` (`file` or `directory`) and the related metadata payload.

## Recovery

On startup the crawler checks for a checkpoint file. If it exists, the crawl resumes from the last saved queue, and sequence numbers continue from the last flushed batch.
