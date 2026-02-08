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

## Architecture diagrams (ASCII)

### High-level components

```
                           +-------------------+
                           |   config.json     |
                           +-------------------+
                                     |
                                     v
                         +-----------------------+
                         |      ConfigLoader     |
                         +-----------------------+
                                     |
                                     v
                         +-----------------------+
                         |     CrawlerEngine     |
                         +-----------------------+
                          /          |          \
                         /           |           \
                        v            v            v
         +-------------------+  +-------------------+  +----------------------+
         | CheckpointManager |  |   Worker Pool     |  |   Consumer Thread    |
         | + RunState        |  | FileMetadataExtr. |  |      JsonBuffer      |
         +-------------------+  +-------------------+  +----------------------+
                 |                         |                    |
                 v                         v                    v
        checkpoint.json             MetadataEnvelope     metadata_*.json
```

**What this means:** `CrawlerEngine` is the hub. It reads configuration, restores a prior run if needed, seeds traversal, and fans out work. The worker pool extracts metadata and produces `MetadataEnvelope` objects, while the consumer thread batches them into JSON files. `CheckpointManager` is consulted periodically to persist the current queue, counters, and directory stats so a crash or pause can be resumed deterministically.

### Detailed runtime flow (queues, threads, and IO)

```
        Roots from config
               |
               v
     +------------------+
     | Directory Queue  |<-------------------------------+
     +------------------+                                |
               |                                           |
               v                                           |
     +------------------+     files + subdirs     +--------+-------+
     | Traversal Loop   |------------------------>| IdRegistry     |
     | (CrawlerEngine)  |                         +----------------+
     +------------------+                                |
               |                                         |
       +-------+-------------------+                     |
       |                           |                     |
       v                           v                     |
+---------------+        +-------------------+           |
| Worker Pool   |        | DirectoryStats    |           |
| (Extractor)   |        | (per directory)   |           |
+---------------+        +-------------------+           |
       |                           |                     |
       v                           v                     |
MetadataEnvelope            DirectoryStatsSnapshot       |
       |                           |                     |
       +------------+              |                     |
                    v              v                     |
               +-------------------------+               |
               | Bounded Envelope Queue  |<--------------+
               +-------------------------+
                            |
                            v
                    +---------------+
                    | JsonBuffer    |
                    | (flush batch) |
                    +---------------+
                            |
                            v
                     metadata_*.json
```

**Step-by-step explanation:**

1. **Seed traversal:** `CrawlerEngine` reads the configured roots and pushes the initial directories onto the in-memory queue.
2. **Traverse and register IDs:** Each directory is popped, assigned a stable numeric ID via `IdRegistry`, and inspected for subdirectories and files.
3. **Dispatch extraction:** File paths are submitted to the worker pool, where `FileMetadataExtractor` performs filesystem reads, MIME detection, hashing, and size/timestamp collection.
4. **Aggregate directory stats:** As files are visited, `DirectoryStats` accumulates totals (counts, bytes, and hashes) and snapshots are created for checkpoint persistence.
5. **Buffer envelopes:** Extracted file and directory records are wrapped in `MetadataEnvelope` objects and placed on the bounded queue for the consumer.
6. **Flush output:** `JsonBuffer` consumes envelopes in batches and writes `metadata_000001.json`, `metadata_000002.json`, etc., ensuring deterministic ordering and segment sizes.

### Checkpointing and recovery sequence

```
                +-------------------+
Start --------->| Checkpoint exists?|--No--> Fresh run
                +-------------------+
                         |
                        Yes
                         v
                +-------------------+
                | Load RunState     |
                | (queue, counters, |
                |  stats snapshots) |
                +-------------------+
                         |
                         v
                +-------------------+
                | Resume traversal  |
                | + continue batch  |
                | numbering         |
                +-------------------+
```

**Why this is safe:** every N entries (configurable by `checkpointEntryInterval`), the crawler persists the directory queue, sequence numbers, and `DirectoryStatsSnapshot` so that a resumed run can continue from the same logical position without re-emitting already-flushed metadata batches.

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
