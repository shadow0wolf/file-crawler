# File Crawler (JDK 17 + Maven)

This project crawls configured folders, extracts file and directory metadata, buffers it as JSON, and flushes batches to disk with checkpoint recovery.

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
