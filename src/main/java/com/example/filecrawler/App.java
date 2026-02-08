package com.example.filecrawler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public final class App {
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    private App() {
    }

    public static void main(String[] args) throws Exception {
        // Basic CLI contract: a single JSON config file path is required.
        if (args.length < 1) {
            LOGGER.error("Usage: java -jar file-crawler.jar <config.json>");
            System.exit(1);
        }
        Path configPath = Path.of(args[0]);
        CrawlerConfig config = ConfigLoader.load(configPath);
        // Default checkpoint path is under the output directory if not explicitly set.
        CheckpointManager checkpointManager = new CheckpointManager(config.checkpointFile().orElse(config.outputDirectory().resolve("checkpoint.json")));
        ProcessingLimiter limiter = config.maxEntries()
                .map(max -> (ProcessingLimiter) processed -> processed >= max)
                .orElse(ProcessingLimiter.NO_LIMIT);
        CrawlerEngine engine = new CrawlerEngine(config, checkpointManager, limiter);
        engine.run();
    }
}
