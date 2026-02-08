package com.example.filecrawler;

import java.nio.file.Path;

public final class App {
    private App() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java -jar file-crawler.jar <config.json>");
            System.exit(1);
        }

        ConfigLoader loader = new ConfigLoader();
        CrawlerConfig config = loader.load(Path.of(args[0]));
        CheckpointManager checkpointManager = config.checkpointFile()
                .map(CheckpointManager::new)
                .orElse(null);
        CrawlerEngine engine = new CrawlerEngine(config, checkpointManager, ProcessingLimiter.NO_LIMIT);
        engine.run();
    }
}
