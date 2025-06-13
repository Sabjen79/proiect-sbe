package org.example.publisher;

import java.util.HashMap;
import java.util.Map;

import org.example.FileLogger;

public class PublisherNodes {
    private static Map<String, Publisher> publishers;

    public static void initialize(int num) {
        publishers = new HashMap<>();

        for (int i = 0; i < num; i++) {
            publishers.put("Publisher" + i, new Publisher());
        }

        FileLogger.info("Initialized Publisher Node");
    }

    public static Publisher getPublisher(String name) {
        return publishers.get(name);
    }

    public static void startGenerating() {
        for (var publisher : publishers.values()) {
            publisher.startGenerator();
        }

        FileLogger.info("Started Publisher Generator");
    }
}
