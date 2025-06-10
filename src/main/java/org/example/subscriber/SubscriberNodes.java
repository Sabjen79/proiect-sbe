package org.example.subscriber;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.example.FileLogger;

public class SubscriberNodes {
    private static Map<String, Subscriber> subscribers;

    public static void initialize(int num) {
        subscribers = new HashMap<>();

        for (int i = 1; i <= num; i++) {
            subscribers.put("Subscriber" + i, new Subscriber());
        }

        FileLogger.info("Initialized Subscriber Node");
    }

    public static Subscriber getSubscriber(String name) {
        return subscribers.get(name);
    }

    public static void notifySubscriber(Subscription sub, Object message) {
        FileLogger.info(MessageFormat.format("User {0} with subscription {1} was notified with message: {2}", sub.userId, sub.toString(), message.toString()));
    }

    public static void startGenerating() {
        for (var subscriber : subscribers.values()) {
            subscriber.startGenerator();
        }

        FileLogger.info("Started Subscriber Generator");
    }
}