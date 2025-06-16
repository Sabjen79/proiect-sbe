package org.example.subscriber;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.example.FileLogger;
import org.example.data.WeatherDataOuterClass.WeatherData.WeatherDirection;
import org.example.data.encryption.SimpleOPE;

public class SubscriberNodes {
    private static Map<String, Subscriber> subscribers;

    public static void initialize(int num) {
        subscribers = new HashMap<>();

        for (int i = 0; i < num; i++) {
            subscribers.put("Subscriber" + i, new Subscriber());
        }

        FileLogger.info("Initialized Subscriber Node");
    }

    public static void startGenerating() {
        for (var subscriber : subscribers.values()) {
            subscriber.startGenerator();
        }

        FileLogger.info("Started Subscriber Generator");
    }

    public static Subscriber getSubscriber(String name) {
        return subscribers.get(name);
    }

    public static void notifySubscriberString(Subscription sub, String message) {
        sub = decryptSubscription(sub);

        FileLogger.debug(MessageFormat.format("User {0} with subscription {1} was notified with message: {2}", sub.userId, sub.toString(), message.toString()));
    }

    public static void notifySubscriberPublication(Subscription sub, Map<String, String> encryptedPublication) {
        var publication = decryptPublication(encryptedPublication);

        notifySubscriberString(sub, publication.toString());
    }

    private static Subscription decryptSubscription(Subscription sub) {
        var conditions = sub.conditions.stream().map((c) -> {
            var value = "";

            switch (c.type) {
                case 0:
                    value = SimpleOPE.decryptString(c.value);
                    break;
            
                case 1:
                    value = SimpleOPE.decryptLong(Long.valueOf(c.value)).toString();
                    break;

                case 2:
                    value = SimpleOPE.decryptDouble(Long.valueOf(c.value)).toString();
                    break;
            }

            return new SubCondition(c.key, c.operation, value, c.type);
        }).toList();
        
        return new Subscription(sub.userId, conditions);
    }

    private static HashMap<String, String> decryptPublication(Map<String, String> encryptedPublication) {
        var publication = new HashMap<String, String>();

        publication.put("station_id", SimpleOPE.decryptString(encryptedPublication.get("station_id")));
        publication.put("city", SimpleOPE.decryptString(encryptedPublication.get("city")));

        publication.put("temp", SimpleOPE.decryptLong(
            Long.parseLong(encryptedPublication.get("temp"))
        ).toString());

        publication.put("wind", SimpleOPE.decryptLong(
            Long.parseLong(encryptedPublication.get("wind"))
        ).toString());

        publication.put("rain", SimpleOPE.decryptDouble(
            Long.parseLong(encryptedPublication.get("rain"))
        ).toString());

        publication.put("date", encryptedPublication.get("date"));

        var weatherDirection = SimpleOPE.decryptLong(Long.parseLong(encryptedPublication.get("direction")));

        publication.put("direction", WeatherDirection.forNumber(
            Math.toIntExact(weatherDirection)
        ).toString());

        return publication;
    }
}