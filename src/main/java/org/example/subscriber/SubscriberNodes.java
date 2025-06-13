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

        for (int i = 1; i <= num; i++) {
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
        // decryptSubscription(sub);

        FileLogger.info(MessageFormat.format("User {0} with subscription {1} was notified with message: {2}", sub.userId, sub.toString(), message.toString()));
    }

    public static void notifySubscriberPublication(Subscription sub, Map<String, String> publication) {
        // decryptSubscription(sub);

        // publication.replace("station_id", SimpleOPE.decryptString(publication.get("station_id")));
        // publication.replace("city", SimpleOPE.decryptString(publication.get("city")));

        // publication.replace("temp", SimpleOPE.decryptLong(
        //     Long.parseLong(publication.get("temp"))
        // ).toString());

        // publication.replace("wind", SimpleOPE.decryptLong(
        //     Long.parseLong(publication.get("wind"))
        // ).toString());

        // publication.replace("rain", SimpleOPE.decryptDouble(
        //     Long.parseLong(publication.get("rain"))
        // ).toString());

        // publication.replace("date", SimpleOPE.decryptDouble(
        //     Long.parseLong(publication.get("date"))
        // ).toString());

        // var weatherDirection = SimpleOPE.decryptLong(Long.parseLong(publication.get("direction")));

        // publication.replace("direction", WeatherDirection.forNumber(
        //     Math.toIntExact(weatherDirection)
        // ).toString());

        FileLogger.info(MessageFormat.format("User {0} with subscription {1} was notified with publication: {2}", sub.userId, sub.toString(), publication.toString()));
    }

    private static void decryptSubscription(Subscription sub) {
        sub.conditions = sub.conditions.stream().map((c) -> {
            switch (c.type) {
                case 0:
                    c.value = SimpleOPE.decryptString(c.value);
                    break;
            
                case 1:
                    c.value = SimpleOPE.decryptLong(Long.valueOf(c.value)).toString();
                    break;

                case 2:
                    c.value = SimpleOPE.decryptDouble(Long.valueOf(c.value)).toString();
                    break;
            }

            return c;
        }).toList();
    }
}