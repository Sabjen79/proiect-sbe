package org.example.util;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.example.FileLogger;

public class StatsTracker {
    public static final AtomicInteger publicationNum = new AtomicInteger(0);
    public static final AtomicInteger subscriptionNum = new AtomicInteger(0);

    public static final AtomicLong latencyValue = new AtomicLong(0);
    public static final AtomicInteger latencyCount = new AtomicInteger(0);

    public static final Set<String> matchedPubSet = ConcurrentHashMap.newKeySet();

    public static final void startTrackingLatency() {
        new Thread(() -> {
            try {
                while(true) {
                    Thread.sleep(60000);

                    FileLogger.info("Average latency of last minute: " + getAveragePubLatency());

                    latencyValue.set(0);
                    latencyCount.set(0);
                }
            } catch (InterruptedException e) {
                FileLogger.error(e.toString());
            }
        }).start();
    }

    public static double getAveragePubLatency() {
        return ((double) latencyValue.get()) / latencyCount.get();
    }
}
