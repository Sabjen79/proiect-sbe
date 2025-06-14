package org.example.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

public class LatencyTracker {
    private static final AtomicLong totalLatency = new AtomicLong(0);
    private static final AtomicInteger count = new AtomicInteger(0);

    public static void addLatency(long latency) {
        totalLatency.addAndGet(latency);
        count.incrementAndGet();
    }

    public static double getAverageLatency() {
        int c = count.get();
        if (c == 0) return 0.0;
        return totalLatency.get() / (double) c;
    }

    public static void reset() {
        totalLatency.set(0);
        count.set(0);
    }
}
