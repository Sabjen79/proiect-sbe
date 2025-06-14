package org.example.topology.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.example.App;
import org.example.FileLogger;
import org.example.data.WeatherDataValues;
import org.example.subscriber.Operation;
import org.example.subscriber.Subscription;

/**
 * Bolt that receives and stores subscriptions, that are then compared and matched against any publication that it receives. 
 * Publications are always forwarded to the next bolt of this type, until all of them parsed it at least once.
 * Subscriptions, if any of them matches a publication, the latter will be send to the corresponding subscriber as a notification.
 * All complex subscriptions are passed to ComplexFilterBolts, while simple ones are stored locally.
 */
public class SimpleFilterBolt extends BaseRichBolt {
    private ConcurrentHashMap<String, List<Subscription>> subscriptionsMap;

    private OutputCollector collector;

    private static final long TEST_DURATION_MS = 90_000; // 3 minutes
    private static long testStartTime = -1;
    private static AtomicInteger successfulDeliveries = new AtomicInteger(0);
    private static AtomicBoolean logged = new AtomicBoolean(false);

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.subscriptionsMap = new ConcurrentHashMap<>();
        this.collector = collector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {
        if (testStartTime == -1) {
            testStartTime = System.currentTimeMillis();
        }
        long now = System.currentTimeMillis();

        if (now - testStartTime < TEST_DURATION_MS) {


            if (input.contains("subscription")) {

                var sub = (Subscription) input.getValueByField("subscription");
                String city = input.getStringByField("city");

                boolean isComplex = false;
                for (var condition : sub.conditions) {
                    for (var prefix : WeatherDataValues.complexPrefixes) {
                        if(condition.key.startsWith(prefix)) {
//                            collector.emit("forward-subscription", new Values(sub, city));
                            isComplex = true;
                            break;
                        }
                    }
                    if (isComplex) break;
                }

                if (isComplex) {
                    collector.emit("forward-subscription", new Values(sub, city));
                } else {
                    subscriptionsMap.putIfAbsent(city, new ArrayList<>());
                    subscriptionsMap.get(city).add(sub);
                }
                // Only save simple subscriptions
//                subscriptionsMap.putIfAbsent(city, new ArrayList<>());
//                subscriptionsMap.get(city).add(sub);
            }

            if (input.contains("publication")) {

                var publication = (Map<String, String>) input.getValueByField("publication");
                String city = input.getStringByField("city");

                int hops = input.contains("hops") ? input.getIntegerByField("hops") : 0;

                if (hops < App.BROKER_NUM) {
                    collector.emit("forward-publication", new Values(publication, city, hops + 1));
                } else return;

                var subscriptions = subscriptionsMap.getOrDefault(city, List.of());

                for (var sub : subscriptions) {
                    if (matchesSubscription(sub, publication)) {
                        collector.emit("notify", new Values(publication, sub));
                        successfulDeliveries.incrementAndGet();
                    }
                }

            }
            
        }

        if (now - testStartTime >= TEST_DURATION_MS && logged.compareAndSet(false, true)) {
            FileLogger.info("Total successful deliveries in 1.5 minutes: " + successfulDeliveries.get());
        }
    }

    private boolean matchesSubscription(Subscription sub, Map<String, String> publication) {
        for (var condition : sub.conditions) {
            // skip city field
            if (condition.key.equals(WeatherDataValues.fields[1])) {
                continue;
            }

            if (!Operation.compare(publication.get(condition.key), condition.operation, condition.value.toString())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Sends a publication to the next SimpleFilterBolt
        declarer.declareStream("forward-publication", new Fields("publication", "city", "hops"));

        // Sends a complex subscription to a ComplexFilterBolt
        declarer.declareStream("forward-subscription", new Fields("subscription", "city"));

        // Sends publication and subscription data to a ClientBolt for notifying
        declarer.declareStream("notify", new Fields("publication", "subscription"));
    }
}
