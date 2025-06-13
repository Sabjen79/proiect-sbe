package org.example.topology.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.example.FileLogger;
import org.example.data.WeatherDataValues;
import org.example.subscriber.Operation;
import org.example.subscriber.Subscription;

public class SimpleFilterBolt extends BaseRichBolt {
    // map that stores subscriptions per city
    private ConcurrentHashMap<String, List<Subscription>> subscriptionsMap;

    private OutputCollector collector;
    private String brokerId;

    // maximum number of hops allowed for forwarding
    private final int MAX_HOPS = 3;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.subscriptionsMap = new ConcurrentHashMap<>();
        this.collector = collector;
        this.brokerId = (String) topoConf.getOrDefault("broker.id", "default-broker");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {
        // retrieve hop count, defaulting to 0 if missing
        int currentHops = input.contains("hops") ? input.getIntegerByField("hops") : 0;

        // handle subscription messages
        if (input.contains("subscription")) {
            var sub = (Subscription) input.getValueByField("subscription");
            String city = input.getStringByField("city");

            FileLogger.debug("Routing subscription: " + sub + " | hops=" + currentHops + " | on broker=" + brokerId);

            // store the subscription locally
            subscriptionsMap.putIfAbsent(city, new ArrayList<>());
            var subscriptions = subscriptionsMap.get(city);
            subscriptions.add(sub);

            FileLogger.debug("Added subscription locally: " + sub + " | broker=" + brokerId);

            // forward the subscription if hop count allows
            if (currentHops < MAX_HOPS) {
                FileLogger.debug("Forwarding subscription to next broker: " + sub + " | from broker=" + brokerId);
                collector.emit("forward", new Values(sub, city, currentHops + 1));
            }

            collector.emit("routing-info", new Values(brokerId, "subscription-added", sub.toString()));
        }

        // handle publication messages
        if (input.contains("publication")) {
            var publication = (Map<String, String>) input.getValueByField("publication");
            String city = input.getStringByField("city");

            FileLogger.debug("Received publication: " + publication + " | city=" + city + " | currentHops=" + currentHops + " | on broker=" + brokerId);

            // retrieve local subscriptions for the city
            var subscriptions = subscriptionsMap.getOrDefault(city, List.of());

            // check if publication matches any subscription
            for (var sub : subscriptions) {
                if (matchesSubscription(sub, publication)) {
                    collector.emit("notify", new Values(sub, publication, currentHops + 1));
                    FileLogger.debug("Matched publication to subscription: " + sub + " | notifying");
                }
            }

            // forward publication to the next broker if hop count allows
            if (currentHops < MAX_HOPS) {
                FileLogger.debug("Forwarding publication to next broker: " + publication + " | city=" + city + " | currentHops=" + currentHops + " | from broker=" + brokerId);
                collector.emit("forward-publication", new Values(publication, city, currentHops + 1));
            }
        }
    }

    // checks if a publication satisfies all conditions in a subscription
    private boolean matchesSubscription(Subscription sub, Map<String, String> publication) {
        for (var condition : sub.conditions) {
            // skip city field
            if (condition.key.equals(WeatherDataValues.fields[1])) {
                continue;
            }

            FileLogger.info(condition.key);
            if (!Operation.compare(publication.get(condition.key), condition.operation, condition.value.toString())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("forward", new Fields("subscription", "city", "hops"));// stream for forwarding subscriptions to other brokers
        declarer.declareStream("notify", new Fields("subscription", "publication", "hops"));// stream for notifying matched subscriptions
        declarer.declareStream("forward-publication", new Fields("publication", "city", "hops"));// stream for forwarding publications to other brokers
        declarer.declareStream("routing-info", new Fields("broker-id", "action", "details"));// stream for logging routing-related events
    }
}
