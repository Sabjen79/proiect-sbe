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
import org.example.App;
import org.example.data.WeatherDataValues;
import org.example.subscriber.Operation;
import org.example.subscriber.Subscription;

/**
 * Bolt that receives and stores complex subscriptions, that are then compared and matched against any publication window that it receives. 
 * Publications windows are always forwarded to the next bolt of this type, until all of them parsed it at least once.
 * Subscriptions, if any of them matches a publication window, a meta-publication will be sent to a corresponding subscriber as a notification.
 */
public class ComplexFilterBolt extends BaseRichBolt {
    private ConcurrentHashMap<String, List<Subscription>> subscriptionsMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.subscriptionsMap = new ConcurrentHashMap<>();
        this.collector = collector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {
        if(input.contains("subscription")) {
            var sub = (Subscription) input.getValueByField("subscription");
            String city = input.getStringByField("city");

            subscriptionsMap.putIfAbsent(city, new ArrayList<>());
            subscriptionsMap.get(city).add(sub);
        }

        if (input.contains("publication-list")) {
            var publicationList = (List<Map<String, String>>) input.getValueByField("publication-list");
            String city = publicationList.getFirst().get(WeatherDataValues.fields[1]);

            int hops = input.contains("hops") ? input.getIntegerByField("hops") : 0;

            if(hops < App.BROKER_NUM) {
                collector.emit("forward-publications", new Values(publicationList, hops + 1));
            } else return;

            var subscriptions = subscriptionsMap.getOrDefault(city, List.of());

            for (var sub : subscriptions) {
                if (matchesSubscription(sub, publicationList)) {
                    var metaPub = "{city = " + city + ", conditions = true}";
                    collector.emit("notify", new Values(metaPub, sub));
                }
            }


        }
    }

    private boolean matchesSubscription(Subscription sub, List<Map<String, String>> publicationList) {
        for (var condition : sub.conditions) {
            if (condition.key.equals(WeatherDataValues.fields[1])) {
                continue; // skip 'city'
            }

            var splitKey = condition.key.split("_");
            if (splitKey.length < 2) continue;

            var prefix = splitKey[0];
            var field = splitKey[1];

            double aggValue = 0;
            switch (prefix) {
                case "min":
                    aggValue = publicationList
                        .stream()
                        .mapToDouble(p -> Double.parseDouble(p.get(field)))
                        .min()
                        .orElse(Double.NaN);
                    break;

                case "max":
                    aggValue = publicationList
                        .stream()
                        .mapToDouble(p -> Double.parseDouble(p.get(field)))
                        .max()
                        .orElse(Double.NaN);
                    break;

                case "avg":
                    aggValue = publicationList
                        .stream()
                        .mapToDouble(p -> Double.parseDouble(p.get(field)))
                        .average()
                        .orElse(Double.NaN);
                    break;
            }

            if (!Operation.compare(String.valueOf(aggValue), condition.operation, condition.value.toString())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Sends a publication to the next ComplexFilterBolt
        declarer.declareStream("forward-publications", new Fields("publication-list", "hops"));

        // Sends meta-publication and subscription data to a ClientBolt for notifying
        declarer.declareStream("notify", new Fields("meta-publication", "subscription"));
    }
    
}
