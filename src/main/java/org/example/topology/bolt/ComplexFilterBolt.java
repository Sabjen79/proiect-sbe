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
        try {
            String stream = input.getSourceStreamId();
            String city = input.getStringByField("city");
            subscriptionsMap.putIfAbsent(city, new ArrayList<>());
            var subscriptions = subscriptionsMap.get(city);

            FileLogger.debug("[ComplexFilterBolt] Received on stream: " + stream + " | city=" + city);

            if (input.contains("subscription")) {
                var sub = (Subscription) input.getValueByField("subscription");
                subscriptions.add(sub);
                FileLogger.debug("Subscription added: " + sub + " | city=" + city);
                return;
            }

            if (input.contains("publication-list")) {
                var publicationList = (List<Map<String, String>>) input.getValueByField("publication-list");

                for (var sub : subscriptions) {
                    boolean isMatch = true;

                    for (var condition : sub.conditions) {
                        if (condition.key.equals(WeatherDataValues.fields[1])) {
                            continue; // skip 'city'
                        }

                        var splitKey = condition.key.split("_");
                        if (splitKey.length < 2) continue;

                        var agg = splitKey[0];
                        var field = splitKey[1];

                        double aggValue = 0;
                        switch (agg) {
                            case "min":
                                aggValue = publicationList.stream()
                                        .mapToDouble(p -> Double.parseDouble(p.get(field)))
                                        .min().orElse(Double.NaN);
                                break;
                            case "max":
                                aggValue = publicationList.stream()
                                        .mapToDouble(p -> Double.parseDouble(p.get(field)))
                                        .max().orElse(Double.NaN);
                                break;
                            case "avg":
                                aggValue = publicationList.stream()
                                        .mapToDouble(p -> Double.parseDouble(p.get(field)))
                                        .average().orElse(Double.NaN);
                                break;
                            default:
                                isMatch = false;
                        }

                        if (!Operation.compare(String.valueOf(aggValue), condition.operation, condition.value)) {
                            isMatch = false;
                            break;
                        }
                    }

                    if (isMatch) {
                        var metaPub = "{city = " + city + ", conditions = true}";
                        collector.emit(new Values(sub, metaPub));
                        FileLogger.debug("Matched publication list to subscription: " + sub + " | metaPub=" + metaPub);
                    }
                }
            }

        } catch (Exception e) {
            FileLogger.error("[ComplexFilterBolt] Exception caught in execute(): " + e.getMessage());
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subscription", "meta-publication"));
        declarer.declareStream("forward-publication", new Fields("publication", "hops"));
    }
    
}
