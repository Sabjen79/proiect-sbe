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
import org.example.data.WeatherDataValues;
import org.example.subscriber.Operation;
import org.example.subscriber.Subscription;

public class SimpleFilterBolt extends BaseRichBolt {
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
        subscriptionsMap.putIfAbsent(input.getStringByField("city"), new ArrayList<>());
        var subscriptions = subscriptionsMap.get(input.getStringByField("city"));

        if(input.contains("subscription")) {
            var sub = (Subscription) input.getValueByField("subscription");

            for(var condition : sub.conditions) {
                for(var prefix : WeatherDataValues.complexPrefixes) {
                    if(condition.key.startsWith(prefix)) {
                        collector.emit("forward", new Values(sub, input.getStringByField("city")));
                        
                        return;
                    }
                }
            }
            
            subscriptions.add(sub);
        }

        if(input.contains("publication")) {
            var publication = (Map<String, String>) input.getValueByField("publication");

            for(var sub : subscriptions) {
                var isMatch = true;

                for(var condition : sub.conditions) {
                    // Skip checking on city since it is handled by the topology
                    if(condition.key.equals(WeatherDataValues.fields[1])) { 
                        continue;
                    } 
                    
                    if(!Operation.compare(publication.get(condition.key), condition.operation, condition.value)) {
                        isMatch = false;
                        break;
                    }
                }

                if(isMatch) {
                    collector.emit("notify", new Values(sub, publication));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Forwards the tuple to the complex filter
        declarer.declareStream("forward", new Fields("subscription", "city"));

        // Notifies for the matched subscription
        declarer.declareStream("notify", new Fields("subscription", "publication"));
    }
    
}
