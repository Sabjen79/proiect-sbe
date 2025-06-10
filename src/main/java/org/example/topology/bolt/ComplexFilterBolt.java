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
        subscriptionsMap.putIfAbsent(input.getStringByField("city"), new ArrayList<>());
        var subscriptions = subscriptionsMap.get(input.getStringByField("city"));

        if(input.contains("subscription")) {
            var sub = (Subscription) input.getValueByField("subscription");
            
            subscriptions.add(sub);
        }

        if(input.contains("publication-list")) {
            var publicationList = (List<Map<String, String>>) input.getValueByField("publication-list");

            for(var sub : subscriptions) {
                var isMatch = true;

                for(var condition : sub.conditions) {
                    // Skip checking on city since it is handled by the topology
                    if(condition.key.equals(WeatherDataValues.fields[1])) { 
                        continue;
                    }

                    var splitKey = condition.key.split("_");
                    var key = splitKey[1];
                    var publicationValue = "0.0";
                    
                    switch (splitKey[0]) {
                        case "min":
                            publicationValue = publicationList
                                .stream()
                                .map((item) -> Double.parseDouble(item.get(key)))
                                .reduce((total, element) -> Math.min(total, element))
                                .get()
                                .toString();
                            break;

                        case "max":
                            publicationValue = publicationList  
                                .stream()
                                .map((item) -> Double.parseDouble(item.get(key)))
                                .reduce((total, element) -> Math.max(total, element))
                                .get()
                                .toString();
                            break;

                        case "avg":
                            Double value = publicationList  
                                .stream()
                                .map((item) -> Double.parseDouble(item.get(key)))
                                .reduce((total, element) -> total + element)
                                .get() / publicationList.size();

                            publicationValue = value.toString();
                            break;
                    }
                    
                    if(!Operation.compare(publicationValue, condition.operation, condition.value)) {
                        isMatch = false;
                        break;
                    }
                }

                if(isMatch) {
                    var metaPub = "{city = " + input.getStringByField("city") + ", conditions = true}";
                    collector.emit(new Values(sub, metaPub));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subscription", "meta-publication"));
    }
    
}
