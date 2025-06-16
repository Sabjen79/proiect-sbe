package org.example.topology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.example.data.WeatherDataValues;
import org.example.subscriber.SubscriberNodes;
import org.example.subscriber.Subscription;
import org.example.util.StatsTracker;
import org.apache.storm.topology.base.BaseRichBolt;

import java.util.Map;

/**
 * Bolt responsible for notifying - sending data - to the SubscriberNodes
 */
public class ClientBolt extends BaseRichBolt {
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {
        if(input.contains("publication")) {
            var publication = (Map<String, String>) input.getValueByField("publication");
            var sub = (Subscription) input.getValueByField("subscription");

            StatsTracker.latencyCount.incrementAndGet();
            StatsTracker.latencyValue.addAndGet(System.currentTimeMillis() - Long.parseLong(publication.get("date")));

            StatsTracker.matchedPubSet.add(publication.get(WeatherDataValues.fields[0]));

            SubscriberNodes.notifySubscriberPublication(sub, publication);
        }

        if(input.contains("meta-publication")) {
            var metaPublication = input.getValueByField("meta-publication").toString();
            var sub = (Subscription) input.getValueByField("subscription");

            SubscriberNodes.notifySubscriberString(sub, metaPublication);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
