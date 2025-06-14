package org.example.topology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.example.FileLogger;
import org.example.subscriber.SubscriberNodes;
import org.example.subscriber.Subscription;
import org.apache.storm.topology.base.BaseRichBolt;
import org.example.util.LatencyTracker;

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

            // latency tracking
            // Uncomment the following lines if you want to track latency
            try {
                long publishTime = Long.parseLong(publication.get("date"));
                long deliveryTime = System.currentTimeMillis();
                long latency = deliveryTime - publishTime;

                // log the latency
                FileLogger.info("[ClientBolt] Publication latency (ms): " + latency);

                // add latency to the tracker
                LatencyTracker.addLatency(latency);

            } catch (Exception e) {
                // if parsing the date fails, log the error
                FileLogger.info("[ClientBolt] Error parsing publication date: " + e.getMessage());
            }

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
