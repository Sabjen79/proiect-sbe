package org.example.topology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.example.subscriber.SubscriberNodes;
import org.example.subscriber.Subscription;
import org.apache.storm.topology.base.BaseRichBolt;

import java.util.Map;

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

            SubscriberNodes.notifySubscriber(sub, publication);
        }

        if(input.contains("meta-publication")) {
            var metaPublication = input.getValueByField("meta-publication");
            var sub = (Subscription) input.getValueByField("subscription");

            SubscriberNodes.notifySubscriber(sub, metaPublication);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
