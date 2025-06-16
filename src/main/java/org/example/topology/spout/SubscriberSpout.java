package org.example.topology.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.example.data.WeatherDataValues;
import org.example.subscriber.SubscriberNodes;
import org.example.util.StatsTracker;
import org.example.util.TopologyStatus;

import java.util.*;

public class SubscriberSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String subscriberName;

    /**
     * Spout that receives subscriptions from a specific Subscriber from SubscriberNodes.
     */
    public SubscriberSpout(String subString) {
        this.subscriberName = subString;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        TopologyStatus.READY.set(true);
    }

    @Override
    public void nextTuple() {
        var data = SubscriberNodes.getSubscriber(subscriberName).pollData();

        if(data != null) {
            StatsTracker.subscriptionNum.incrementAndGet();

            var city = "";

            for (var condition : data.conditions) {
                if (condition.key.equals(WeatherDataValues.fields[1])) {
                    city = condition.value.toString();
                }
            }

            collector.emit(new Values(data, city));
        }

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subscription", "city"));
    }
}
