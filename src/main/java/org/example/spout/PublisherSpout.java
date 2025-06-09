package org.example.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.example.data.DataGenerator;

import java.util.*;

public class PublisherSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        DataGenerator.startGenerator();
    }

    @Override
    public void nextTuple() {
        var weatherData = DataGenerator.pollData();

        if(weatherData != null) {
            collector.emit(new Values(weatherData.toByteArray()));
        }

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication"));
    }
}
