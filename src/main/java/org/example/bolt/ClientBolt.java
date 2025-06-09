package org.example.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class ClientBolt implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            if (input.contains("meta-publication")) {
                Map<String, Object> pub = (Map<String, Object>) input.getValueByField("meta-publication");
                System.out.println("Client received: " + pub);
            } else {
                System.err.println("Client received unexpected tuple: " + input.getFields());
            }
        } catch (Exception e) {
            System.err.println("ClientBolt exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
