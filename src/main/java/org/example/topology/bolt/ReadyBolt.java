package org.example.topology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.example.FileLogger;
import org.example.util.TopologyStatus;

import java.util.Map;

public class ReadyBolt extends BaseRichBolt {
    private boolean signaled = false;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {}

    @Override
    public void execute(Tuple input) {
        if (!signaled) {
            TopologyStatus.READY.set(true);
            signaled = true;
            FileLogger.info("TOPOLOGY IS NOW READY");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
