package org.example.topology.bolt;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

/**
 * Bolt that groups publications in a TupleWindow, forwarding them in a single tuple
 */
public class PublicationWindowedBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuples = inputWindow.get();
        
        List<Map<String, String>> list = tuples
            .stream()
            .map((item) -> {
                return (Map<String, String>) item.getValueByField("publication");
            })
            .toList();

        collector.emit(new Values(list));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication-list"));
    }
}

