package org.example.topology.bolt;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.example.data.WeatherDataValues;

import java.util.List;
import java.util.Map;

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

        collector.emit(new Values(list, list.getFirst().get(WeatherDataValues.fields[1])));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication-list", "city"));
    }
}

