package org.example.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.example.FileLogger;
import org.example.data.WeatherDataOuterClass.WeatherData;

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Map;

public class DebugBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        var input = (byte[]) tuple.getValueByField("publication");

        try {
            var data = WeatherData.parseFrom(input);

            FileLogger.debug("Publication: " + data.toString());
        } catch (InvalidProtocolBufferException e) {
            FileLogger.error(e.toString());
        }

        collector.emit(tuple.getValues()); // Re-emit as-is
        collector.ack(tuple); // Acknowledge
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication"));
    }
}

