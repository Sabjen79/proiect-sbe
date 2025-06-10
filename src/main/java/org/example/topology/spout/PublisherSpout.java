package org.example.topology.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.example.FileLogger;
import org.example.data.WeatherDataValues;
import org.example.data.WeatherDataOuterClass.WeatherData;
import org.example.publisher.PublisherNodes;

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.*;

public class PublisherSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String publisherName;

    public PublisherSpout(String publString) {
        this.publisherName = publString;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        var byteData = PublisherNodes.getPublisher(publisherName).pollData();

        if(byteData != null) {
            WeatherData weatherData = null;

            try {
                weatherData = WeatherData.parseFrom(byteData);
            } catch (InvalidProtocolBufferException e) {
                FileLogger.error(e.toString());
            }

            Map<String, String> publication = new HashMap<>();

            publication.put(WeatherDataValues.fields[0], weatherData.getStationId());
            publication.put(WeatherDataValues.fields[1], weatherData.getCity());
            publication.put(WeatherDataValues.fields[2], String.valueOf(weatherData.getTemperature()));
            publication.put(WeatherDataValues.fields[3], String.valueOf(weatherData.getRainChance()));
            publication.put(WeatherDataValues.fields[4], String.valueOf(weatherData.getWindSpeed()));
            publication.put(WeatherDataValues.fields[5], String.valueOf(weatherData.getWeatherDirection()));
            publication.put(WeatherDataValues.fields[6], String.valueOf(weatherData.getDate()));

            collector.emit(new Values(publication, weatherData.getCity()));
        }

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication", "city"));
    }
}
