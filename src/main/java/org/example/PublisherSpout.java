package org.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PublisherSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Random random;

    private final List<String> cities = List.of("Bucharest", "Cluj", "Iasi", "Timisoara");
    private final List<String> directions = List.of("N", "NE", "E", "SE", "S", "SW", "W", "NW");

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        Map<String, Object> publication = new HashMap<>();

        publication.put("stationid", random.nextInt(10) + 1);
        publication.put("city", cities.get(random.nextInt(cities.size())));
        publication.put("temp", random.nextInt(41)); // 0..40
        publication.put("rain", Math.round(random.nextDouble() * 100.0) / 100.0); // max 1.00
        publication.put("wind", random.nextInt(31)); // 0..30
        publication.put("direction", directions.get(random.nextInt(directions.size())));

        LocalDate date = LocalDate.now();
        publication.put("date", date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

        collector.emit(new Values(publication));

        // Întârziere opțională pentru a nu inunda sistemul
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication"));
    }
}
