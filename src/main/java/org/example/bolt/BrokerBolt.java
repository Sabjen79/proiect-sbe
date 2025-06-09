package org.example.bolt;

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

public class BrokerBolt extends BaseWindowedBolt {

    private OutputCollector collector;
    private final String cityFilter = "Bucharest";
    private final double avgTempThreshold = 8.5;
    private final double avgWindThreshold = 13.0;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        this.collector = collector;
        System.out.println("BrokerBolt initialized.");
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuples = inputWindow.get();
        double totalTemp = 0;
        double totalWind = 0;
        int count = 0;

        try {
            for (Tuple tuple : tuples) {

                Object pubObj = tuple.contains("publication") ? tuple.getValueByField("publication")
                        : tuple.contains("meta-publication") ? tuple.getValueByField("meta-publication")
                        : null;

                if (!(pubObj instanceof Map)) {
                    System.err.println("Invalid tuple content: " + pubObj);
                    continue;
                }

                Map<String, Object> pub = (Map<String, Object>) pubObj;
                if (!pub.containsKey("city") || !pub.containsKey("temp") || !pub.containsKey("wind")) {
                    System.err.println("Incomplete publication: " + pub);
                    continue;
                }

                String city = String.valueOf(pub.get("city"));
                double temp = Double.parseDouble(pub.get("temp").toString());
                double wind = Double.parseDouble(pub.get("wind").toString());

                if (city.equals(cityFilter)) {
                    totalTemp += temp;
                    totalWind += wind;
                    count++;
                }
            }

            if (count > 0) {
                double avgTemp = totalTemp / count;
                double avgWind = totalWind / count;

                System.out.printf("AvgTemp: %.2f, AvgWind: %.2f%n", avgTemp, avgWind);

                if (avgTemp > avgTempThreshold && avgWind <= avgWindThreshold) {
                    Map<String, Object> metaPub = Map.of(
                            "city", cityFilter,
                            "conditions", true,
                            "avg_temp", avgTemp,
                            "avg_wind", avgWind
                    );
                    collector.emit(new Values(metaPub));
                }
            }
        } catch (Exception e) {
            System.err.println("Fatal BrokerBolt error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            for (Tuple tuple : tuples) {
                try {
                    collector.ack(tuple);
                } catch (Exception ackEx) {
                    System.err.println("Failed to ack tuple: " + ackEx.getMessage());
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("meta-publication"));
    }
}
