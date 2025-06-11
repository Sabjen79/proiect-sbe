package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.example.data.WeatherDataOuterClass.WeatherData;
import org.example.data.WeatherDataValues;
import org.example.publisher.PublisherNodes;
import org.example.subscriber.SubscriberNodes;
import org.example.topology.bolt.*;
import org.example.topology.spout.PublisherSpout;
import org.example.topology.spout.SubscriberSpout;

import java.util.HashMap;
import java.util.Map;

public class App {
    private final static String TOPOLOGY_ID = "pubsub-topology";

    public static void main(String[] args) throws Exception {
        FileLogger.LOG_LEVEL = 2;

        // define number of publishers and subscribers
        var PUBLISHER_NUM = 3;
        var SUBSCRIBER_NUM = 3;

        // initialize publishers and subscribers
        PublisherNodes.initialize(PUBLISHER_NUM);
        SubscriberNodes.initialize(SUBSCRIBER_NUM);

        // initialize topology builder and simple filter bolt map
        TopologyBuilder builder = new TopologyBuilder();
        Map<String, org.apache.storm.topology.BoltDeclarer> simpleFilterBolts = new HashMap<>();

        // create windowed bolt for aggregated publication processing
        var publicationWindowedBolt = builder.setBolt(
                "PublicationWindowedBolt",
                new PublicationWindowedBolt().withTumblingWindow(new BaseWindowedBolt.Count(10)),
                WeatherDataValues.cities.length
        );

        // create spouts and bolts for each broker node
        for (int i = 1; i <= PUBLISHER_NUM; i++) {
            String publisherSpoutId = "PublisherSpout" + i;
            String subscriberSpoutId = "SubscriberSpout" + i;
            String simpleFilterBoltId = "SimpleFilterBolt" + i;
            String complexFilterBoltId = "ComplexFilterBolt" + i;

            // add publisher and subscriber spouts
            builder.setSpout(publisherSpoutId, new PublisherSpout("Publisher" + i));
            builder.setSpout(subscriberSpoutId, new SubscriberSpout("Subscriber" + i));

            // set up simple filter bolt with input from publisher and subscriber
            var simpleBolt = builder.setBolt(simpleFilterBoltId, new SimpleFilterBolt(), 1)
                    .fieldsGrouping(publisherSpoutId, new Fields("city"))
                    .fieldsGrouping(subscriberSpoutId, new Fields("city"));

            // store bolt reference for later connection
            simpleFilterBolts.put(simpleFilterBoltId, simpleBolt);

            // connect publisher to windowed publication bolt
            publicationWindowedBolt.fieldsGrouping(publisherSpoutId, new Fields("city"));

            // create complex filter bolt with input from simple filter and windowed publications
            builder.setBolt(complexFilterBoltId, new ComplexFilterBolt(), WeatherDataValues.cities.length)
                    .fieldsGrouping(simpleFilterBoltId, "forward", new Fields("city"))
                    .fieldsGrouping("PublicationWindowedBolt", new Fields("city"));
        }

        // enable advanced publication routing between all simple filter bolts
        for (int i = 1; i <= PUBLISHER_NUM; i++) {
            for (int j = 1; j <= PUBLISHER_NUM; j++) {
                if (i != j) {
                    String targetBoltId = "SimpleFilterBolt" + i;
                    String sourceBoltId = "SimpleFilterBolt" + j;
                    simpleFilterBolts.get(targetBoltId)
                            .shuffleGrouping(sourceBoltId, "forward-publication");
                }
            }
        }

        // create client bolt and wire up notify and complex filter output
        var clientBolt = builder.setBolt("ClientBolt", new ClientBolt(), WeatherDataValues.cities.length);
        for (int i = 1; i <= PUBLISHER_NUM; i++) {
            clientBolt.shuffleGrouping("SimpleFilterBolt" + i, "notify");
        }
        for (int i = 1; i <= SUBSCRIBER_NUM; i++) {
            clientBolt.shuffleGrouping("ComplexFilterBolt" + i);
        }

        // configure topology
        Config config = new Config();
        config.setDebug(true);
        config.registerSerialization(WeatherData.class);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
        config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 1);

        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();
        cluster.submitTopology(TOPOLOGY_ID, config, topology);

        FileLogger.info("Created broker topology");
        PublisherNodes.startGenerating();
        SubscriberNodes.startGenerating();

        Thread.sleep(20000);

        // shutdown everything
        cluster.killTopology(TOPOLOGY_ID);
        cluster.shutdown();
        cluster.close();

        System.exit(0);
    }
}
