package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;
import org.example.data.WeatherDataValues;
import org.example.data.WeatherDataOuterClass.WeatherData;
import org.example.publisher.PublisherNodes;
import org.example.subscriber.SubscriberNodes;
import org.example.topology.bolt.ClientBolt;
import org.example.topology.bolt.ComplexFilterBolt;
import org.example.topology.bolt.PublicationWindowedBolt;
import org.example.topology.bolt.SimpleFilterBolt;
import org.example.topology.spout.PublisherSpout;
import org.example.topology.spout.SubscriberSpout;

public class App {
    private final static String TOPOLOGY_ID = "pubsub-topology";

    public static void main(String[] args) throws Exception {
        FileLogger.LOG_LEVEL = 2;

        var PUBLISHER_NUM = 3;
        var SUBSCRIBER_NUM = 3;

        PublisherNodes.initialize(PUBLISHER_NUM);
        SubscriberNodes.initialize(SUBSCRIBER_NUM);

        TopologyBuilder builder = new TopologyBuilder();

        var simpleFilterBolt = builder.setBolt("SimpleFilterBolt", new SimpleFilterBolt(), WeatherDataValues.cities.length);

        var publicationWindowedBolt = builder.setBolt(
            "PublicationWindowedBolt", 
            new PublicationWindowedBolt().withTumblingWindow(new Count(10)), 
            WeatherDataValues.cities.length
        );

        for (int i = 1; i <= PUBLISHER_NUM; i++) {
            builder.setSpout("PublisherSpout" + i, new PublisherSpout("Publisher" + i));

            simpleFilterBolt.fieldsGrouping("PublisherSpout" + i, new Fields("city"));
            publicationWindowedBolt.fieldsGrouping("PublisherSpout" + i, new Fields("city"));
        }

        for (int i = 1; i <= SUBSCRIBER_NUM; i++) {
            builder.setSpout("SubscriberSpout" + i, new SubscriberSpout("Subscriber" + i));

            simpleFilterBolt.fieldsGrouping("SubscriberSpout" + i, new Fields("city"));
        }
        
        builder.setBolt("ComplexFilterBolt", new ComplexFilterBolt(), WeatherDataValues.cities.length)
            .fieldsGrouping("SimpleFilterBolt", "forward", new Fields("city"))
            .fieldsGrouping("PublicationWindowedBolt", new Fields("city"));
        
        builder.setBolt("ClientBolt", new ClientBolt(), WeatherDataValues.cities.length)
            .shuffleGrouping("SimpleFilterBolt", "notify")
            .shuffleGrouping("ComplexFilterBolt");

        Config config = new Config();
        config.setDebug(true);

        config.registerSerialization(WeatherData.class);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,1024);
    	config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE,1);

        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();
        cluster.submitTopology(TOPOLOGY_ID, config, topology);

        FileLogger.info("Created broker topology");

        PublisherNodes.startGenerating();
        SubscriberNodes.startGenerating();
        
        Thread.sleep(20000);

        cluster.killTopology(TOPOLOGY_ID);        
        cluster.shutdown();
        cluster.close();

        System.exit(0);
    }
}
