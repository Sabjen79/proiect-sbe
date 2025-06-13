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

public class App {
    private final static String TOPOLOGY_ID = "pubsub-topology";
    public final static int BROKER_NUM = 3;
    public final static int PARRARELISM = WeatherDataValues.cities.length;
    public final static Fields CITY_FIELDS = new Fields("city");

    public static void main(String[] args) throws Exception {
        FileLogger.LOG_LEVEL = 2;

        PublisherNodes.initialize(BROKER_NUM);
        SubscriberNodes.initialize(BROKER_NUM);

        TopologyBuilder builder = new TopologyBuilder();

        var pubWinBolt = builder.setBolt(
            "PublicationWindowedBolt",
            new PublicationWindowedBolt().withTumblingWindow(new BaseWindowedBolt.Count(10)),
            PARRARELISM
        );

        for(int i = 0; i < BROKER_NUM; i++) {
            builder.setSpout("PublisherSpout" + i, new PublisherSpout("Publisher" + i));
            builder.setSpout("SubscriberSpout" + i, new SubscriberSpout("Subscriber" + i));

            pubWinBolt.fieldsGrouping("PublisherSpout" + i, CITY_FIELDS);

            builder.setBolt("SimpleFilterBolt" + i, new SimpleFilterBolt(), PARRARELISM)
                .fieldsGrouping("PublisherSpout" + i, CITY_FIELDS)
                .fieldsGrouping("SubscriberSpout" + i, CITY_FIELDS)
                .fieldsGrouping("SimpleFilterBolt" + ((i + 1) % BROKER_NUM), "forward-publication", CITY_FIELDS);

            builder.setBolt("ComplexFilterBolt" + i, new ComplexFilterBolt(), PARRARELISM)
                .fieldsGrouping("SimpleFilterBolt" + i, "forward-subscription", CITY_FIELDS)
                .shuffleGrouping("PublicationWindowedBolt")
                .shuffleGrouping("ComplexFilterBolt" + ((i + 1) % BROKER_NUM), "forward-publications");

            builder.setBolt("ClientBolt" + i, new ClientBolt(), PARRARELISM)
                .shuffleGrouping("SimpleFilterBolt" + i, "notify")
                .shuffleGrouping("ComplexFilterBolt" + i, "notify");
        }

        // configure topology
        Config config = new Config();
        config.setDebug(false);
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

        cluster.killTopology(TOPOLOGY_ID);
        cluster.shutdown();
        cluster.close();

        System.exit(0);
    }
}
