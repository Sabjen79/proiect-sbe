package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.example.bolt.BrokerBolt;
import org.example.bolt.ClientBolt;
import org.example.bolt.DebugBolt;
import org.example.data.WeatherDataOuterClass.WeatherData;
import org.example.spout.PublisherSpout;

import java.util.concurrent.TimeUnit;

public class App {
    private final static String SPOUT_ID = PublisherSpout.class.getName();
    private final static String BOLT_ID = DebugBolt.class.getName();
    private final static String TOPOLOGY_ID = "pubsub-topology";

    public static void main(String[] args) throws Exception {
        FileLogger.LOG_LEVEL = 2;
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, new PublisherSpout());
        builder.setBolt(BOLT_ID, new DebugBolt()).shuffleGrouping(SPOUT_ID);

        builder.setSpout("PublisherSpout", new PublisherSpout());
        builder.setBolt("BrokerBolt1", new BrokerBolt().withWindow(BaseWindowedBolt.Duration.of(5000))).shuffleGrouping("PublisherSpout");
        builder.setBolt("BrokerBolt2", new BrokerBolt().withWindow(BaseWindowedBolt.Duration.of(5000))).shuffleGrouping("BrokerBolt1");
        builder.setBolt("ClientBolt", new ClientBolt()).shuffleGrouping("BrokerBolt2");

        Config config = new Config();
        config.setDebug(true);

        config.registerSerialization(WeatherData.class);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,1024);
    	config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE,1);

        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();
        cluster.submitTopology(TOPOLOGY_ID, config, topology);
        
        Thread.sleep(20000);

        cluster.killTopology(TOPOLOGY_ID);        
        cluster.shutdown();
        cluster.close();

        System.exit(0);
    }
}
