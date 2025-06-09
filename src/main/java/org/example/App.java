package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.example.bolt.DebugBolt;
import org.example.spout.PublisherSpout;

public class App {
    private final static String SPOUT_ID = PublisherSpout.class.getName();
    private final static String BOLT_ID = DebugBolt.class.getName();
    private final static String TOPOLOGY_ID = "pubsub-topology";

    public static void main( String[] args ) throws Exception {
        FileLogger.LOG_LEVEL = 2;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, new PublisherSpout());
        builder.setBolt(BOLT_ID, new DebugBolt()).shuffleGrouping(SPOUT_ID);

        Config config = new Config();

        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();

        cluster.submitTopology(TOPOLOGY_ID, config, topology);
        
        Thread.sleep(10000);

        cluster.killTopology(TOPOLOGY_ID);
        cluster.close();
        cluster.shutdown();

        System.exit(0);
    }
}
