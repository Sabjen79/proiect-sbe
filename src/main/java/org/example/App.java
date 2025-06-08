package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class App {
    public static void main( String[] args ) throws Exception {
        System.out.println( "Hello World!" );
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("publisher", new PublisherSpout());
        builder.setBolt("debug", new DebugBolt()).shuffleGrouping("publisher");

        Config config = new Config();
        config.setDebug(false);

        // creează cluster local pentru testare
        LocalCluster cluster = new LocalCluster();

        // pornește topologia
        cluster.submitTopology("pubsub-topology", config, builder.createTopology());

        // rulează 10 secunde (sau cât vrei)
        Thread.sleep(10000);

        // oprește clusterul local
        cluster.shutdown();

    }
}
