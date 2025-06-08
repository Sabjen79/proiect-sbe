//package org.example;
//
//import org.apache.storm.spout.SpoutOutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseRichSpout;
//import org.apache.storm.tuple.Fields;
//import org.apache.storm.tuple.Values;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Random;
//
///**
// * posibilitatea de fixare a: numarului total de mesaje (publicatii, respectiv subscriptii), ponderii pe frecventa campurilor din subscriptii si ponderii operatorilor de egalitate din subscriptii pentru cel putin un camp
// *
// */
//
//public class SubscriptionSpout extends BaseRichSpout {
//
//    private SpoutOutputCollector collector;
//    private Random random = new Random();
//
//    private List<String> cities = List.of("Bucharest", "Cluj", "Iasi", "Timisoara");
//    private List<String> directions = List.of("N", "NE", "E", "SE", "S", "SW", "W", "NW");
//    private List<String> operators = List.of("=", "!=", "<", "<=", ">", ">=");
//
//    private Map<String, Double> fieldFrequencies;
//    private Map<String, Double> equalityFrequencies;
//
//    public SubscriptionSpout(Map<String, Double> fieldFrequencies, Map<String, Double> equalityFrequencies) {
//        this.fieldFrequencies = fieldFrequencies;
//        this.equalityFrequencies = equalityFrequencies;
//    }
//
//    /**
//     * Unele campuri pot lipsi; frecventa campurilor prezente trebuie sa fie configurabila
//     * @param map
//     * @param topologyContext
//     * @param spoutOutputCollector
//     */
//    @Override
//    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
//        this.collector = spoutOutputCollector;
//    }
//
//
//
//    @Override
//    public void nextTuple() {
//        Map<String, Map.Entry<String, Object>> subscription = new HashMap<>();
//
//        if (Math.random() < fieldFrequencies.getOrDefault("city", 0.0)) {
//            String op = chooseOperator("city");
//            String val = cities.get(random.nextInt(cities.size()));
//            subscription.put("city", Map.entry(op, val));
//        }
//
//        if (Math.random() < fieldFrequencies.getOrDefault("temp", 0.0)) {
//            String op = chooseOperator("temp");
//            int val = random.nextInt(41);
//            subscription.put("temp", Map.entry(op, val));
//        }
//
//        // repeat for other fields...
//
//        collector.emit(new Values(subscription));
//
//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//    }
//
//    private String chooseOperator(String field) {
//        double eqFreq = equalityFrequencies.getOrDefault(field, 0.5);
//        return random.nextDouble() < eqFreq ? "=" : operators.get(random.nextInt(operators.size()));
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("subscription"));
//    }
//}
