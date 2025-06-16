package org.example.subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.example.FileLogger;
import org.example.data.WeatherDataValues;
import org.example.data.encryption.SimpleOPE;
import org.example.util.RandomUtil;

public class Subscriber {
    private final static Random rand = new Random();
    private static BlockingQueue<Subscription> queue = new LinkedBlockingDeque<>();

    private static final int TOTAL_SUBSCRIPTIONS = 2500;

    protected Subscriber() {}

    public void startGenerator() {
        var thread = new Thread(() -> {
            for (int k = 0; k < TOTAL_SUBSCRIPTIONS; k++) {
                List<SubCondition> conditions = new ArrayList<>();
                    
                conditions.add(new SubCondition(
                    WeatherDataValues.fields[1],
                    Operation.EQUAL,
                    SimpleOPE.encryptString(RandomUtil.randomFrom(WeatherDataValues.cities)),
                    0
                ));

                boolean isComplex = (rand.nextDouble() < 0);

                addCondition(conditions, isComplex, 2); // Add temp condition
                for(int j = 0; j < rand.nextInt(0, 2); j++) {
                    addRandomCondition(conditions, isComplex);
                }
                
                Subscription sub = new Subscription(RandomUtil.randomString(8), conditions);

                queue.add(sub);

                try {
                    Thread.sleep(180_000 / TOTAL_SUBSCRIPTIONS);
                } catch (InterruptedException e) {
                    FileLogger.error(e.toString());
                    break;
                }
            }

            FileLogger.info("Subscriber Thread finished generating");
        });

        thread.setDaemon(true);
        thread.start();
    }

    public Subscription pollData() {
        return queue.poll();
    }

    private void addRandomCondition(List<SubCondition> list, boolean isComplex) {
        var index = rand.nextInt(2, 5);
        
        addCondition(list, isComplex, index);
    }

    private void addCondition(List<SubCondition> list, boolean isComplex, int index) {
        Object value = 0;
        int type = 0;
        Operation operation;

        switch (index) {
            case 2: // Temperature
                value = SimpleOPE.encryptLong(rand.nextInt(0, 30));
                operation = Operation.randomEqual(0.25);
                type = 1;
                break;

            case 3: // Rain Chance
                value = SimpleOPE.encryptDouble(0.25 + rand.nextDouble() / 2.0);
                operation = RandomUtil.randomFrom(Operation.values());
                type = 2;
                break;

            case 4: // Wind Speed
                value = SimpleOPE.encryptLong(rand.nextInt(5, 15));
                operation = RandomUtil.randomFrom(Operation.values());
                type = 1;
                break;
            default:
                return;
        }

        var prefix = "";

        if(isComplex) {
            prefix = RandomUtil.randomFrom(WeatherDataValues.complexPrefixes);
        }

        for(var cond : list) {
            if(cond.key == prefix + WeatherDataValues.fields[index]) {
                return; // Don't add existing conditions
            }
        }

        list.add(
            new SubCondition(
                prefix + WeatherDataValues.fields[index],
                operation,
                String.valueOf(value),
                type
            )
        );
    }
}
