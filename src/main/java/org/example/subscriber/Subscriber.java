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

    protected Subscriber() {}

    public void startGenerator() {
        var thread = new Thread(() -> {
            while (true) {
                int count = rand.nextInt(4);

                for(int i = 0; i < count; i++) {
                    List<SubCondition> conditions = new ArrayList<>();
                    
                    conditions.add(new SubCondition(
                        WeatherDataValues.fields[1],
                        Operation.EQUAL,
                        SimpleOPE.encryptString(RandomUtil.randomFrom(WeatherDataValues.cities)),
                        0
                    ));

                    boolean isComplex = (rand.nextDouble() < 0.25);

                    for(int j = 0; j < rand.nextInt(1, 3); j++) {
                        addRandomCondition(conditions, isComplex);
                    }
                    
                    Subscription sub = new Subscription(RandomUtil.randomString(8), conditions);

                    queue.add(sub);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    FileLogger.error(e.toString());
                    break;
                }
            }
        });

        thread.setDaemon(true);
        thread.start();
    }

    public Subscription pollData() {
        return queue.poll();
    }

    private void addRandomCondition(List<SubCondition> list, boolean isComplex) {
        var index = rand.nextInt(2, 5);
        Object value = 0;
        int type = 0;

        switch (index) {
            case 2: // Temperature
                value = SimpleOPE.encryptLong(rand.nextInt(5, 25));
                type = 1;
                break;

            case 3: // Rain Chance
                value = SimpleOPE.encryptDouble(0.25 + rand.nextDouble() / 2.0);
                type = 2;
                break;

            case 4: // Wind Speed
                value = SimpleOPE.encryptLong(rand.nextInt(5, 15));
                type = 1;
                break;
        }

        var prefix = "";

        if(isComplex) {
            prefix = RandomUtil.randomFrom(WeatherDataValues.complexPrefixes);
        }

        list.add(
            new SubCondition(
                prefix + WeatherDataValues.fields[index],
                RandomUtil.randomFrom(Operation.valuesNoEqual()),
                String.valueOf(value),
                type
            )
        );
    }
}
