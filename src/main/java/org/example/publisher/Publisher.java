package org.example.publisher;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.example.FileLogger;
import org.example.data.WeatherDataValues;
import org.example.data.WeatherDataOuterClass.WeatherData;
import org.example.data.WeatherDataOuterClass.WeatherData.WeatherDirection;
import org.example.data.encryption.SimpleOPE;
import org.example.util.RandomUtil;

public class Publisher {
    private final static Random rand = new Random();
    private static BlockingQueue<byte[]> queue = new LinkedBlockingDeque<>();

    protected Publisher() {}

    public void startGenerator() {
        var thread = new Thread(() -> {
            
            while (true) {
                var data = WeatherData
                    .newBuilder()
                    .setStationId(
                        SimpleOPE.encryptString(UUID.randomUUID().toString())
                    )
                    .setCity(
                        SimpleOPE.encryptString(RandomUtil.randomFrom(WeatherDataValues.cities))
                    )
                    .setWeatherDirection(
                        SimpleOPE.encryptLong(WeatherDirection.forNumber(rand.nextInt(8)).getNumber())
                    )
                    .setTemperature(
                        SimpleOPE.encryptLong(rand.nextInt(-10, 41))
                    )
                    .setRainChance(
                        SimpleOPE.encryptDouble(rand.nextDouble())
                    )
                    .setWindSpeed(
                        SimpleOPE.encryptLong(rand.nextInt(21))
                    )
                    .setDate(System.currentTimeMillis())
                    .build();

                queue.add(data.toByteArray());

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    FileLogger.error(e.toString());
                    break;
                }
            }
        });

        thread.setDaemon(true);
        thread.start();
    }

    public byte[] pollData() {
        return queue.poll();
    }
}
