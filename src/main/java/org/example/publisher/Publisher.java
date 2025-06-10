package org.example.publisher;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.example.FileLogger;
import org.example.data.WeatherDataOuterClass.WeatherData;
import org.example.data.WeatherDataOuterClass.WeatherData.WeatherDirection;
import org.example.util.RandomUtil;

public class Publisher {
    private static final List<String> cities = List.of("Bucharest", "Cluj", "Iasi", "Timisoara");
    private final static Random rand = new Random();
    private static BlockingQueue<byte[]> queue = new LinkedBlockingDeque<>();

    protected Publisher() {}

    public void startGenerator() {
        var thread = new Thread(() -> {
            while (true) {
                int count = rand.nextInt(4);

                for(int i = 0; i < count; i++) {
                    var data = WeatherData
                        .newBuilder()
                        .setStationId(UUID.randomUUID().toString())
                        .setCity(RandomUtil.randomFrom(cities))
                        .setWeatherDirection(WeatherDirection.forNumber(rand.nextInt(8)))
                        .setTemperature(rand.nextInt(-10, 41))
                        .setRainChance(rand.nextDouble())
                        .setWindSpeed(rand.nextInt(21))
                        .setDate(new Date().getTime())
                        .build();

                    queue.add(data.toByteArray());
                }

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
