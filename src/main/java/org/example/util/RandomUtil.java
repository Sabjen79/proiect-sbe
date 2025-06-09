package org.example.util;

import java.util.List;
import java.util.Random;

public class RandomUtil {
    public static <T> T randomFrom(List<T> array) {
        var random = new Random();
        return array.get(random.nextInt(array.size()));
    }

    public static <T> T randomFrom(T[] array) {
        var random = new Random();
        
        return array[random.nextInt(array.length)];
    }
}
