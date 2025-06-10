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

    public static String randomString(int length) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < length) {
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }
}
