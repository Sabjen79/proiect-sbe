package org.example.data.encryption;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.example.FileLogger;

public class SimpleOPE {
    private static final long key = 57346583178L;
    private static final long factor = 73421231;

    public static Long encryptLong(long value) {
        return value * factor + key;
    }

    public static Long decryptLong(long value) {
        return (value - key) / factor;
    }

    public static Long encryptDouble(Double value) {
        var longValue = (long) Math.floor(value * 10000);

        return encryptLong(longValue);
    }

    public static Double decryptDouble(long value) {
        Double decrypted = (double) decryptLong(value);

        return decrypted / 10000.0;
    }

    // Aes for Strings
    private static final byte[] SECRET_KEY = "ThisIsASecretKey".getBytes(StandardCharsets.UTF_8);
    private static final SecretKeySpec secretKeySpec = new SecretKeySpec(SECRET_KEY, "AES");

    public static String encryptString(String strToEncrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);

            byte[] encryptedBytes = cipher.doFinal(strToEncrypt.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encryptedBytes);
        } catch(Exception e) {
            FileLogger.error(e.toString());
        }
        
        return "";
    }

    public static String decryptString(String strToDecrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);

            byte[] decodedBytes = Base64.getDecoder().decode(strToDecrypt);
            byte[] decryptedBytes = cipher.doFinal(decodedBytes);
            return new String(decryptedBytes, StandardCharsets.UTF_8);
        } catch(Exception e) {
            FileLogger.error(e.toString());
        }
        
        return "";
    }
}
