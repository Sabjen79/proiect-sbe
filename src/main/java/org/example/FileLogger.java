package org.example;

import java.io.FileWriter;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FileLogger {
    /** 
     * <p>
     * Level of logging
     * </p>
     * <ul>
     * <li>0 - {@code NO_LOGS}</li>
     * <li>1 - {@code DEFAULT} Only important logs are written</li>
     * <li>2 - {@code DEBUG} All logs are written</li>
     * </ul>
    */
    public static int LOG_LEVEL = 0;

    public static void info(String log) {
        if(LOG_LEVEL < 1) return;

        log(log, "INFO");
    }

    public static void error(String log) {
        if(LOG_LEVEL < 1) return;

        log(log, "ERROR");
    }
    
    public static void debug(String log) {
        if(LOG_LEVEL < 2) return;

        log(log, "DEBUG");
    }

    private static void log(String log, String type) {
        new Thread(() -> {
            try {
                String filename = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                var fileWriter = new FileWriter("logs\\" + filename + ".log", true);

                synchronized (fileWriter) {
                    fileWriter.write(MessageFormat.format(
                        "[{0}] [{1}] {2}\n",
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")),
                        type,
                        log
                    ));

                    fileWriter.flush();
                    fileWriter.close();
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}
