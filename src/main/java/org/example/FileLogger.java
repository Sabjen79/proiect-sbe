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

    private static FileWriter fileWriter = null;

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
        try {
            if(fileWriter == null) {
                String filename = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                fileWriter = new FileWriter("logs\\" + filename + ".log", true);
            }

            synchronized (fileWriter) {
                fileWriter.write(
                    MessageFormat.format("[{0}] [{1}.{2}] [{3}] {4}\n",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")),
                    Thread.currentThread().getStackTrace()[3].getClassName(),
                    Thread.currentThread().getStackTrace()[3].getMethodName(),
                    type,
                    log
                ));

                fileWriter.flush();
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
