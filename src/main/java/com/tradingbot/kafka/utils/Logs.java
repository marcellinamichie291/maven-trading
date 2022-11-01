package com.tradingbot.kafka.utils;

import com.tradingbot.kafka.AppConfigs;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Logs {
    public static Logger initLogger(String loggerName) {

        Logger logger = Logger.getLogger(loggerName);
        FileHandler fh;

        try {
            // This block configure the logger with handler and formatter
            fh = new FileHandler(String.format(AppConfigs.Logpath,loggerName));
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return logger;
    }

}
