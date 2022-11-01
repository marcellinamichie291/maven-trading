package com.tradingbot.kafka;

import java.time.Instant;

public class test {
    public static void main(String[] args) {
        long open =1665468000000L;
        long current = Instant.now().toEpochMilli();
        int primary = 360;
        double gracePeriod = 2.0;

        double v = ((current - open) / 1000.0) - primary * 60*2;
        boolean b = ((Instant.now().toEpochMilli()-open) / 1000.0) - (primary*60*2) < gracePeriod ;
        System.out.printf("current:%s%n((Instant.now().toEpochMilli()-open) / 1000.0):%s%n((current-open) / 1000.0) - primary*60*2:%s%nB:%s%nprimary * 60:%s%n",current,((current-open) / 1000.0), v,b,primary * 60*2);
    }
}
