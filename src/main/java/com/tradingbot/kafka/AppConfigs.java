package com.tradingbot.kafka;

public class AppConfigs {
    public final static String applicationID = "TradingBot";
    public final static String topicName = "BTCUSDT_1M";
    static String ApiEndpoint = "157.245.153.151:8000";
    public final static String PrimaryIndicatorDataServerEndpoint = "http://"+ApiEndpoint+"/primary_indicator_data/%s/%d/";
    public final static String EmaDataServerEndpoint = "http://"+ApiEndpoint+"/ema_data/%s/%d/%d";
    public final static String LatestKlineIntervalEndpoint = "http://"+ApiEndpoint+"/lastest_klines/%s/%s";
    public final static String KafkaThreadsEndpoint = "http://"+ApiEndpoint+"/kafka_threads";
    public final static String Logpath = "/home/kafka/tradingbot_logs/%s.txt";
//    public final static String Logpath = "C:\\Freelance\\OLLIEBOT\\project docs\\output\\%s.txt";


}
