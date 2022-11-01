package com.tradingbot.binance;

import com.binance.connector.client.impl.SpotClientImpl;

import java.util.LinkedHashMap;

public class KlinesEndpoint {

    public static String getKline(LinkedHashMap<String, Object> parameters) {
        SpotClientImpl client = new SpotClientImpl("", "");
        return  client.createMarket().klines(parameters);
    }
}
