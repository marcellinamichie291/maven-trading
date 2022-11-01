package com.tradingbot.binance;

import com.binance.connector.client.impl.SpotClientImpl;


import java.util.LinkedHashMap;


public class Client {

    public static String getMarketData(String symbol, String interval, int limit){

        // Create params object for the client
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("symbol", symbol);
        parameters.put("interval", interval);
        parameters.put("limit", limit);

        // Init Client
        SpotClientImpl client = new SpotClientImpl(Config.API_KEY, Config.SECRET_KEY);

        // Pull data
        return client.createMarket().klines(parameters);

    }


}

