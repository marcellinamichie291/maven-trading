//package com.tradingbot.binance.utils;
//
//import com.binance.connector.client.impl.SpotClientImpl;
//import com.tradingbot.binance.binanceApiKeys;
//
//import java.util.LinkedHashMap;
//
//public class executeOrders {
//
//    public static String executeOrder(String side){
//        LinkedHashMap<String,Object> parameters = new LinkedHashMap<String,Object>();
//
//        SpotClientImpl client = new SpotClientImpl(binanceApiKeys.API_KEY, binanceApiKeys.SECRET_KEY);
//
//        parameters.put("symbol","BTCUSDT");
//        parameters.put("side", side);
//        parameters.put("type", "MARKET");
//        parameters.put("quantity", 0.001);
//        long currentTimestamp = System.currentTimeMillis();
//        System.out.println("Executing trade at " + currentTimestamp);
////        return "testing";
//        return client.createTrade().newOrder(parameters);
//    }
//}
