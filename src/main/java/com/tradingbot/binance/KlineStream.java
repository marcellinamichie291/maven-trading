package com.tradingbot.binance;


import com.binance.connector.client.impl.WebsocketClientImpl;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.concurrent.atomic.AtomicReference;


public class KlineStream {
    public static void main(String[] args) {

        AtomicReference<Long> lastT = new AtomicReference<>(1657305182008L);
        WebsocketClientImpl client = new WebsocketClientImpl();
        client.klineStream("BTCUSDT", "1m", ((event) -> {
            JSONObject jsonObj = new JSONObject(event.toString());

            JSONObject j = (JSONObject) jsonObj.get("k");
            Long currentTimestamp = (Long) j.get("t");

            boolean newCandle = !currentTimestamp.equals(lastT.get());
            if (newCandle) {
                System.out.println(lastT);
                System.out.println("\n\nNew Candle detected!");
                lastT.set(currentTimestamp);
                System.out.println(lastT.get());
                JSONArray currentKlineArray = new JSONArray();
                currentKlineArray.put(j.get("t"));
                currentKlineArray.put(j.get("o"));
                currentKlineArray.put(j.get("h"));
                currentKlineArray.put(j.get("l"));
                currentKlineArray.put(j.get("c"));
                currentKlineArray.put(j.get("v"));
                currentKlineArray.put(j.get("T"));
                currentKlineArray.put(j.get("q"));
                currentKlineArray.put(j.get("n"));
                currentKlineArray.put(j.get("V"));
                currentKlineArray.put(j.get("Q"));
                currentKlineArray.put(j.get("B"));
            }




            System.out.println("\n"+jsonObj);
//            System.out.println("Current candle timestamp is: "+currentTimestamp);

        }));
    }
}
