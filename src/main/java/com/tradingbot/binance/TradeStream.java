package com.tradingbot.binance;


import com.binance.connector.client.impl.WebsocketClientImpl;
import org.json.JSONObject;


public class TradeStream {
    public static void main(String[] args) {

        WebsocketClientImpl client = new WebsocketClientImpl();
        client.tradeStream("BTCUSDTFUTURES", ((event) -> {
            JSONObject jsonObj = new JSONObject(event.toString());
            System.out.println("\n"+jsonObj);
        }));
    }
}
