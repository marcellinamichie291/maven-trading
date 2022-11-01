package com.tradingbot.kafka;
import org.json.JSONObject;


public class RunTest {
    public static void main(String[] args) {
        String thread = System.getProperty("thread");
        JSONObject threadParams = new JSONObject(thread);
        System.out.println(threadParams);
        Thread t = new Thread(new Runnable(threadParams));
        t.start();
    }
}
