package com.tradingbot.kafka.utils;

import org.apache.commons.compress.utils.IOUtils;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.stream.Collectors;

public class ApiClient {
    public static String fetch(String apiEndPoint){
        String result = "";
        try {
            URL url = new URL(apiEndPoint);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP Error code : "
                        + conn.getResponseCode());
            }
            result = new BufferedReader(new InputStreamReader(conn.getInputStream())).lines().parallel().collect(Collectors.joining("\n"));
            System.out.println("server response :"+result);
            conn.disconnect();
        } catch (Exception e) {
            System.out.println("Exception in NetClientGet:- " + e);
        }
        return result;
    }
}
