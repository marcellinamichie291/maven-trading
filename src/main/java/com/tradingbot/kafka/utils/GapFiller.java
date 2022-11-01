package com.tradingbot.kafka.utils;

import com.binance.connector.client.impl.SpotClientImpl;
import com.tradingbot.kafka.AppConfigs;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Instant;
import java.util.*;
import java.util.logging.Logger;

import static com.tradingbot.kafka.model.klineHistoryloader.getLatestKlineInterval;
import static com.tradingbot.kafka.model.klineHistoryloader.getPrimaryIndicatorInitData;
import static com.tradingbot.kafka.utils.Indicators.*;
import static java.util.logging.Logger.getLogger;

public class GapFiller {


    public static Map<String, Object>  fillGapPrimaryAggregator(String pair, int interval) throws Exception {

        JSONArray latestApiKlines = getLatestKlineInterval(pair, interval, 1);
        JSONObject latestApiKline =  (JSONObject) latestApiKlines.get(0);
        JSONObject previousIndicatorDatapoints = getPrimaryIndicatorInitData(pair, interval);
        System.out.println("latest kline:" + latestApiKline);
        System.out.println("latest Primary:" + previousIndicatorDatapoints);

        long latestApiTimestamp = (long) latestApiKline.get("open_time");

        long CurrentTimestamp = Instant.now().toEpochMilli();

        int gap_minutes = (int) ((CurrentTimestamp - latestApiTimestamp) / 1000 / 60);

//        Get Klines
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("symbol", pair);
        parameters.put("interval", "1m");
        parameters.put("limit", gap_minutes);
        SpotClientImpl client = new SpotClientImpl("", "");
        String klines = client.createMarket().klines(parameters);
        JSONArray klinesJ = new JSONArray(klines);
        System.out.println(klinesJ.length() + " " + klinesJ);

//        Aggregate
        int step = 0;
        long open_time = 0;
        double volume_interval = 0;
        double open_interval = 0;
        double high_interval = 0;
        double low_interval = 0;
        double close = 0;
        for (int i = interval - 1; i < klinesJ.length() - 1; i++) {
            JSONArray klineJ = (JSONArray) klinesJ.get(i);
            System.out.println(klineJ);
            double high = (double) Double.parseDouble((String) klineJ.get(2));
            double low = (double) Double.parseDouble((String) klineJ.get(3));
            close = (double) Double.parseDouble((String) klineJ.get(4));
            double volume = (double) Double.parseDouble((String) klineJ.get(5));


            volume_interval += volume;
            if (high > high_interval) {
                high_interval = high;
            }
            if (low < low_interval) {
                low_interval = low;
            }

            if (step == 0) {
                open_time = (long) klineJ.get(0);
                open_interval = (double) Double.parseDouble((String) klineJ.get(1));
                low_interval = low;
                high_interval = high;
            }
            step += 1;
            if (step == interval) {

                JSONArray currentKline = new JSONArray();
                currentKline.put(open_time);
                currentKline.put(open_interval);
                currentKline.put(high_interval);
                currentKline.put(low_interval);
                currentKline.put(close);

                previousIndicatorDatapoints = primaryIndicator(previousIndicatorDatapoints, currentKline, interval);
                System.out.println("Open time:" + open_time + " Open:" + open_interval + " High:" + high_interval + " Low:" + low_interval + " Close:" + close + " Volume:" + volume_interval);
                System.out.println("Primary indicator:" + previousIndicatorDatapoints + "\n\n");
                volume_interval = 0;
                step = 0;
            }

        }
        Map<String, Object> primaryIndicatorAgg = new HashMap<String, Object>();
        primaryIndicatorAgg.put("open_interval", open_interval);
        primaryIndicatorAgg.put("high_interval", high_interval);
        primaryIndicatorAgg.put("low_interval", low_interval);
        primaryIndicatorAgg.put("volume_interval", volume_interval);
        primaryIndicatorAgg.put("close", close);
        primaryIndicatorAgg.put("step", step);
        primaryIndicatorAgg.put("open_time", open_time);
        primaryIndicatorAgg.put("previousIndicatorDatapoints", previousIndicatorDatapoints);
        return primaryIndicatorAgg;
    }

    public static Map<String, Object> fillGapAggregator(String pair, int interval) throws Exception {
        Logger logger = getLogger(String.format("fillGapAggregator %s %s",pair,interval));
        JSONArray latestApiKlines = getLatestKlineInterval(pair, interval, 1);
        JSONObject latestApiKline = (JSONObject) latestApiKlines.get(0);

        logger.info("latest kline:" + latestApiKline);

        long latestApiTimestamp = (long) latestApiKline.get("open_time");

        long CurrentTimestamp = Instant.now().toEpochMilli();

        int gap_minutes = (int) ((CurrentTimestamp - latestApiTimestamp) / 1000 / 60);

//        Get Klines
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("symbol", pair);
        parameters.put("interval", "1m");
        parameters.put("limit", gap_minutes);
        SpotClientImpl client = new SpotClientImpl("", "");
        JSONArray klinesJ = new JSONArray(client.createMarket().klines(parameters));
        logger.info(klinesJ.length() + " " + klinesJ);

//        Aggregate
        int step = 0;
        long open_time = 0;
        double volume_interval = 0;
        double open_interval = 0;
        double high_interval = 0;
        double low_interval = 0;
        double close = 0;
        for (int i = interval - 1; i < klinesJ.length() - 1; i++) {

            JSONArray klineJ = (JSONArray) klinesJ.get(i);
            logger.info(klineJ.toString());
            double high = Double.parseDouble((String) klineJ.get(2));
            double low = Double.parseDouble((String) klineJ.get(3));
            close = Double.parseDouble((String) klineJ.get(4));
            double volume = Double.parseDouble((String) klineJ.get(5));


            volume_interval += volume;
            if (high > high_interval) {
                high_interval = high;
            }
            if (low < low_interval) {
                low_interval = low;
            }

            if (step == 0) {
                open_time = (long) klineJ.get(0);
                open_interval = (double) Double.parseDouble((String) klineJ.get(1));
                low_interval = low;
                high_interval = high;
            }
            step += 1;
            if (step == interval) {

                JSONArray currentKline = new JSONArray();
                currentKline.put(open_time);
                currentKline.put(open_interval);
                currentKline.put(high_interval);
                currentKline.put(low_interval);
                currentKline.put(close);

                logger.info("Open time:" + open_time + " Open:" + open_interval + " High:" + high_interval + " Low:" + low_interval + " Close:" + close + " Volume:" + volume_interval);
                volume_interval = 0;
                step = 0;
            }

        }
        Map<String, Object> intervalAgg = new HashMap<String, Object>();
        intervalAgg.put("open_interval", open_interval);
        intervalAgg.put("high_interval", high_interval);
        intervalAgg.put("low_interval", low_interval);
        intervalAgg.put("volume_interval", volume_interval);
        intervalAgg.put("close", close);
        intervalAgg.put("step", step);
        intervalAgg.put("open_time", open_time);
        return intervalAgg;
    }


    public static Map<String, Object> fillGapEmaAggregator(String pair, int interval, int period) throws Exception {
//        Get Primary Indicator Init Data from historical data server
        String apiEndPoint = String.format(AppConfigs.EmaDataServerEndpoint, pair, interval, period);
        String api_response = ApiClient.fetch(apiEndPoint);
        JSONObject previousEmaDatapoints = new JSONObject(api_response);


        System.out.println("previousEmaDatapoints:" + previousEmaDatapoints);

        long latestApiTimestamp = (long) previousEmaDatapoints.get("open_time");

        double previousEma = Double.parseDouble(previousEmaDatapoints.get("latest_ema").toString());

        long CurrentTimestamp = Instant.now().toEpochMilli();

        int gap_minutes = (int) ((CurrentTimestamp - latestApiTimestamp) / 1000 / 60);

//        Get Klines
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("symbol", pair);
        parameters.put("interval", "1m");
        parameters.put("limit", gap_minutes);
        SpotClientImpl client = new SpotClientImpl("", "");
        String klines = client.createMarket().klines(parameters);
        JSONArray klinesJ = new JSONArray(klines);
        System.out.println(klinesJ.length() + " " + klinesJ);

//        Aggregate
        List<Double> opens = new ArrayList<Double>();
        List<Double> highs = new ArrayList<Double>();
        List<Double> lows = new ArrayList<Double>();
        List<Double> closes = new ArrayList<Double>();
        int step = 0;
        long open_time = 0;
        double volume_interval = 0;

        for (int i = interval - 1; i < klinesJ.length() - 1; i++) {

            JSONArray klineJ = (JSONArray) klinesJ.get(i);
            System.out.println(klineJ);
            double open = (double) Double.parseDouble((String) klineJ.get(1));
            double high = (double) Double.parseDouble((String) klineJ.get(2));
            double low = (double) Double.parseDouble((String) klineJ.get(3));
            double close = (double) Double.parseDouble((String) klineJ.get(4));
            double volume = (double) Double.parseDouble((String) klineJ.get(5));


            opens.add(open);
            highs.add(high);
            lows.add(low);
            closes.add(close);
            volume_interval += volume;


            if (step == 0) {
                open_time = (long) klineJ.get(0);
            }
            step += 1;
            if (step == interval) {
                double open_interval = opens.get(0);
                double high_interval = highs.stream().mapToDouble(Double::doubleValue).max().getAsDouble();
                double low_interval = lows.stream().mapToDouble(Double::doubleValue).min().getAsDouble();
                double close_interval = closes.get(interval - 1);

                JSONArray currentKline = new JSONArray();
                currentKline.put(open_time);
                currentKline.put(open_interval);
                currentKline.put(high_interval);
                currentKline.put(low_interval);
                currentKline.put(close_interval);
                previousEma = ema(close_interval, previousEma, period);
                System.out.println("Open time:" + open_time + " Open:" + open_interval + " High:" + high_interval + " Low:" + low_interval + " Close:" + close_interval + " Volume:" + volume_interval);
                System.out.println("EMA" + period + ": " + previousEma + "\n\n");
                opens = new ArrayList<Double>();
                highs = new ArrayList<Double>();
                lows = new ArrayList<Double>();
                closes = new ArrayList<Double>();
                volume_interval = 0;
                step = 0;
            }
        }
        Map<String, Object> currentEma = new HashMap<String, Object>();
        currentEma.put("open_time", open_time);
        currentEma.put("ema", previousEma);

        return currentEma;
    }

    public static Map<String, Object> getFilterFromApi(String pair, int interval, String filterType) throws Exception {
        JSONArray latestApiKlines = getLatestKlineInterval(pair, interval, 2);
        JSONObject latestApiKline = (JSONObject) latestApiKlines.get(0);
        JSONObject previousKline = (JSONObject) latestApiKlines.get(1);

        System.out.printf("latestApiKlines %s%n",latestApiKlines);
        System.out.printf("latestApiKline %s%n",latestApiKline);
        double openLast = Double.parseDouble(latestApiKline.get("open").toString());
        double highLast = Double.parseDouble(latestApiKline.get("high").toString());
        double lowLast = Double.parseDouble(latestApiKline.get("low").toString());
        double closeLast = Double.parseDouble(latestApiKline.get("close").toString());
        double volumeLast = Double.parseDouble(latestApiKline.get("volume").toString());
        double previousVolume = Double.parseDouble(previousKline.get("volume").toString());
        Map<String, Object> filter = new HashMap<String, Object>();
        filter.put("open_time",Long.parseLong(latestApiKline.get("open_time").toString()));
        if(Objects.equals(filterType, "VOLPC")){
            filter.put("value",(volumeLast-previousVolume)/previousVolume);
        } else if(Objects.equals(filterType, "RANGE")){
            filter.put("value",(highLast-lowLast)/openLast);
        } else if(Objects.equals(filterType, "VOLATILITY")){
            filter.put("value",(closeLast-openLast)/openLast);
        } else {
            throw new Exception(String.format("Filter Type not found %s",filterType));
        }
        return filter;
    }

    public static Map<String, Object> getMcFromApi(String pair, int interval) throws Exception {
        JSONObject previousIndicatorDatapoints = getPrimaryIndicatorInitData(pair, interval);
        double wt1 = Double.parseDouble(previousIndicatorDatapoints.get("wt1").toString());
        double wt2 = Double.parseDouble(previousIndicatorDatapoints.get("wt2").toString());
        Map<String, Object> mc = new HashMap<String, Object>();
        mc.put("open_time",Long.parseLong(previousIndicatorDatapoints.get("open_time").toString()));
        if (wt1>wt2){
            mc.put("value","G");
        }else{
            mc.put("value","R");
        }
        return mc;
    }

    public static JSONObject getEmafromApi(String pair, int interval, int period) throws Exception {
        String apiEndPoint = String.format(AppConfigs.EmaDataServerEndpoint, pair, interval, period);
        String api_response = ApiClient.fetch(apiEndPoint);
        return new JSONObject(api_response);
    }
}