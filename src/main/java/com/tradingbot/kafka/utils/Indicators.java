package com.tradingbot.kafka.utils;

import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Indicators {

    public static double ema(double source,double previousEma,int period){
        double alpha = 2.0 / (double) (period + 1);
        return alpha * source + (1 - alpha) * previousEma;
    }


    public static double sma(List<Double> source , int period) throws Exception {
        if (source.size() != period){
            throw new Exception("source.size() != period");
        }
        Double sum = 0.0;
        if(!source.isEmpty()) {
            for (Double mark : source) {
                sum += mark;
            }
            return sum / source.size();
        }
        return sum;
    }

    public static JSONObject primaryIndicator(JSONObject previousIndicatorDatapoints, JSONArray klineJ,int interval) throws Exception {
        int n1=9;
        int n2=12;
        int s1 =3;
        double high = 0;
        double low =0;
        double close =0;


//        Kline
        Long openTime = (Long) klineJ.get(0);
        try {
            high = (double) klineJ.get(2);
            low = (double) klineJ.get(3);
            close = (double) klineJ.get(4);
        }catch(Exception e){
            high = Double.parseDouble(klineJ.get(2).toString());
            low = Double.parseDouble(klineJ.get(3).toString());
            close = Double.parseDouble(klineJ.get(4).toString());
        }

//        Validate primary indicator
        long previousPrimaryOpenTime = (long) previousIndicatorDatapoints.get("next_candle_timestamp");
        if (openTime  - previousPrimaryOpenTime > 10){
            throw new Exception("Open time of Kline and Primary indicator didn't match " + openTime + " " + previousPrimaryOpenTime);
        }

//        Primary Datapoint's
        double previous_esa = Double.parseDouble(previousIndicatorDatapoints.get("esa").toString());
        double previous_d = Double.parseDouble(previousIndicatorDatapoints.get("d").toString());
        double previous_absApmEsa = Double.parseDouble(previousIndicatorDatapoints.get("absApmEsa").toString());
        double previousWt1 = Double.parseDouble(previousIndicatorDatapoints.get("wt1").toString());
        JSONArray wt1sJ = (JSONArray) previousIndicatorDatapoints.get("wt1s");
        List<Double> wt1s = new ArrayList<Double>();
        for (int i=0;i<wt1sJ.length();i++){
            double Dwt1 = Double.parseDouble(wt1sJ.get(i).toString());
            wt1s.add(Dwt1);
        }
//                Ap
        double ap = (high + close + low)/3;
//        System.out.println("ap : "+ String.valueOf(ap));

//                Esa
        double esa = ema(ap,previous_esa,n1);
//        System.out.println("esa : "+ String.valueOf(esa));

//                D
        double absApmEsa = Math.abs(esa - ap);
//        System.out.println("absApmEsa : "+ String.valueOf(absApmEsa));
//        System.out.println("previous_absApmEsa : "+ String.valueOf(previous_absApmEsa));
        double d = ema(absApmEsa,previous_d,n1);
//        System.out.println("d : "+ String.valueOf(d));

//                Ci
        double ci = (ap - esa) / (0.015 * d);
//        System.out.println("ci : "+ String.valueOf(ci));

//                Wt1
        double wt1 = ema(ci,previousWt1,n2);
//        System.out.println("wt1 : "+ String.valueOf(wt1));

//                Update wt1s
        wt1s.add(wt1);
        wt1s = wt1s.subList(1,wt1s.size());
//        System.out.println("wt1s : "+ String.valueOf(wt1s));

//                Wt2
        double wt2 = sma(wt1s,s1);
//        System.out.println("wt2 : "+ String.valueOf(wt2));

//                Buy Signal
        String buySignal;
        if (wt1>wt2){
            buySignal="G";
        }else{
            buySignal="R";
        }

//        Update ESA

        JSONObject currentIndicatorDatapoints = new JSONObject();
        currentIndicatorDatapoints.put("esa",BigDecimal.valueOf(esa));
        currentIndicatorDatapoints.put("d",BigDecimal.valueOf(d));
        currentIndicatorDatapoints.put("absApmEsa",BigDecimal.valueOf(absApmEsa));
        currentIndicatorDatapoints.put("wt1",BigDecimal.valueOf(wt1));
        currentIndicatorDatapoints.put("buySignal",buySignal);
        currentIndicatorDatapoints.put("next_candle_timestamp",previousPrimaryOpenTime+((long) interval *60*1000));

        List<BigDecimal> wt1sB = new ArrayList<BigDecimal>();
        for (Double aDouble : wt1s) {
            BigDecimal Dwt1 = BigDecimal.valueOf(aDouble);
            wt1sB.add(Dwt1);
        }
        currentIndicatorDatapoints.put("wt1s",wt1sB);
        return currentIndicatorDatapoints;
    }
}


