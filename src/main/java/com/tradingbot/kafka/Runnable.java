package com.tradingbot.kafka;

import org.json.JSONObject;

import java.io.IOException;
import java.util.Objects;

public class Runnable implements java.lang.Runnable {
    private final JSONObject params;

    public Runnable(JSONObject params) {
        this.params = params;
    }

    public void run() {
        try {
            String className = (String) this.params.get("class");
            JSONObject params = (JSONObject) this.params.get("params");

            if(Objects.equals(className, "MarketDataStreamWindowedAgg")){
                MarketDataStreamWindowedAgg.run(params);
            } else if(Objects.equals(className, "TradeStreamProducer")){
                TradeStreamProducer.run(params);
            } else if(Objects.equals(className, "AggregateInterval")){
                AggregateInterval.run(params);
            } else if(Objects.equals(className, "EmaAggregatedInterval")){
                EmaAggregatedInterval.run(params);
            } else if(Objects.equals(className, "PrimaryAggregatedInterval")){
                PrimaryAggregatedInterval.run(params);
            } else if(Objects.equals(className, "RangeCalculation")){
                RangeCalculation.run(params);
            } else if(Objects.equals(className, "StrategyAggregator")){
                StrategyAggregator.run(params);
            } else if(Objects.equals(className, "VolatilityCalculation")){
                VolatilityCalculation.run(params);
            }else if(Objects.equals(className, "VolumePerChange")){
                VolumePerChange.run(params);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

