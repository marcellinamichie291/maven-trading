package com.tradingbot.kafka.model;

import com.tradingbot.kafka.AppConfigs;
import com.tradingbot.kafka.TradeStreamProducer;
import com.tradingbot.kafka.utils.ApiClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.Properties;
import java.util.logging.Logger;

public class klineHistoryloader {

    public static JSONObject getPrimaryIndicatorInitData(String pair, Integer interval) {

//        Build link
        String apiEndPoint = String.format(AppConfigs.PrimaryIndicatorDataServerEndpoint, pair, interval);
        System.out.printf("Request: %s",apiEndPoint);
//        Get Primary Indicator Init Data from historical data server
        String api_response = ApiClient.fetch(apiEndPoint);

        return new JSONObject(api_response);

    }

    public static JSONArray getLatestKlineInterval(String pair, Integer interval, Integer limit) {

//        Build link
        String apiEndPoint = String.format(AppConfigs.LatestKlineIntervalEndpoint, pair+"_"+interval+"m",limit);
        System.out.println(apiEndPoint);
//        Get Primary Indicator Init Data from historical data server
        String apiResponse = ApiClient.fetch(apiEndPoint);

        return new JSONArray(apiResponse);

    }

    public static void publishKlineToTopic(String pair, String interval, JSONArray klines){
        final Logger log = Logger.getLogger( TradeStreamProducer.class.getName() );
        String topicName= pair + "_" + interval;

//        Create producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", " org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("client.id", pair+"_Producer");


//        Init producer
        KafkaProducer<Long, String> producer = new KafkaProducer<Long, String>(props);

//        Publish Kline
        for (int i = 0, size = klines.length(); i < size; i++) {
            JSONObject klineJ = new JSONObject(klines.get(i).toString());
            JSONObject klineFinal = new JSONObject();
            klineFinal.put("open_time",klineJ.get("open_time"));
            klineFinal.put("open",klineJ.get("open"));
            klineFinal.put("high",klineJ.get("high"));
            klineFinal.put("low",klineJ.get("low"));
            klineFinal.put("close",klineJ.get("close"));
            Long key = (Long) klineFinal.get("open_time");
            String value = klineFinal.toString();
            System.out.println(value);
            producer.send(new ProducerRecord<>(topicName, key, value));
        }
        producer.close();
    }


}
