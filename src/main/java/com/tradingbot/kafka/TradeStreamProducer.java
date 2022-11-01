package com.tradingbot.kafka;

import com.binance.connector.client.impl.WebsocketClientImpl;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import static com.tradingbot.kafka.AppConfigs.Logpath;


public class TradeStreamProducer {

    public static void run(JSONObject params) throws IOException {
        try {
            String pair = (String) params.get("pair");
            String topicName = pair + "_TRADES";
            String bootstrapServers = (String) params.get("bootstrapServers");

            //Create producer properties
            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrapServers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("acks", "all");
            props.put("client.id", AppConfigs.applicationID);

            //Init producer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

            //Create websocket client
            WebsocketClientImpl client = new WebsocketClientImpl();

            //Listen to trades
            client.tradeStream(pair, ((event) -> {
                JSONObject tradeJson = new JSONObject(event.toString());
                String key = AppConfigs.topicName;
                String value = tradeJson.toString();

                //Publish trades
                producer.send(new ProducerRecord<>(topicName, key, value));
            }));


        } catch (Exception e) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(String.format(Logpath, "TradeStreamProducer"))));
            writer.write(ExceptionUtils.getStackTrace(e));
            writer.close();
        }
    }
}