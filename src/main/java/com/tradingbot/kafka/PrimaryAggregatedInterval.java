package com.tradingbot.kafka;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import static com.tradingbot.kafka.AppConfigs.Logpath;
import static com.tradingbot.kafka.utils.GapFiller.fillGapPrimaryAggregator;
import static com.tradingbot.kafka.utils.Indicators.primaryIndicator;

public class PrimaryAggregatedInterval {

    public static void run(JSONObject params) throws IOException {
        int interval =0;
        try {
            String pair = (String) params.get("pair");
            interval = Integer.parseInt( String.valueOf(params.get("interval")));
            String sourceTopic = String.format("%s_%sM", pair, interval);
            String producerTopic = String.format("%s_%sM_PI", pair, interval);
            String bootstrapServers = (String) params.get("bootstrapServers");
//        Producer init
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, " org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


//        Create consumer properties
            props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

//        Init consumer
            KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);

//        Assign Consumer
            TopicPartition topicPartition = new TopicPartition(sourceTopic, 0);
            List<TopicPartition> topics = Collections.singletonList(topicPartition);
            consumer.assign(topics);

//        Get Latest Kline from historical server

            Map<String, Object> primaryIndicatorAgg = fillGapPrimaryAggregator(pair, interval);
            System.out.println("primaryIndicatorAgg:" + primaryIndicatorAgg + "\n\n");
            long open_time = (long) primaryIndicatorAgg.get("open_time");
            JSONObject previousIndicatorDatapoints = (JSONObject) primaryIndicatorAgg.get("previousIndicatorDatapoints");
            JSONArray currentKline;
//        Consume Klines and aggregate
            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(1));
                for (ConsumerRecord<Long, String> record : records) {
                    open_time = record.key();

                    if (interval == 1) {
                        JSONObject klineJ = new JSONObject(record.value());
                        currentKline = new JSONArray();
                        currentKline.put(klineJ.get("open_time"));
                        currentKline.put(Double.parseDouble(klineJ.get("open").toString()));
                        currentKline.put(Double.parseDouble(klineJ.get("high").toString()));
                        currentKline.put(Double.parseDouble(klineJ.get("low").toString()));
                        currentKline.put(Double.parseDouble(klineJ.get("close").toString()));
                    } else {
                        System.out.println(record.value());
                        JSONArray klineJ = new JSONArray(record.value());
                        System.out.println(klineJ);
                        currentKline = new JSONArray(record.value());
                        currentKline.put(klineJ.get(0));
                        currentKline.put(Double.parseDouble(klineJ.get(1).toString()));
                        currentKline.put(Double.parseDouble(klineJ.get(2).toString()));
                        currentKline.put(Double.parseDouble(klineJ.get(3).toString()));
                        currentKline.put(Double.parseDouble(klineJ.get(4).toString()));
                    }
                    previousIndicatorDatapoints = primaryIndicator(previousIndicatorDatapoints, currentKline, interval);
//                    System.out.printf("Primary indicator (%s) : %s%n%n",params,previousIndicatorDatapoints);

                    JSONObject currentPI = new JSONObject();
                    currentPI.put("open_time", open_time);
                    currentPI.put("value", previousIndicatorDatapoints.get("buySignal"));
                    producer.send(new ProducerRecord<>(producerTopic, producerTopic, currentPI.toString()));

                }
            }

        } catch (Exception e) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(String.format(Logpath, "PrimaryAggregatedInterval"+interval))));
            writer.write(ExceptionUtils.getStackTrace(e));
            writer.close();
        }
    }
}
