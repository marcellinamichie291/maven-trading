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
import static com.tradingbot.kafka.utils.GapFiller.fillGapAggregator;
import static com.tradingbot.kafka.utils.Logs.initLogger;

public class AggregateInterval {

    public static void run(JSONObject params) throws IOException {
        int interval=0;
        try {
            String pair = (String) params.get("pair");
            String bootstrapServers = (String) params.get("bootstrapServers");
            interval = Integer.parseInt( String.valueOf(params.get("interval")));
            String sourceTopic = pair + "_1M";
            String producerTopic = String.format("%s_%sM", pair, interval);
            Logger logger = initLogger(String.format("AggregateInterval_%s",producerTopic));

//        Producer init
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, " org.apache.kafka.common.serialization.LongSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            KafkaProducer<Long, String> producer = new KafkaProducer<Long, String>(props);


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

            Map<String, Object> intervalAgg = fillGapAggregator(pair, interval);
            logger.info(String.format("intervalAgg %s",intervalAgg));

            int step = (int) intervalAgg.get("step");
            long open_time = (long) intervalAgg.get("open_time");
            double open_interval = (double) intervalAgg.get("open_interval");
            double high_interval = (double) intervalAgg.get("high_interval");
            double low_interval = (double) intervalAgg.get("low_interval");
            double volume_interval = (double) intervalAgg.get("volume_interval");
//        Consume Klines and aggregate
            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(1));
                for (ConsumerRecord<Long, String> record : records) {
                    JSONObject klineJson = new JSONObject(record.value());
//                System.out.println(klineJson);
                    double high = Double.parseDouble(klineJson.get("high").toString());
                    double low = Double.parseDouble(klineJson.get("low").toString());
                    double close = Double.parseDouble(klineJson.get("close").toString());
                    double volume = Double.parseDouble(klineJson.get("volume").toString());

                    if (step == 0) {
                        open_time = record.key();
                        open_interval = Double.parseDouble(klineJson.get("open").toString());
                        low_interval = low;
                        high_interval = high;
                    }

                    if (high > high_interval) {
                        high_interval = high;
                    }
                    if (low < low_interval) {
                        low_interval = low;
                    }
                    volume_interval += volume;
                    step += 1;
                    if (step == interval) {

                        JSONArray currentKline = new JSONArray();
                        currentKline.put(open_time);
                        currentKline.put(open_interval);
                        currentKline.put(high_interval);
                        currentKline.put(low_interval);
                        currentKline.put(close);
                        currentKline.put(volume_interval);

                        logger.info(params + " Open time:" + open_time + " Open:" + open_interval + " High:" + high_interval + " Low:" + low_interval + " Close:" + close + " Volume:" + volume_interval+"\n\n");

                        producer.send(new ProducerRecord<>(producerTopic, open_time, currentKline.toString()));
                        step = 0;
                        volume_interval = 0;
                    }
                }
            }
        } catch (Exception e) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(String.format(Logpath, "AggregateInterval" + interval))));
            writer.write(ExceptionUtils.getStackTrace(e));
            writer.close();
        }

    }
}
