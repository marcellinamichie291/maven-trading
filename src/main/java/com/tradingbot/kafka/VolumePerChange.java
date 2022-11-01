package com.tradingbot.kafka;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
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
import java.util.Properties;

import static com.tradingbot.kafka.AppConfigs.Logpath;

public class VolumePerChange {

    public static void run(JSONObject params) throws IOException {
        int interval=0;
        try {

            String pair = (String) params.get("pair");
            interval = Integer.parseInt( String.valueOf(params.get("interval")));
            String bootstrapServers = (String) params.get("bootstrapServers");

            String sourceTopic = String.format("%s_%sM", pair, interval);
            String producerTopic = String.format("%s_%sM_VOLPC", pair, interval);

//        Producer init
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("key.serializer", " org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("acks", "all");
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

            double currentVolume;
            double previousVolume = 1;
            Long open_time;
            double volpc;
            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(1));
                for (ConsumerRecord<Long, String> record : records) {

                    JSONArray klineJ = new JSONArray(record.value());
                    System.out.println(klineJ);
                    open_time = record.key();

                    currentVolume = Double.parseDouble(klineJ.get(5).toString());
                    volpc = (currentVolume - previousVolume) / previousVolume;
                    previousVolume = currentVolume;
//                    System.out.printf("volpc (%s): %s%n%n",params,volpc);

                    JSONObject emaRecord = new JSONObject();
                    emaRecord.put("open_time", open_time);
                    emaRecord.put("value", volpc);
                    producer.send(new ProducerRecord<>(producerTopic, producerTopic, emaRecord.toString()));

                }
            }
        } catch (Exception e) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(String.format(Logpath, "VolumePerChange" + interval))));
            writer.write(ExceptionUtils.getStackTrace(e));
            writer.close();
        }
    }
}