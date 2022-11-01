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
import java.time.Instant;
import java.util.*;
import java.util.logging.Logger;

import static com.tradingbot.kafka.AppConfigs.Logpath;
import static com.tradingbot.kafka.model.klineHistoryloader.getLatestKlineInterval;
import static com.tradingbot.kafka.utils.GapFiller.*;
import static com.tradingbot.kafka.utils.Logs.initLogger;


public class StrategyAggregator {

    public static void run(JSONObject params) throws IOException {
        int strategyId = 0;
        try {
            String pair = (String) params.get("pair");
            String strategyType = (String) params.get("strategyType");
            int primary = Integer.parseInt( String.valueOf(params.get("primary")));
            int secondary = Integer.parseInt( String.valueOf(params.get("secondary")));
            String emaFirstField = (String) params.get("emaFirstField");
            String emaSecondField = (String) params.get("emaSecondField");
            String emaOperator = (String) params.get("emaOperator");
            int emaInterval = Integer.parseInt( String.valueOf(params.get("emaInterval")));
            String filterType = (String) params.get("filterType");
            int filterInterval = Integer.parseInt( String.valueOf(params.get("filterInterval")));
            double filterRangeS = Double.parseDouble(String.valueOf(params.get("filterRangeS")));
            double filterRangeE = Double.parseDouble(String.valueOf(params.get("filterRangeE")));
            String bootstrapServers = (String) params.get("bootstrapServers");
            strategyId = Integer.parseInt( String.valueOf(params.get("strategyId")));
            String strategyName = (String) params.get("strategyName");
            Logger logger = initLogger(String.format("Strategy_%s",strategyId));

//            Create producer properties
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("key.serializer", " org.apache.kafka.common.serialization.IntegerSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("acks", "all");
//            Init producer
            KafkaProducer<String, JSONObject> producer = new KafkaProducer<String, JSONObject>(props);

//            Create consumer properties
            props = new Properties();
            props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

//            Init consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


//            Assign Consumer
            String primaryIndicatorTopic = String.format("%s_%sM_PI", pair, primary);
            String secondaryIndicatorTopic = String.format("%s_%sM_PI", pair, secondary);
            String filterTopic = String.format("%s_%sM_%s", pair, filterInterval, filterType);
            String emaSecondFieldTopic = String.format("%s_%sM_EMA%s", pair, emaInterval, emaSecondField.replace("EMA", ""));
            String emaFirstFieldTopic;

            if (emaFirstField.contains("Open")) {
                emaFirstFieldTopic = String.format("%s_%sM", pair, emaInterval);
            } else {
                emaFirstFieldTopic = String.format("%s_%sM_EMA%s", pair, emaInterval, emaFirstField.replace("EMA", ""));
            }
            logger.info(String.format("Topics Subscribed to:\nprimaryIndicatorTopic: %s\nsecondaryIndicatorTopic: %s\nfilterTopic: %s\nemaFirstFieldTopic: %s\nemaSecondFieldTopic: %s\n", primaryIndicatorTopic, secondaryIndicatorTopic, filterTopic, emaFirstFieldTopic, emaSecondFieldTopic));

            TopicPartition primaryIndicatorTopicPartition = new TopicPartition(primaryIndicatorTopic, 0);
            TopicPartition secondaryIndicatorTopicPartition = new TopicPartition(secondaryIndicatorTopic, 0);
            TopicPartition emaFirstFieldTopicP = new TopicPartition(emaFirstFieldTopic, 0);
            TopicPartition emaSecondFieldTopicP = new TopicPartition(emaSecondFieldTopic, 0);
            TopicPartition filterTopicP = new TopicPartition(filterTopic, 0);

            List<TopicPartition> topics = new ArrayList<>();
            topics.add(primaryIndicatorTopicPartition);
            topics.add(secondaryIndicatorTopicPartition);
            topics.add(emaFirstFieldTopicP);
            topics.add(emaSecondFieldTopicP);
            topics.add(filterTopicP);
            consumer.assign(topics);


            Map<String, Object> filterMap = getFilterFromApi(pair, filterInterval, filterType);
            double filter = (double) filterMap.get("value");
            long filterOpenTime = (long) filterMap.get("open_time");

            Map<String, Object> previousPrimaryBuySignalMap = getMcFromApi(pair, primary);
            String previousPrimaryBuySignal = (String) previousPrimaryBuySignalMap.get("value");
            long primaryBuySignalOpenTime = (long) previousPrimaryBuySignalMap.get("open_time");
            String primaryBuySignal = previousPrimaryBuySignal;
            boolean primaryBuySignalSwitch = false;


            Map<String, Object> secondaryBuySignalMap = getMcFromApi(pair, secondary);
            String secondaryBuySignal = (String) secondaryBuySignalMap.get("value");
            long secondaryBuySignalOpenTime =  (long) secondaryBuySignalMap.get("open_time");


            double emaFirst;
            long emaFirstOpenTime;
            long lastSignalSwitchOpenTime=0L;

            if (emaFirstField.contains("Open")) {
                JSONArray latestKline = getLatestKlineInterval(pair, emaInterval, 1);
                JSONObject latestKlineJ = (JSONObject) latestKline.get(0);
                emaFirst = Double.parseDouble(latestKlineJ.get("open").toString());
                emaFirstOpenTime = (long) latestKlineJ.get("open_time");
            } else {
                JSONObject emaFirstJ = getEmafromApi(pair, emaInterval, Integer.parseInt(emaFirstField.replace("EMA", "")));
                emaFirst = Double.parseDouble(emaFirstJ.get("latest_ema").toString());
                emaFirstOpenTime = (long) emaFirstJ.get("open_time");
            }
            JSONObject emaSecondJ = getEmafromApi(pair, emaInterval, Integer.parseInt(emaSecondField.replace("EMA", "")));
            double emaSecond = Double.parseDouble(emaSecondJ.get("latest_ema").toString());
            long emaSecondOpenTime = (long) emaSecondJ.get("open_time");


            boolean primaryBuySignalAgeValidator = false;
            boolean secondaryBuySignalAgeValidator = false;
            boolean emaFirstAgeValidator = false;
            boolean emaSecondAgeValidator = false;
            boolean filterAgeValidator = false;
            double gracePeriod= 2.0;
            JSONArray recordJA = null;
            JSONObject recordJ = null;


            logger.info(String.format("%n%n%n##INIT DATA POINTS For Strategy: %s##%n%n%n" +
                    "previousPrimaryBuySignal :%s%n" +
                    "previousPrimaryBuySignalOpen :%s%n" +
                    "previousSecondaryBuySignal :%s%n" +
                    "previousSecondaryBuySignalOpen :%s%n" +
                    "emaFirst: %s%n" +
                    "emaFirstOpen: %s%n" +
                    "emaSecond: %s%n" +
                    "emaSecondOpen: %s%n" +
                    "filter %s(%s): %s%n" +
                    "filterOpenTime: %s%n" +
                    "%n%n",strategyName, previousPrimaryBuySignal, primaryBuySignalOpenTime,secondaryBuySignal, secondaryBuySignalOpenTime, emaFirst, emaFirstOpenTime, emaSecond,emaSecondOpenTime, filterType, filterInterval, filter,filterOpenTime));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info(String.format("%s | %s | %s%n",Instant.now().toEpochMilli(), record.topic(), record.value()));


//                    Handle First Ema being an Open price (JSONArray)
                    if (Objects.equals(record.topic(), emaFirstFieldTopic) && emaFirstField.contains("Open")) {
                        recordJA = new JSONArray(record.value());
                    } else {
                        recordJ = new JSONObject(record.value());
                    }


//                    Fetch indicators

                    if (Objects.equals(record.topic(), filterTopic)) {
                        filterOpenTime = (long) recordJ.get("open_time");
                        filter = Double.parseDouble(recordJ.get("value").toString());
                    }

                    if (Objects.equals(record.topic(), primaryIndicatorTopic)) {
                        primaryBuySignalOpenTime = (long) recordJ.get("open_time");
                        primaryBuySignal = (String) recordJ.get("value");
                    }

                    if (Objects.equals(record.topic(), secondaryIndicatorTopic)) {
                        secondaryBuySignalOpenTime = (long) recordJ.get("open_time");
                        secondaryBuySignal = (String) recordJ.get("value");
                    }

                    if (Objects.equals(record.topic(), emaFirstFieldTopic)) {
                        if (emaFirstField.contains("Open")) {
                            emaFirstOpenTime = (long) recordJA.get(0);
                            emaFirst = Double.parseDouble(recordJA.get(1).toString());
                        } else {
                            emaFirstOpenTime = (long) recordJ.get("open_time");
                            emaFirst = Double.parseDouble(recordJ.get("value").toString());
                        }
                    }

                    if (Objects.equals(record.topic(), emaSecondFieldTopic)) {
                        emaSecondOpenTime = (long) recordJ.get("open_time");
                        emaSecond = Double.parseDouble(recordJ.get("value").toString());
                    }

//                    Detect Primary buy signal switch
                    if (!Objects.equals(previousPrimaryBuySignal, primaryBuySignal)) {
                        primaryBuySignalSwitch = (strategyType.equals("Long") && Objects.equals(primaryBuySignal, "G")) || (strategyType.equals("Short") && Objects.equals(primaryBuySignal, "R"));
                        previousPrimaryBuySignal = primaryBuySignal;
                        lastSignalSwitchOpenTime = primaryBuySignalOpenTime;
                        if (primaryBuySignalSwitch) {
                            logger.info(String.format("primaryBuySignalSwitch (%s) Detected",strategyName));
                        }
                    }

//                    check if filter is within range or null
                    if ((filter > filterRangeS && filter < filterRangeE) || filterType == null) {

                        if (Objects.equals(record.topic(), filterTopic)) {
                            logger.info(String.format("Filter %s is within range [%s,%s]%n", filterType, filterRangeS, filterRangeE));
                        }

//                        Get Age validators
                        primaryBuySignalAgeValidator =  ((Instant.now().toEpochMilli()-lastSignalSwitchOpenTime) / 1000.0) - (primary*60*2) < gracePeriod ;
                        secondaryBuySignalAgeValidator =  ((Instant.now().toEpochMilli()-secondaryBuySignalOpenTime) / 1000.0) - (secondary*60*2) < gracePeriod ;
                        emaFirstAgeValidator =  ((Instant.now().toEpochMilli()-emaFirstOpenTime) / 1000.0) - (emaInterval*60*2) < gracePeriod ;
                        emaSecondAgeValidator =  ((Instant.now().toEpochMilli()-emaSecondOpenTime) / 1000.0) - (emaInterval*60*2) < gracePeriod ;
                        filterAgeValidator =  ((Instant.now().toEpochMilli()-filterOpenTime) / 1000.0) - (filterInterval*60*2) < gracePeriod ;

//                        Collect current datapoints
                        HashMap<String, Object> currentDatapoints = new HashMap<>();
                        currentDatapoints.put("primaryBuySignal", primaryBuySignal);
                        currentDatapoints.put("primaryBuySignalOpenTime", primaryBuySignalOpenTime);
                        currentDatapoints.put("secondaryBuySignal", secondaryBuySignal);
                        currentDatapoints.put("secondaryBuySignalOpenTime", secondaryBuySignalOpenTime);
                        currentDatapoints.put("emaFirst", emaFirst);
                        currentDatapoints.put("emaSecond", emaSecond);
                        currentDatapoints.put("emaFirstOpenTime", emaFirstOpenTime);
                        currentDatapoints.put("emaSecondOpenTime", emaSecondOpenTime);
                        currentDatapoints.put("filter", filter);
                        currentDatapoints.put("filterOpenTime", filterOpenTime);
                        currentDatapoints.put("primaryBuySignalAgeValidator", primaryBuySignalAgeValidator);
                        currentDatapoints.put("secondaryBuySignalAgeValidator", secondaryBuySignalAgeValidator);
                        currentDatapoints.put("emaFirstAgeValidator", emaFirstAgeValidator);
                        currentDatapoints.put("emaSecondAgeValidator", emaSecondAgeValidator);
                        currentDatapoints.put("filterAgeValidator", filterAgeValidator);
//                    Check if the Strategy is active

                        if (primaryBuySignalSwitch  && primaryBuySignalAgeValidator && secondaryBuySignalAgeValidator && emaFirstAgeValidator && emaSecondAgeValidator && filterAgeValidator) {

                            if (strategyType.equals("Long")) {
                                if (Objects.equals(secondaryBuySignal, "G") && compareEmas(emaFirst, emaSecond, emaOperator)) {
                                    pushStrategy(strategyId, strategyName, producer, pair, primary, currentDatapoints.toString());
                                    logger.info(String.format("%s | STRATEGY PUSHED %s%n", Instant.now().toEpochMilli(), strategyName));
                                }
                            } else {
                                if (Objects.equals(secondaryBuySignal, "R") && compareEmas(emaFirst, emaSecond, emaOperator)) {
                                    pushStrategy(strategyId, strategyName, producer, pair, primary, currentDatapoints.toString());
                                    logger.info(String.format("%s | STRATEGY PUSHED %s%n", Instant.now().toEpochMilli(), strategyName));
                                }
                            }
                            primaryBuySignalSwitch = false;
                        }else{
                            if (primaryBuySignalSwitch){
                                logger.info(String.format("One of the age validators was off Current data points:%s%n",currentDatapoints.toString()));
                                FileWriter fw = new FileWriter(String.valueOf(String.format(Logpath,"AgeValidators")),true);
                                fw.write(strategyId+"\n");
                                fw.close();
                            }
                        }

                    } else {
                        if (Objects.equals(record.topic(), primaryIndicatorTopic)) {
                            logger.info(String.format("(%s) %s isn't within the range [%s,%s] (%s)%n",strategyName, filterType, filterRangeS, filterRangeE, filter));
                        }
                    }

                }
            }
        } catch (Exception e) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(String.format(Logpath,strategyId))));
            writer.write(ExceptionUtils.getStackTrace(e));
            writer.close();
        }

    }

    private static void pushStrategy(int strategyId, String strategyName, KafkaProducer producer, String pair, int primary,String currentDatapoints) {
        JSONObject strategyJ = new JSONObject();
        strategyJ.put("open_time", Instant.now().toEpochMilli());
        strategyJ.put("value", strategyId);
        strategyJ.put("strategyName", strategyName);
        strategyJ.put("currentDatapoints", currentDatapoints);
        producer.send(new ProducerRecord<>(String.format("TRIGGERED_STRATEGIES", pair, primary), strategyId, strategyJ.toString()));
    }

    private static boolean compareEmas(double emaFirst, double emaSecond, String emaOperator) throws Exception {

        if (Objects.equals(emaOperator, ">=")) {
            return emaFirst >= emaSecond;
        } else if (Objects.equals(emaOperator, "<=")) {
            return emaFirst <= emaSecond;
        } else if (Objects.equals(emaOperator, "==")) {
            return emaFirst == emaSecond;
        } else if (Objects.equals(emaOperator, ">")) {
            return emaFirst > emaSecond;
        } else if (Objects.equals(emaOperator, "<")) {
            return emaFirst < emaSecond;
        } else {
            throw new Exception(String.format("emaOperator Unorganized (%s)", emaOperator));
        }
    }
}