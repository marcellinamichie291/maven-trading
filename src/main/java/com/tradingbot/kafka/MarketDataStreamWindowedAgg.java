package com.tradingbot.kafka;


import com.tradingbot.kafka.serde.AppSerdes;
import com.tradingbot.kafka.utils.TradeTimeExtractor;
import com.tradingbot.types.Kline;
import com.tradingbot.types.Trade;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.logging.Logger;

import static com.tradingbot.kafka.AppConfigs.Logpath;
import static java.lang.Math.max;
import static java.lang.Math.min;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.json.JSONObject;

public class MarketDataStreamWindowedAgg {


    public static void run(JSONObject params) throws IOException {
        try {
            String pair = (String) params.get("pair");
            String topicName = pair + "_TRADES";
            String bootstrapServers = (String) params.get("bootstrapServers");

//        Create Stream properties
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

//        Init stream
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, Trade> kStream = builder.stream(topicName,
                    Consumed.with(AppSerdes.String(), AppSerdes.Trades())
                            .withTimestampExtractor(new TradeTimeExtractor())
            );

//      Group the trades
            KGroupedStream<String, Trade> KS1 = kStream.groupByKey(
                    Grouped.with(AppSerdes.String(),
                            AppSerdes.Trades()));

            TimeWindowedKStream<String, Trade> KS2 = KS1.windowedBy(
                    TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ofMillis(0))
            );
//        Windowed by 1 minute
            KS2.aggregate(
                            Kline::new, // initialize Kline data
                            (key, value, aggregate) -> {
                                aggregate.setnbrOrders(aggregate.getnbrOrders()+1);
                                double currentPrice = Double.parseDouble(value.getPrice()); // get price
                                double currentQuantity = Double.parseDouble(value.getQuantity()); // get quantity

                                aggregate.setVolume(aggregate.getVolume() + currentQuantity); // volume

                                double high = max(currentPrice, aggregate.getHigh()); // high
                                double low = min(currentPrice, aggregate.getLow()); // low
                                aggregate.setHigh(high);
                                aggregate.setLow(low);

                                if (aggregate.getOpen() == 0.0) {
                                    aggregate.setOpen(currentPrice);
                                } // open price

                                if (aggregate.getOpenTime() == 0L) {
                                    aggregate.setOpenTime(value.getTradeTime());
                                } // open time

                                aggregate.setClose(currentPrice); // close price

                                return aggregate;
                            },
                            Materialized.with(Serdes.String(), AppSerdes.Klines()))
//                .mapValues((stringWindowed, kline) -> )
                    .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())) // Supress until candle closes
                    .toStream()
                    .selectKey((k, v) -> k.window().start()) // Use Window start time as a key for the Kline
                    .to(pair + "_1M", Produced.with(Serdes.Long(), AppSerdes.Klines())); // Publish to the topic

//        Build the stream
            Topology topology = builder.build();

            KafkaStreams streams = new KafkaStreams(topology, props);

//        Start the stream
            streams.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close();
            }));
        } catch (Exception e) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(String.format(Logpath, "MarketDataStreamWindowedAgg"))));
            writer.write(ExceptionUtils.getStackTrace(e));
            writer.close();
        }

    }
}