package com.tradingbot.kafka.utils;

import com.tradingbot.types.Trade;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Optional;

public class TradeTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
        Trade trade = (Trade) consumerRecord.value();
        return ((trade.getTradeTime() > 0) ? trade.getTradeTime() : prevTime);
    }
}
