
package com.tradingbot.kafka.serde;



import com.tradingbot.kafka.serde.JsonDeserializer;
import com.tradingbot.kafka.serde.JsonSerializer;
import com.tradingbot.types.Kline;
import com.tradingbot.types.Trade;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory class for Serdes
 */
public class AppSerdes extends Serdes {


    static final class TradeSerde extends WrapperSerde<Trade> {
        TradeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<Trade> Trades() {
        TradeSerde serde = new TradeSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put("specific.class.name", Trade.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    public static final class KlineSerde extends WrapperSerde<Kline> {
        KlineSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<Kline> Klines() {
        KlineSerde kserde = new KlineSerde();

        Map<String, Object> kserdeConfigs = new HashMap<>();
        kserdeConfigs.put("specific.class.name", Kline.class);
        kserde.configure(kserdeConfigs, false);

        return kserde;
    }

}
