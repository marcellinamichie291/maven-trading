package com.tradingbot.types;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "first trade time",
        "open",
        "high",
        "low",
        "close",
        "volume"
})
@Generated("jsonschema2pojo")
public class Kline {

    @JsonProperty("nbrOrders")
    private int nbrOrders = 0;
    @JsonProperty("open_time")
    private Long openTime = 0L;
    @JsonProperty("open")
    private Double open = 0.0D;
    @JsonProperty("high")
    private Double high = 0.0D;
    @JsonProperty("low")
    private Double low = 1000000.0D;
    @JsonProperty("close")
    private Double close = 0.0D;
    @JsonProperty("volume")
    private Double volume = 0.0D;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("nbrOrders")
    public int getnbrOrders() {
        return nbrOrders;
    }

    @JsonProperty("nbrOrders")
    public void setnbrOrders(int nbrOrders) {
        this.nbrOrders = nbrOrders;
    }

    @JsonProperty("first trade time")
    public Long getOpenTime() { return openTime; }

    @JsonProperty("first trade time")
    public void setOpenTime(Long openTime) {this.openTime = openTime;}

    @JsonProperty("open")
    public Double getOpen() {
        return open;
    }

    @JsonProperty("open")
    public void setOpen(Double open) {
        this.open = open;
    }

    @JsonProperty("high")
    public Double getHigh() {
        return high;
    }

    @JsonProperty("high")
    public void setHigh(Double high) {
        this.high = high;
    }

    @JsonProperty("low")
    public Double getLow() {
        return low;
    }

    @JsonProperty("low")
    public void setLow(Double low) {
        this.low = low;
    }

    @JsonProperty("close")
    public Double getClose() {
        return close;
    }

    @JsonProperty("close")
    public void setClose(Double close) {
        this.close = close;
    }

    @JsonProperty("volume")
    public Double getVolume() {
        return volume;
    }

    @JsonProperty("volume")
    public void setVolume(Double volume) {
        this.volume = volume;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}