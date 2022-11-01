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
        "e",
        "E",
        "s",
        "T",
        "p",
        "q",
        "b",
        "a",
        "T",
        "m",
        "M"
})
@Generated("jsonschema2pojo")
public class Trade {

    @JsonProperty("e")
    private String eventType;
    @JsonProperty("E")
    private Long eventTime;
    @JsonProperty("s")
    private String symbol;
    @JsonProperty("T")
    private Integer tradeID;
    @JsonProperty("p")
    private String price;
    @JsonProperty("q")
    private String quantity;
    @JsonProperty("b")
    private Long buyerOrderID;
    @JsonProperty("a")
    private Long sellerOrderID;
    @JsonProperty("T")
    private Long tradeTime;
    @JsonProperty("m")
    private Boolean buyerTheMarketMaker;
    @JsonProperty("M")
    private Boolean ignore;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("e")
    public String getEventType() {
        return eventType;
    }

    @JsonProperty("e")
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @JsonProperty("E")
    public Long getEventTime() {
        return eventTime;
    }

    @JsonProperty("E")
    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @JsonProperty("s")
    public String getSymbol() {
        return symbol;
    }

    @JsonProperty("s")
    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    @JsonProperty("t")
    public Integer getTradeID() {
        return tradeID;
    }

    @JsonProperty("t")
    public void setTradeID(Integer tradeID) {
        this.tradeID = tradeID;
    }

    @JsonProperty("p")
    public String getPrice() {
        return price;
    }

    @JsonProperty("p")
    public void setPrice(String price) {
        this.price = price;
    }

    @JsonProperty("q")
    public String getQuantity() {
        return quantity;
    }

    @JsonProperty("q")
    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    @JsonProperty("b")
    public Long getBuyerOrderID() {
        return buyerOrderID;
    }

    @JsonProperty("b")
    public void setBuyerOrderID(Long buyerOrderID) {
        this.buyerOrderID = buyerOrderID;
    }

    @JsonProperty("a")
    public Long getSellerOrderID() {
        return sellerOrderID;
    }

    @JsonProperty("a")
    public void setSellerOrderID(Long sellerOrderID) {
        this.sellerOrderID = sellerOrderID;
    }

    @JsonProperty("T")
    public Long getTradeTime() {
        return tradeTime;
    }

    @JsonProperty("T")
    public void setTradeTime(Long tradeTime) {
        this.tradeTime = tradeTime;
    }

    @JsonProperty("m")
    public Boolean getBuyerTheMarketMaker() {
        return buyerTheMarketMaker;
    }

    @JsonProperty("m")
    public void setBuyerTheMarketMaker(Boolean buyerTheMarketMaker) {
        this.buyerTheMarketMaker = buyerTheMarketMaker;
    }

    @JsonProperty("M")
    public Boolean getIgnore() {
        return ignore;
    }

    @JsonProperty("M")
    public void setIgnore(Boolean ignore) {
        this.ignore = ignore;
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