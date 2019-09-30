package org.apache.ignite.yardstick.user;

public class Trade {
    private Long tradeId;
    private Integer tid;
    private Double quantity;
    private Double price;

    public Long getTradeId() {
        return tradeId;
    }

    public Integer getTid() {
        return tid;
    }

    public Double getQuantity() {
        return quantity;
    }

    public Double getPrice() {
        return price;
    }

    public Trade(Long tradeId, Integer tid, Double quantity, Double price) {
        this.tradeId = tradeId;
        this.tid = tid;
        this.quantity = quantity;
        this.price = price;
    }
}
