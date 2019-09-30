package org.apache.ignite.yardstick.user;

public class Position {
    private Integer tid;
    private Double quantity;

    public Position(Integer tid, Double quantity) {
        this.tid = tid;
        this.quantity = quantity;
    }


    public Integer getTid() {
        return tid;
    }

    public Double getQuantity() {
        return quantity;
    }

    public void setQuantity(Double quantity) {
        this.quantity = quantity;
    }

}
