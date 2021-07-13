package org.apache.flink.sql;

import java.io.Serializable;

public class ShippingPriorityItem implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long orderkey;
    private Double revenue;
    private String orderdate;
    private String shippriority;

    public ShippingPriorityItem(Long orderkey, Double revenue, String orderdate, String shippriority) {
        this.orderkey = orderkey;
        this.revenue = revenue;
        this.orderdate = orderdate;
        this.shippriority = shippriority;
    }

    public Long getOrderkey() {
        return orderkey;
    }

    public void setOrderkey(Long orderkey) {
        this.orderkey = orderkey;
    }

    public Double getRevenue() {
        return revenue;
    }

    public void setRevenue(Double revenue) {
        this.revenue = revenue;
    }

    public String getOrderdate() {
        return orderdate;
    }

    public void setOrderdate(String orderdate) {
        this.orderdate = orderdate;
    }

    public String getShippriority() {
        return shippriority;
    }

    public void setShippriority(String shippriority) {
        this.shippriority = shippriority;
    }

    @Override
    public String toString() {
        return "ShippingPriorityItem [orderkey=" + orderkey + ", revenue=" + revenue + ", orderdate=" + orderdate
                + ", shippriority=" + shippriority + "]";
    }
}
