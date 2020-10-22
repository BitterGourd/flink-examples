package com.github.streaming.connect.pojo;

/**
 * 订单交易
 */
public class Order {

    /** 订单发生的时间 */
    public long time;

    /** 订单 id */
    public String orderId;

    /** 用户id */
    public String userId;

    /** 商品id */
    public int goodsId;

    /** 价格 */
    public int price;

    /** 城市 */
    public int cityId;

    @Override
    public String toString() {
        return "Order{" +
                "orderId=" + orderId +
                ", time=" + time +
                '}';
    }
}
