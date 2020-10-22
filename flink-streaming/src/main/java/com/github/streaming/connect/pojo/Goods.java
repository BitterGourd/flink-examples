package com.github.streaming.connect.pojo;

/**
 * 商品信息
 */
public class Goods {

    /** 商品id */
    public int goodsId;

    /** 价格 */
    public String goodsName;

    /**
     * 当前商品是否被下架，如果下架应该从 State 中去移除
     * true 表示下架
     * false 表示上架
     */
    public boolean isRemove;
}
