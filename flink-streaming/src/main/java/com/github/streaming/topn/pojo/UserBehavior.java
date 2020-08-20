package com.github.streaming.topn.pojo;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * 用户行为
 */
public class UserBehavior {

    /** 用户ID */
    private long userId;
    /** 商品ID */
    private long itemId;
    /** 商品类目ID */
    private int categoryId;
    /** 用户行为, 包括("pv", "buy", "cart", "fav") */
    private String behavior;
    /** 行为发生的时间戳，单位秒 */
    private long timestamp;

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        UserBehavior that = (UserBehavior) o;

        return new EqualsBuilder()
                .append(userId, that.userId)
                .append(itemId, that.itemId)
                .append(categoryId, that.categoryId)
                .append(timestamp, that.timestamp)
                .append(behavior, that.behavior)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(userId)
                .append(itemId)
                .append(categoryId)
                .append(behavior)
                .append(timestamp)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
