package com.github.connectors.pojo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * log 实体类，数据格式类似这样：{"lt": "click", "plat": "app", "timestamp": 1597980113000, "user_id": 1}
 */
public class Log {

    private String plat;
    private String lt;
    @JsonProperty(value = "user_id") private int userId;
    private long timestamp;

    public String getPlat() {
        return plat;
    }

    public void setPlat(String plat) {
        this.plat = plat;
    }

    public String getLt() {
        return lt;
    }

    public void setLt(String lt) {
        this.lt = lt;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public static Log of(String plat, String lt, int userId, long timestamp) {
        Log log = new Log();
        log.setPlat(plat);
        log.setLt(lt);
        log.setUserId(userId);
        log.setTimestamp(timestamp);

        return log;
    }

    @Override
    public String toString() {
        return "Log{" +
                "plat='" + plat + '\'' +
                ", lt='" + lt + '\'' +
                ", userId=" + userId +
                ", timestamp=" + timestamp +
                '}';
    }
}
