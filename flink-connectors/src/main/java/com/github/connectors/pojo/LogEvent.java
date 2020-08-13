package com.github.connectors.pojo;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

public class LogEvent implements Serializable {

    private static final long serialVersionUID = 9203611369444569939L;

    private String lt;
    private String timestamp;
    private String event;

    public LogEvent() {
    }

    public LogEvent(String lt, String timestamp, String event) {
        this.lt = lt;
        this.timestamp = timestamp;
        this.event = event;
    }

    public String getLt() {
        return lt;
    }

    public void setLt(String lt) {
        this.lt = lt;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        LogEvent logEvent = (LogEvent) o;

        return new EqualsBuilder()
                .append(lt, logEvent.lt)
                .append(timestamp, logEvent.timestamp)
                .append(event, logEvent.event)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(lt)
                .append(timestamp)
                .append(event)
                .toHashCode();
    }

    @Override
    public String toString() {
        return event;
    }
}
