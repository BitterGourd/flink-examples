package com.github.streaming.state.pojo;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Action {

    private long userId;
    private String action;

    public Action() {
    }

    public Action(long userId, String action) {
        this.userId = userId;
        this.action = action;
    }

    public long getUserId() {
        return userId;
    }

    public String getAction() {
        return action;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Action action1 = (Action) o;

        return new EqualsBuilder()
                .append(userId, action1.userId)
                .append(action, action1.action)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(userId)
                .append(action)
                .toHashCode();
    }

    @Override
    public String toString() {
        return String.format("Action{userId=%s, action=%s}", this.userId, this.action);
    }
}
