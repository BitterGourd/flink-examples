package com.github.streaming.state.pojo;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Pattern {

    private String firstAction;
    private String secondAction;

    public Pattern() {
    }

    public Pattern(String firstAction, String secondAction) {
        this.firstAction = firstAction;
        this.secondAction = secondAction;
    }

    public String getFirstAction() {
        return firstAction;
    }

    public String getSecondAction() {
        return secondAction;
    }

    public void setFirstAction(String firstAction) {
        this.firstAction = firstAction;
    }

    public void setSecondAction(String secondAction) {
        this.secondAction = secondAction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Pattern pattern = (Pattern) o;

        return new EqualsBuilder()
                .append(firstAction, pattern.firstAction)
                .append(secondAction, pattern.secondAction)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(firstAction)
                .append(secondAction)
                .toHashCode();
    }

    @Override
    public String toString() {
        return String.format("Pattern{firstAction=%s, secondAction=%s}", this.firstAction, this.secondAction);
    }
}
