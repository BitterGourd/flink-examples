package com.github.streaming.state.pojo;

public class Alert {

    private long accountId;

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "accountId=" + accountId +
                '}';
    }
}
