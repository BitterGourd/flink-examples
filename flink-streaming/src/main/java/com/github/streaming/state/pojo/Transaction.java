package com.github.streaming.state.pojo;

public class Transaction {

    private long accountId;
    private double amount;

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public static Transaction of(long accountId, double amount) {
        Transaction transaction = new Transaction();
        transaction.setAccountId(accountId);
        transaction.setAmount(amount);

        return transaction;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "accountId=" + accountId +
                ", amount=" + amount +
                '}';
    }
}
