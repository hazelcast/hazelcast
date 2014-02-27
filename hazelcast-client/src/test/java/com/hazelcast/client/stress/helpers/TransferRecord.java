package com.hazelcast.client.stress.helpers;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class TransferRecord implements Serializable{

    public final static AtomicLong ID_GENERATOR = new AtomicLong(1);

    private long id;
    private Account from;
    private Account to;
    private long amount;

    private boolean decliened;

    private String reason;

    public TransferRecord(Account form, Account to, long amount){
        id = ID_GENERATOR.getAndIncrement();
        this.from = form;
        this.to = to;
        this.amount = amount;
    }

    public Account getFrom() {
        return from;
    }

    public void setFrom(Account from) {
        this.from = from;
    }

    public Account getTo() {
        return to;
    }

    public void setTo(Account to) {
        this.to = to;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public boolean isDecliened() {
        return decliened;
    }

    public void setDecliened(boolean decliened) {
        this.decliened = decliened;
    }

    public long getId(){
        return id;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

}
