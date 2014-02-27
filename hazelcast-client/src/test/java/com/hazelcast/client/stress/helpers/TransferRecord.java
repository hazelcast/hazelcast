package com.hazelcast.client.stress.helpers;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class TransferRecord implements Serializable{

    public final static AtomicLong ID_GENERATOR = new AtomicLong(1);

    private long id;
    private Acount from;
    private Acount to;
    private double amount;

    private boolean accepted;

    public TransferRecord(Acount form, Acount to, double amount){
        id = ID_GENERATOR.getAndIncrement();
        this.from = form;
        this.to = to;
        this.amount = amount;
    }

    public Acount getFrom() {
        return from;
    }

    public void setFrom(Acount from) {
        this.from = from;
    }

    public Acount getTo() {
        return to;
    }

    public void setTo(Acount to) {
        this.to = to;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public void setAccepted(boolean accepted) {
        this.accepted = accepted;
    }

    public long getId(){
        return id;
    }
}
