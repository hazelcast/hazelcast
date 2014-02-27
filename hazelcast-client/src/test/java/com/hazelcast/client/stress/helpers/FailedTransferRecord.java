package com.hazelcast.client.stress.helpers;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class FailedTransferRecord implements Serializable{

    public final static AtomicLong ID_GENERATOR = new AtomicLong(1);

    private long id;
    private int from;
    private int to;
    private long amount;


    private String reason;

    public FailedTransferRecord(int from, int to, long amount){
        id = ID_GENERATOR.getAndIncrement();

        this.from = from;
        this.to = to;
        this.amount = amount;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getTo() {
        return to;
    }

    public void setTo(int to) {
        this.to = to;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
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


    @Override
    public String toString() {
        return "TransferRecord{" +
                "id=" + id +
                ", from=" + from +
                ", to=" + to +
                ", amount=" + amount +
                ", reason='" + reason + '\'' +
                '}';
    }

    public static class Comparator  implements java.util.Comparator<FailedTransferRecord>{

        @Override
        public int compare(FailedTransferRecord a, FailedTransferRecord b) {
            return (int) (a.id - b.id);
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }
    }
}
