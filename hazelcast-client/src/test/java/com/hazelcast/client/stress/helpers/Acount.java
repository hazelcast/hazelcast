package com.hazelcast.client.stress.helpers;

import java.io.Serializable;

public class Acount implements Serializable {

    private int acountNumber;

    private byte[] someData = new byte[1024];

    private double balance = 0.0;

    public Acount(int acountNumber, double balance){
        this.acountNumber = acountNumber;
        this.balance = balance;
    }

    public int getAcountNumber(){
        return acountNumber;
    }

    public double getBalance(){
        return balance;
    }

    public void increase(double amount){
        balance+=amount;
    }

    public void decrease(double amount){
        balance-=amount;
    }

    public TransferRecord transferTo(Acount to, double amount){

        TransferRecord tr = new TransferRecord(this, to, amount);
        tr.setAccepted(false);

        if ( balance>=amount ) {

            this.decrease(amount);
            to.increase(amount);
            tr.setAccepted(true);
        }

        return  tr;
    }
}
