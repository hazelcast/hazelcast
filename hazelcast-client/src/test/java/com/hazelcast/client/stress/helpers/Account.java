package com.hazelcast.client.stress.helpers;

import java.io.Serializable;
import java.util.Arrays;

public class Account implements Serializable {

    private int acountNumber = -1;

    private byte[] someData = new byte[1024];

    private long balance = -1;

    public Account(int acountNumber, long balance){
        this.acountNumber = acountNumber;
        this.balance = balance;
    }

    public Account(Account acount){
        this(acount.acountNumber, acount.balance);
    }

    public int getAcountNumber(){
        return acountNumber;
    }

    public long getBalance(){
        return balance;
    }

    public void increase(long amount){
        balance+=amount;
    }

    public void decrease(long amount){
        balance-=amount;
    }

    public TransferRecord transferTo(Account to, long amount){

        TransferRecord tr = new TransferRecord(this, to, amount);

        if ( acountNumber == to.acountNumber){
            tr.setReason("same account");
        }
        else if ( balance < amount ){
            tr.setReason("to low");
        }
        else {
            this.decrease(amount);
            to.increase(amount);
            tr.setDecliened(false);
            tr.setReason("ok");
        }

        return  tr;
    }

    @Override
    public String toString() {
        return "Account{" +
                "acountNumber=" + acountNumber +
                ", balance=" + balance +
                '}';
    }
}
