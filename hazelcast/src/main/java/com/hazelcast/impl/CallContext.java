package com.hazelcast.impl;


public class CallContext {
    TransactionImpl txn = null;
    final int threadId;
    final boolean client;

    public CallContext(int threadId, boolean client) {
        this.threadId = threadId;
        this.client = client;
    }

    public TransactionImpl getTxn() {
        return txn;
    }

    public void setTxn(TransactionImpl txn) {
        this.txn = txn;
    }

    public void setTransaction(TransactionImpl txn) {
        this.txn = txn;

    }

    public void finalizeTxn() {
        txn = null;
    }

    public long getTxnId() {
        return (txn == null) ? -1L : txn.getId();
    }

    public int getThreadId() {
        return threadId;
    }

    public boolean isClient() {
        return client;
    }
}
