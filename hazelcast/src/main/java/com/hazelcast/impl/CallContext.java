package com.hazelcast.impl;


public class CallContext {
	TransactionImpl txn = null;
	int threadId = Thread.currentThread().hashCode();

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
}
