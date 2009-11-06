package com.hazelcast.client;

import com.hazelcast.core.Transaction;



public final class ThreadContext {
    private final static ThreadLocal<ThreadContext> threadLocal = new ThreadLocal<ThreadContext>();
    TransactionClientProxy transactionProxy = new TransactionClientProxy(null, null);
	boolean transaction;
	
	
    public static ThreadContext get() {
        ThreadContext threadContext = threadLocal.get();
        if (threadContext == null) {
            threadContext = new ThreadContext();
            threadLocal.set(threadContext);
        }
        return threadContext;
    }


	public Transaction getTransaction() {
		return transactionProxy;
	}

}
