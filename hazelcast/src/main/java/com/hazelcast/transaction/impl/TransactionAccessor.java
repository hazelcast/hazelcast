package com.hazelcast.transaction.impl;

import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.transaction.TransactionContext;

/**
 * @author mdogan 7/11/13
 */
@PrivateApi
public final class TransactionAccessor {

    public static Transaction getTransaction(TransactionContext ctx) {
        if (ctx instanceof TransactionContextImpl) {
            TransactionContextImpl ctxImpl = (TransactionContextImpl) ctx;
            return ctxImpl.getTransaction();
        }
        throw new IllegalArgumentException();
    }

    private TransactionAccessor() {}
}
