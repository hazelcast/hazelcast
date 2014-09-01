package com.hazelcast.map.impl;

import com.hazelcast.map.impl.tx.TransactionalMapProxy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.transaction.impl.TransactionSupport;

/**
 * Defines transactional service behavior of map service.
 *
 * @see MapService
 */
class MapTransactionalService implements TransactionalService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public MapTransactionalService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @SuppressWarnings("unchecked")
    @Override
    public TransactionalMapProxy createTransactionalObject(String name, TransactionSupport transaction) {
        return new TransactionalMapProxy(name, mapServiceContext.getService(), nodeEngine, transaction);
    }

    @Override
    public void rollbackTransaction(String transactionId) {

    }
}
