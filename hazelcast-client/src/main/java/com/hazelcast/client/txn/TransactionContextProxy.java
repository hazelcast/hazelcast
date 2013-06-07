package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.connection.Connection;
import com.hazelcast.core.*;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;

import java.io.IOException;

/**
 * @ali 6/6/13
 */
public class TransactionContextProxy implements TransactionContext {

    final HazelcastClient client;
    final TransactionProxy transaction;

    public TransactionContextProxy(HazelcastClient client, TransactionOptions options) {
        this.client = client;
        final Connection connection;
        connection = getConnection();
        if (connection == null){
            throw new HazelcastException("Could not obtain Connection!!!");
        }
        this.transaction = new TransactionProxy(client, options);
    }

    public String getTxnId() {
        return transaction.getTxnId();
    }

    public void beginTransaction() {

    }

    public void commitTransaction() throws TransactionException {

    }

    public void rollbackTransaction() {

    }

    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return null;
    }

    public <E> TransactionalQueue<E> getQueue(String name) {
        return null;
    }

    public <K, V> TransactionalMultiMap<K, V> getMultiMap(String name) {
        return null;
    }

    public <E> TransactionalList<E> getList(String name) {
        return null;
    }

    public <E> TransactionalSet<E> getSet(String name) {
        return null;
    }

    public <T extends TransactionalObject> T getTransactionalObject(String serviceName, Object id) {
        return null;
    }

    private Connection getConnection() {
        Connection conn = null;
        for (int i=0; i<5; i++){
            try {
                conn = client.getConnectionManager().getRandomConnection();
            } catch (IOException e) {
                continue;
            }
            break;
        }
        return conn;
    }
}
