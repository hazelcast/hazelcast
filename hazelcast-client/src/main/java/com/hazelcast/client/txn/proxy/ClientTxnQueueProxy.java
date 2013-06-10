package com.hazelcast.client.txn.proxy;

import com.hazelcast.client.txn.TransactionContextProxy;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.client.TxnOfferRequest;
import com.hazelcast.queue.client.TxnPollRequest;
import com.hazelcast.queue.client.TxnSizeRequest;

import java.util.concurrent.TimeUnit;

/**
 * @ali 6/7/13
 */
public class ClientTxnQueueProxy<E> extends ClientTxnProxy implements TransactionalQueue<E>{


    public ClientTxnQueueProxy(String name, TransactionContextProxy proxy) {
        super(name, proxy);
    }

    public boolean offer(E e) {
        try {
            return offer(e, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e1) {
            return false;
        }
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        final Data data = proxy.getClient().getSerializationService().toData(e);
        TxnOfferRequest request = new TxnOfferRequest(getName(), unit.toMillis(timeout), data);
        Boolean result = invoke(request);
        return result;
    }

    public E poll() {
        try {
            return poll(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        TxnPollRequest request = new TxnPollRequest(getName(), unit.toMillis(timeout));
        return invoke(request);
    }

    public int size() {
        TxnSizeRequest request = new TxnSizeRequest(getName());
        Integer result = invoke(request);
        return result;
    }

    public String getName() {
        return (String)getId();
    }


    void onDestroy() {
        //TODO
    }
}
