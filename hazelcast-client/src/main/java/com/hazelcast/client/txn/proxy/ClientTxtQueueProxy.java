package com.hazelcast.client.txn.proxy;

import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.txn.TransactionContextProxy;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.client.TxnOfferRequest;
import com.hazelcast.queue.client.TxnPollRequest;
import com.hazelcast.queue.client.TxnSizeRequest;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @ali 6/7/13
 */
public class ClientTxtQueueProxy<E>  implements TransactionalQueue<E>{


    final String name;
    final TransactionContextProxy proxy;

    public ClientTxtQueueProxy(String name, TransactionContextProxy proxy) {
        this.name = name;
        this.proxy = proxy;
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
        TxnPollRequest request = new TxnPollRequest(name, unit.toMillis(timeout));
        return invoke(request);
    }

    public int size() {
        TxnSizeRequest request = new TxnSizeRequest(name);
        Integer result = invoke(request);
        return result;
    }

    public Object getId() {
        return name;
    }

    public void destroy() {
        //TODO
    }

    public String getName() {
        return name;
    }

    private <T> T invoke(Object request){
        final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl)proxy.getClient().getClientClusterService();
        try {
            return clusterService.sendAndReceiveFixedConnection(proxy.getConnection(), request);
        } catch (IOException e) {
            ExceptionUtil.rethrow(new HazelcastException(e));
        }
        return null;
    }
}
