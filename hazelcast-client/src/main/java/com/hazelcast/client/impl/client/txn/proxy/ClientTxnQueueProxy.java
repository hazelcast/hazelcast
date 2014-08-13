/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.client.txn.proxy;

import com.hazelcast.client.impl.client.txn.TransactionContextProxy;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.queue.impl.client.TxnOfferRequest;
import com.hazelcast.queue.impl.client.TxnPeekRequest;
import com.hazelcast.queue.impl.client.TxnPollRequest;
import com.hazelcast.queue.impl.client.TxnSizeRequest;


import java.util.concurrent.TimeUnit;

/**
 * @author ali 6/7/13
 */
public class ClientTxnQueueProxy<E> extends ClientTxnProxy implements TransactionalQueue<E> {

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
        final Data data = toData(e);
        TxnOfferRequest request = new TxnOfferRequest(getName(), unit.toMillis(timeout), data);
        Boolean result = invoke(request);
        return result;
    }

    @Override
    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
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

    @Override
    public E peek() {
        try {
            return peek(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public E peek(long timeout, TimeUnit unit) throws InterruptedException {
        TxnPeekRequest request = new TxnPeekRequest(getName(), unit.toMillis(timeout));
        return invoke(request);
    }

    public int size() {
        TxnSizeRequest request = new TxnSizeRequest(getName());
        Integer result = invoke(request);
        return result;
    }

    public String getName() {
        return (String) getId();
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    void onDestroy() {
    }
}
