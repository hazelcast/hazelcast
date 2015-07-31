/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy.txn;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueueOfferCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueuePeekCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueuePollCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueueSizeCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueueTakeCodec;
import com.hazelcast.client.spi.ClientTransactionContext;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ThreadUtil;

import java.util.concurrent.TimeUnit;

public class ClientTxnQueueProxy<E> extends ClientTxnProxy implements TransactionalQueue<E> {

    public ClientTxnQueueProxy(String name, ClientTransactionContext transactionContext) {
        super(name, transactionContext);
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
        ClientMessage request = TransactionalQueueOfferCodec.encodeRequest(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), data, unit.toMillis(timeout));
        ClientMessage response = invoke(request);
        return TransactionalQueueOfferCodec.decodeResponse(response).response;
    }

    @Override
    public E take() throws InterruptedException {
        ClientMessage request = TransactionalQueueTakeCodec.encodeRequest(getName(), getTransactionId(),
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        return (E) toObject(TransactionalQueueTakeCodec.decodeResponse(response).response);
    }

    public E poll() {
        try {
            return poll(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        ClientMessage request = TransactionalQueuePollCodec.encodeRequest(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), unit.toMillis(timeout));
        ClientMessage response = invoke(request);
        return (E) toObject(TransactionalQueuePollCodec.decodeResponse(response).response);
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
        ClientMessage request = TransactionalQueuePeekCodec.encodeRequest(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), unit.toMillis(timeout));
        ClientMessage response = invoke(request);
        return (E) toObject(TransactionalQueuePeekCodec.decodeResponse(response).response);
    }

    public int size() {
        ClientMessage request = TransactionalQueueSizeCodec.encodeRequest(getName(), getTransactionId(),
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        return TransactionalQueueSizeCodec.decodeResponse(response).response;
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
