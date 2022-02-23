/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy.txn;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueueOfferCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueuePeekCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueuePollCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueueSizeCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueueTakeCodec;
import com.hazelcast.client.impl.spi.ClientTransactionContext;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.internal.serialization.Data;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static java.lang.Thread.currentThread;

/**
 * Proxy implementation of {@link TransactionalQueue}.
 *
 * @param <E> the type of elements in this queue
 */
public class ClientTxnQueueProxy<E> extends ClientTxnProxy implements TransactionalQueue<E> {

    public ClientTxnQueueProxy(String name, ClientTransactionContext transactionContext) {
        super(name, transactionContext);
    }

    @Override
    public boolean offer(@Nonnull E e) {
        try {
            return offer(e, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e1) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean offer(@Nonnull E e, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        Data data = toData(e);
        ClientMessage request = TransactionalQueueOfferCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), data, unit.toMillis(timeout));
        ClientMessage response = invoke(request);
        return TransactionalQueueOfferCodec.decodeResponse(response);
    }

    @Nonnull
    @Override
    public E take() throws InterruptedException {
        ClientMessage request = TransactionalQueueTakeCodec.encodeRequest(name, getTransactionId(), getThreadId());
        ClientMessage response = invoke(request);
        return (E) toObject(TransactionalQueueTakeCodec.decodeResponse(response));
    }

    @Override
    public E poll() {
        try {
            return poll(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        }
    }

    @Override
    public E poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        ClientMessage request = TransactionalQueuePollCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), unit.toMillis(timeout));
        ClientMessage response = invoke(request);
        return (E) toObject(TransactionalQueuePollCodec.decodeResponse(response));
    }

    @Override
    public E peek() {
        try {
            return peek(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        }
    }

    @Override
    public E peek(long timeout, TimeUnit unit) throws InterruptedException {
        ClientMessage request = TransactionalQueuePeekCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), unit.toMillis(timeout));
        ClientMessage response = invoke(request);
        return (E) toObject(TransactionalQueuePeekCodec.decodeResponse(response));
    }

    @Override
    public int size() {
        ClientMessage request = TransactionalQueueSizeCodec.encodeRequest(name, getTransactionId(), getThreadId());
        ClientMessage response = invoke(request);
        return TransactionalQueueSizeCodec.decodeResponse(response);
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    void onDestroy() {
    }
}
