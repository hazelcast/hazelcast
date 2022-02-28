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

package com.hazelcast.collection.impl.txnqueue;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.transaction.impl.Transaction;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.Thread.currentThread;

/**
 * Provides proxy for the Transactional Queue.
 *
 * @param <E> the type of elements in the queue.
 */
public class TransactionalQueueProxy<E> extends TransactionalQueueProxySupport<E> {

    public TransactionalQueueProxy(NodeEngine nodeEngine, QueueService service, String name, Transaction tx) {
        super(nodeEngine, service, name, tx);
    }

    @Override
    public boolean offer(@Nonnull E e) {
        try {
            return offer(e, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            currentThread().interrupt();
        }
        return false;
    }

    @Override
    public boolean offer(@Nonnull E e, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        checkNotNull(e, "Offered item should not be null.");
        checkNotNull(unit, "TimeUnit should not be null.");

        checkTransactionState();
        Data data = getNodeEngine().toData(e);
        return offerInternal(data, unit.toMillis(timeout));
    }

    @Nonnull
    @Override
    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    @Override
    public E poll() {
        try {
            return poll(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            currentThread().interrupt();
        }
        return null;
    }

    @Override
    public E poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        checkNotNull(unit, "TimeUnit should not be null.");

        checkTransactionState();
        Data data = pollInternal(unit.toMillis(timeout));
        return (E) toObjectIfNeeded(data);
    }

    @Override
    public E peek() {
        try {
            return peek(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            currentThread().interrupt();
        }
        return null;
    }

    @Override
    public E peek(long timeout, TimeUnit unit) throws InterruptedException {
        checkNotNull(unit, "TimeUnit should not be null.");

        checkTransactionState();
        Data data = peekInternal(unit.toMillis(timeout));
        return (E) toObjectIfNeeded(data);
    }

    @Override
    public String toString() {
        return "TransactionalQueue{name=" + name + '}';
    }
}
