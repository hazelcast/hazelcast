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

package com.hazelcast.queue.impl.tx;

import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

/**
 * Provides proxy for the Transactional Queue.
 *
 * @param <E>
 */
public class TransactionalQueueProxy<E> extends TransactionalQueueProxySupport implements TransactionalQueue<E> {

    public TransactionalQueueProxy(NodeEngine nodeEngine, QueueService service, String name, TransactionSupport tx) {
        super(nodeEngine, service, name, tx);
    }

    @Override
    public boolean offer(E e) {
        try {
            return offer(e, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            EmptyStatement.ignore(ignored);
        }
        return false;
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        checkTransactionState();
        Data data = getNodeEngine().toData(e);
        return offerInternal(data, unit.toMillis(timeout));
    }

    @Override
    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    @Override
    public E poll() {
        try {
            return poll(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            //todo: interrupt status swallowed
            EmptyStatement.ignore(ignored);
        }
        return null;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        checkTransactionState();
        Data data = pollInternal(unit.toMillis(timeout));
        return getNodeEngine().toObject(data);
    }

    @Override
    public E peek() {
        try {
            return peek(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            //todo: interrupt status swallowed
            EmptyStatement.ignore(ignored);
        }
        return null;
    }

    @Override
    public E peek(long timeout, TimeUnit unit) throws InterruptedException {
        checkTransactionState();
        Data data = peekInternal(unit.toMillis(timeout));
        return getNodeEngine().toObject(data);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TransactionalQueue{");
        sb.append("name=").append(name);
        sb.append('}');
        return sb.toString();
    }
}
