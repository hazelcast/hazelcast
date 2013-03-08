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

package com.hazelcast.queue.tx;

import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionException;

import java.util.concurrent.TimeUnit;

/**
 * @mdogan 3/8/13
 */
public class TransactionalQueueProxy<E> extends TransactionalQueueProxySupport implements TransactionalQueue<E> {

    public TransactionalQueueProxy(String name, NodeEngine nodeEngine, QueueService service, Transaction tx) {
        super(name, nodeEngine, service, tx);
    }

    public boolean offer(E e) throws TransactionException {
        try {
            return offer(e, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e1) {
            return false;
        }
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException, TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(e);
        return offerInternal(data, timeout, unit);
    }

    public E poll() throws TransactionException {
        try {
            return poll(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException, TransactionException {
        return getNodeEngine().toObject(pollInternal(timeout, unit));
    }

    public E peek() throws TransactionException {
        return getNodeEngine().toObject(peekInternal());
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }
}
