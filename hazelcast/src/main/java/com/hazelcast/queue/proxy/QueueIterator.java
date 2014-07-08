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

package com.hazelcast.queue.proxy;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import com.hazelcast.queue.QueueService;
import com.hazelcast.queue.RemoveOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import java.util.Iterator;

/**
 * @author ali 12/18/12
 */
public class QueueIterator<E> implements Iterator<E> {

    private final Iterator<Data> iterator;
    private NodeEngine nodeEngine;
    private final int partitionId;
    private final String name;
    private final SerializationService serializationService;
    private final boolean binary;
    private transient Data lastReturned;

    public QueueIterator(Iterator<Data> iterator, NodeEngine nodeEngine, int partitionId, String name, boolean binary) {
        this.iterator = iterator;
        this.nodeEngine = nodeEngine;
        this.partitionId = partitionId;
        this.name = name;
        this.serializationService = nodeEngine.getSerializationService();
        this.binary = binary;
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public E next() {
        lastReturned = iterator.next();
        if (binary) {
            return (E) lastReturned;
        }
        return (E) serializationService.toObject(lastReturned);
    }

    public void remove() {
        iterator.remove();
        final OperationService operationService = nodeEngine.getOperationService();
        final RemoveOperation removeOperation = new RemoveOperation(name, lastReturned);
        final InternalCompletableFuture<Object> future = operationService.invokeOnPartition(
                QueueService.SERVICE_NAME,
                removeOperation,
                partitionId
        );
        future.getSafely();
    }
}
