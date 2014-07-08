/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.queue.client.RemoveRequest;
import com.hazelcast.util.ExceptionUtil;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * Iterator for the Queue.
 * @param <E>
 */
public class ClientQueueIterator<E> implements Iterator<E> {

    private final Iterator<Data> iterator;
    private final ClientContext context;
    private final String name;
    private final String partitionKey;
    private final SerializationService serializationService;
    private final boolean binary;
    private Data lastReturned;

    public ClientQueueIterator(Iterator<Data> iterator, ClientContext context, String name, String partitionKey, boolean binary) {
        this.iterator = iterator;
        this.context = context;
        this.name = name;
        this.partitionKey = partitionKey;
        this.serializationService = context.getSerializationService();
        this.binary = binary;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public E next() {
        lastReturned = iterator.next();
        if (binary) {
            return (E) lastReturned;
        }
        return (E) serializationService.toObject(lastReturned);
    }

    @Override
    public void remove() {
        iterator.remove();
        final ClientInvocationService invocationService = context.getInvocationService();
        final RemoveRequest removeRequest = new RemoveRequest(name, lastReturned);
        try {
            final Future future = invocationService.invokeOnKeyOwner(removeRequest, partitionKey);
            future.get();
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }

    }
}
