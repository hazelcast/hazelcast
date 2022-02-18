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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.Iterator;

/**
 * Iterator for the Queue.
 *
 * @param <E> the type of elements in the queue.
 */
public class QueueIterator<E> implements Iterator<E> {

    private final Iterator<Data> iterator;
    private final SerializationService serializationService;
    private final boolean binary;

    public QueueIterator(Iterator<Data> iterator, SerializationService serializationService, boolean binary) {
        this.iterator = iterator;
        this.serializationService = serializationService;
        this.binary = binary;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public E next() {
        Data item = iterator.next();
        if (binary) {
            return (E) item;
        }
        return (E) serializationService.toObject(item);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported!");
    }
}
