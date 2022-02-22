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

package com.hazelcast.map.impl.iterator;

import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Base class for iterating map entries in the whole cluster.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class AbstractMapIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    protected static final int DEFAULT_FETCH_SIZE = 100;
    private final int partitionCount;
    private Iterator<Map.Entry<K, V>> lastReadIterator;
    private final ConstructorFunction<Integer, Iterator<Map.Entry<K, V>>> createPartitionIterator;
    private Iterator<Map.Entry<K, V>> it;
    private int idx;

    public AbstractMapIterator(ConstructorFunction<Integer, Iterator<Map.Entry<K, V>>> createPartitionIterator,
                               int partitionCount) {
        this.createPartitionIterator = createPartitionIterator;
        this.partitionCount = partitionCount;
        idx = 0;
        it = createPartitionIterator.createNew(idx);
    }

    @Override
    public Map.Entry<K, V> next() {
        if (hasNext()) {
            if (it != lastReadIterator) {
                lastReadIterator = it;
            }
            return it.next();
        }
        throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
        while (!it.hasNext()) {
            if (idx == partitionCount - 1) {
                return false;
            }
            it = createPartitionIterator.createNew(++idx);
        }
        return true;
    }

    @Override
    public void remove() {
        if (lastReadIterator != null) {
            lastReadIterator.remove();
        } else {
            throw new IllegalStateException("Iterator.next() must be called before remove()!");
        }
    }
}
