/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Base class for iterating map entries in the whole cluster.
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class AbstractMapIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    protected static final int DEFAULT_FETCH_SIZE = 100;
    private final int size;
    private final List<Iterator<Map.Entry<K, V>>> partitionIterators;

    private Iterator<Map.Entry<K, V>> it;
    private int idx;

    public AbstractMapIterator(List<Iterator<Map.Entry<K, V>>> partitionIterators) {
        this.partitionIterators = partitionIterators;
        this.size = partitionIterators.size();
        it = partitionIterators.get(idx);
    }

    @Override
    public Map.Entry<K, V> next() {
        if (hasNext()) {
            return it.next();
        }
        throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
        while (!it.hasNext()) {
            if (idx == size - 1) {
                return false;
            }
            it = partitionIterators.get(++idx);
        }
        return true;
    }

    @Override
    public void remove() {
        while (idx >= 0) {
            try {
                it.remove();
                break;
            } catch (IllegalStateException e) {
                if (idx == 0) {
                    throw new IllegalStateException("Iterator.next() must be called before remove()!");
                }
                it = partitionIterators.get(--idx);
            }
        }
    }
}

