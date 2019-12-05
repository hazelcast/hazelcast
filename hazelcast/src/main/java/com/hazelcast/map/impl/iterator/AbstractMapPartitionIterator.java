/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.IMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Base class for iterating a partition. When iterating you can control:
 * <ul>
 * <li>the fetch size</li>
 * <li>whether values are prefetched or fetched when iterating</li>
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class AbstractMapPartitionIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    protected IMap<K, V> map;
    protected final int fetchSize;
    protected final int partitionId;
    protected boolean prefetchValues;

    /**
     * The table is segment table of hash map, which is an array that stores the actual records.
     * This field is used to mark where the latest entry is read.
     * <p>
     * The iteration will start from highest index available to the table.
     * It will be converted to array size on the server side.
     */
    protected int lastTableIndex = Integer.MAX_VALUE;

    protected int index;
    protected int currentIndex = -1;

    protected List result;

    public AbstractMapPartitionIterator(IMap<K, V> map, int fetchSize, int partitionId, boolean prefetchValues) {
        this.map = map;
        this.fetchSize = fetchSize;
        this.partitionId = partitionId;
        this.prefetchValues = prefetchValues;
    }

    @Override
    public boolean hasNext() {
        return (result != null && index < result.size()) || advance();
    }

    @Override
    public Map.Entry<K, V> next() {
        while (hasNext()) {
            currentIndex = index;
            index++;
            final Data keyData = getKey(currentIndex);
            final Object value = getValue(currentIndex, keyData);
            if (value != null) {
                return new LazyMapEntry(keyData, value, (InternalSerializationService) getSerializationService());
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        if (result == null || currentIndex < 0) {
            throw new IllegalStateException("Iterator.next() must be called before remove()!");
        }
        Data keyData = getKey(currentIndex);
        map.remove(keyData);
        currentIndex = -1;
    }

    protected boolean advance() {
        if (lastTableIndex < 0) {
            lastTableIndex = Integer.MAX_VALUE;
            return false;
        }
        result = fetch();
        if (result != null && result.size() > 0) {
            index = 0;
            return true;
        }
        return false;
    }

    protected void setLastTableIndex(List response, int lastTableIndex) {
        if (response != null && response.size() > 0) {
            this.lastTableIndex = lastTableIndex;
        }
    }

    protected abstract List fetch();

    protected abstract SerializationService getSerializationService();

    private Data getKey(int index) {
        if (result != null) {
            if (prefetchValues) {
                Map.Entry<Data, Data> entry = (Map.Entry<Data, Data>) result.get(index);
                return entry.getKey();
            } else {
                return (Data) result.get(index);
            }
        }
        return null;
    }

    private Object getValue(int index, Data keyData) {
        if (result != null) {
            if (prefetchValues) {
                Map.Entry<Data, Data> entry = (Map.Entry<Data, Data>) result.get(index);
                return entry.getValue();
            } else {
                return map.get(keyData);
            }
        }
        return null;
    }
}
