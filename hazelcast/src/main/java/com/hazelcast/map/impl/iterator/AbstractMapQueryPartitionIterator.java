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
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterationType;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.hazelcast.internal.util.CollectionUtil.isNotEmpty;

/**
 * Base class for iterating a partition with a {@link Predicate} and a {@link Projection}.
 * When iterating you can control:
 * <ul>
 * <li>the fetch size</li>
 * <li>whether a projection was applied to the entries</li>
 * <li>whether a predicate was applied to the entries</li>
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @param <R> in the case {@link #query} is null, this is {@code Map.Entry<K,V>}, otherwise it is the return type of
 *            the projection
 */
public abstract class AbstractMapQueryPartitionIterator<K, V, R> implements Iterator<R> {
    protected final IMap<K, V> map;
    protected final int fetchSize;
    protected final int partitionId;
    protected final Query query;

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

    protected List<Data> segment;

    public AbstractMapQueryPartitionIterator(IMap<K, V> map,
                                             int fetchSize,
                                             int partitionId,
                                             Predicate<K, V> predicate,
                                             Projection<? super Entry<K, V>, R> projection) {
        this.map = map;
        this.fetchSize = fetchSize;
        this.partitionId = partitionId;
        this.query = Query.of()
                          .mapName(map.getName())
                          .iterationType(IterationType.VALUE)
                          .predicate(predicate)
                          .projection(projection)
                          .build();
    }

    @Override
    public boolean hasNext() {
        return (segment != null && index < segment.size()) || advance();
    }

    @Override
    public R next() {
        if (hasNext()) {
            currentIndex = index;
            index++;
            return getSerializationService().toObject(getQueryResult(currentIndex));
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing when iterating map with query is not supported");
    }

    protected boolean advance() {
        if (lastTableIndex < 0) {
            lastTableIndex = Integer.MAX_VALUE;
            return false;
        }
        segment = fetch();
        if (isNotEmpty(segment)) {
            index = 0;
            return true;
        }
        return false;
    }

    protected void setLastTableIndex(List<Data> segment, int lastTableIndex) {
        if (isNotEmpty(segment)) {
            this.lastTableIndex = lastTableIndex;
        }
    }

    protected abstract List<Data> fetch();

    protected abstract SerializationService getSerializationService();

    private Data getQueryResult(int index) {
        if (segment != null) {
            return segment.get(index);
        }
        return null;
    }
}
