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

import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;

/**
 * Iterable that provides an iterator for iterating the result of the projection
 * on map entries in the specified partition which satisfies the predicate.
 * @see MapQueryPartitionIterator
 */
public class MapQueryPartitionIterable<K, V, R> implements Iterable<R> {
    private final MapProxyImpl<K, V> mapProxy;
    private final Predicate<K, V> predicate;
    private final Projection<? super Map.Entry<K, V>, R> projection;
    private final int fetchSize;
    private final int partitionId;

    public MapQueryPartitionIterable(
            MapProxyImpl<K, V> mapProxy,
            int fetchSize, int partitionId,
            Projection<? super Map.Entry<K, V>, R> projection, Predicate<K, V> predicate
    ) {
        this.mapProxy = mapProxy;
        this.partitionId = partitionId;
        this.fetchSize = fetchSize;
        this.predicate = predicate;
        this.projection = projection;
    }

    @Nonnull
    @Override
    public Iterator<R> iterator() {
        return mapProxy.iterator(fetchSize, partitionId, projection, predicate);
    }
}
