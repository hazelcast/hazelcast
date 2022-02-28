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

package com.hazelcast.client.map.impl.iterator;

import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;

/**
 * Client-side iterable that provides an iterator for iterating map
 * entries in the specified partition.
 * It returns {@link ClientMapQueryPartitionIterator}.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 * @param <R> the return type of iterator after the projection
 * @see ClientMapQueryPartitionIterator
 */
public class ClientMapQueryPartitionIterable<K, V, R> implements Iterable<R> {
    private final ClientMapProxy<K, V> clientMapProxy;
    private final Predicate<K, V> predicate;
    private final Projection<? super Map.Entry<K, V>, R> projection;
    private final int fetchSize;
    private final int partitionId;

    public ClientMapQueryPartitionIterable(
            ClientMapProxy<K, V> clientMapProxy,
            int fetchSize,
            int partitionId,
            Projection<? super Map.Entry<K, V>, R> projection, Predicate<K, V> predicate
    ) {
        this.clientMapProxy = clientMapProxy;
        this.partitionId = partitionId;
        this.fetchSize = fetchSize;
        this.predicate = predicate;
        this.projection = projection;
    }

    @Nonnull
    @Override
    public Iterator<R> iterator() {
        return clientMapProxy.iterator(fetchSize, partitionId, projection, predicate);
    }
}
