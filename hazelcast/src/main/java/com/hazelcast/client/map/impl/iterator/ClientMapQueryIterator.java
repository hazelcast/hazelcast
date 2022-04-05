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
import com.hazelcast.map.impl.iterator.AbstractMapQueryIterator;
import com.hazelcast.map.impl.iterator.MapQueryPartitionIterator;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import java.util.Iterator;
import java.util.Map;

/**
 * Client-side iterator for iterating the result of the projection on
 * entries in the whole cluster which satisfy the {@code predicate}. The
 * values are fetched in batches. The {@link Iterator#remove()} method
 * is not supported and will throw a {@link UnsupportedOperationException}.
 * It uses {@link ClientMapQueryPartitionIterator} and provides same guarantees
 * with it.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 * @param <R> the return type of iterator after the projection
 * @see MapQueryPartitionIterator
 */
public class ClientMapQueryIterator<K, V, R> extends AbstractMapQueryIterator<R> {
    public ClientMapQueryIterator(ClientMapProxy<K, V> mapProxy, int fetchSize, int partitionCount,
                                  Projection<? super Map.Entry<K, V>, R> projection, Predicate<K, V> predicate) {
        super(partitionId -> mapProxy.iterator(fetchSize, partitionId, projection, predicate), partitionCount);
    }
}
