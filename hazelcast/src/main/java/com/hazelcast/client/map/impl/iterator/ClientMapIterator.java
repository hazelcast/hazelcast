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

package com.hazelcast.client.map.impl.iterator;


import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.map.impl.iterator.AbstractMapIterator;
import com.hazelcast.map.impl.iterator.MapPartitionIterator;

/**
 * Client-side iterator for iterating map entries in the whole cluster. The values
 * are fetched in batches.
 * It uses {@link MapPartitionIterator} and provides the same guarantees
 * with this partition iterator.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 * @see MapPartitionIterator
 */
public class ClientMapIterator<K, V> extends AbstractMapIterator<K, V> {

    public ClientMapIterator(ClientMapProxy<K, V> clientMapProxy, int fetchSize, int partitionCount, boolean prefetchValues) {
        super(partitionId -> clientMapProxy.iterator(fetchSize, partitionId, prefetchValues), partitionCount);
    }

    public ClientMapIterator(ClientMapProxy<K, V> clientMapProxy, int partitionCount, boolean prefetchValues) {
        this(clientMapProxy, DEFAULT_FETCH_SIZE, partitionCount, prefetchValues);
    }
}

