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

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;

/**
 * Client-side iterable that provides an iterator for iterating map
 * entries in the whole cluster.
 * It returns {@link ClientMapIterator}.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 * @see ClientMapIterator
 */
public class ClientMapIterable<K, V> implements Iterable<Map.Entry<K, V>> {
    private final ClientMapProxy<K, V> clientMapProxy;
    private final int fetchSize;
    private final int partitionCount;
    private final boolean prefetchValues;

    public ClientMapIterable(ClientMapProxy<K, V> clientMapProxy,
                             int fetchSize, int partitionCount,
                             boolean prefetchValues
    ) {
        this.clientMapProxy = clientMapProxy;
        this.partitionCount = partitionCount;
        this.fetchSize = fetchSize;
        this.prefetchValues = prefetchValues;
    }

    @Nonnull
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return new ClientMapIterator<>(clientMapProxy, fetchSize, partitionCount, prefetchValues);
    }
}
