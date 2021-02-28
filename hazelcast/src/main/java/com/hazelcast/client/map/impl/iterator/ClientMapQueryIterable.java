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
import com.hazelcast.map.impl.iterator.MapQueryIterator;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Map;

/**
 * Client-side iterable that provides an iterator for iterating the result
 * of the projection on map entries in the whole cluster which satisfies the
 * predicate. It returns {@link ClientMapQueryIterator}.
 *
 * @see MapQueryIterator
 */
public class ClientMapQueryIterable<K, V, R> implements Iterable<R> {
    private final ClientMapProxy<K, V> clientMapProxy;
    private final Predicate<K, V> predicate;
    private final Projection<? super Map.Entry<K, V>, R> projection;
    private final int fetchSize;
    private final int partitionCount;

    public ClientMapQueryIterable(ClientMapProxy<K, V> clientMapProxy,
                                  int fetchSize, int partitionCount,
                                  Projection<? super Map.Entry<K, V>, R> projection, Predicate<K, V> predicate) {
        this.clientMapProxy = clientMapProxy;
        this.partitionCount = partitionCount;
        this.fetchSize = fetchSize;
        this.predicate = predicate;
        this.projection = projection;
    }

    @NotNull
    @Override
    public Iterator<R> iterator() {
        return new ClientMapQueryIterator<>(clientMapProxy, fetchSize, partitionCount, projection, predicate);
    }
}
