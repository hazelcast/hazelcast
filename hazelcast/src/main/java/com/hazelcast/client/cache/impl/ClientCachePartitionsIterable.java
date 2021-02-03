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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.AbstractCachePartitionsIterable;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.client.impl.spi.ClientContext;
import org.jetbrains.annotations.NotNull;

import javax.cache.Cache;
import java.util.Iterator;

public class ClientCachePartitionsIterable<K, V> extends AbstractCachePartitionsIterable<K, V> {
    protected final ICacheInternal<K, V> cacheProxy;
    protected final ClientContext context;

    public ClientCachePartitionsIterable(ICacheInternal<K, V> cacheProxy,
                                         ClientContext context,
                                         int fetchSize,
                                         boolean prefetchValues) {
        super(fetchSize, prefetchValues);
        this.cacheProxy = cacheProxy;
        this.context = context;

    }

    public ClientCachePartitionsIterable(ICacheInternal<K, V> cacheProxy, ClientContext context, boolean prefetchValues) {
        this(cacheProxy, context, DEFAULT_FETCH_SIZE, prefetchValues);
    }

    @NotNull
    @Override
    public Iterator<Cache.Entry<K, V>> iterator() {
        return new ClientCachePartitionsIterator<>(cacheProxy, context, fetchSize, prefetchValues);
    }
}
