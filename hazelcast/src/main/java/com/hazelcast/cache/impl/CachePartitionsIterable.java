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

package com.hazelcast.cache.impl;

import org.jetbrains.annotations.NotNull;

import javax.cache.Cache;
import java.util.Iterator;

public class CachePartitionsIterable<K, V> extends AbstractCachePartitionsIterable<K, V> {
    protected final CacheProxy<K, V> cacheProxy;

    public CachePartitionsIterable(CacheProxy<K, V> cacheProxy, int fetchSize, boolean prefetchValues) {
        super(fetchSize, prefetchValues);
        this.cacheProxy = cacheProxy;
    }

    public CachePartitionsIterable(CacheProxy<K, V> cacheProxy, boolean prefetchValues) {
        this(cacheProxy, DEFAULT_FETCH_SIZE, prefetchValues);
    }

    @NotNull
    @Override
    public Iterator<Cache.Entry<K, V>> iterator() {
        return new CachePartitionsIterator<>(cacheProxy, fetchSize, prefetchValues);
    }
}
