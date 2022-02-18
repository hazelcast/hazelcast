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

package com.hazelcast.cache.impl;


import javax.annotation.Nonnull;
import javax.cache.Cache;
import java.util.Iterator;

public abstract class AbstractCachePartitionIterable<K, V> implements Iterable<Cache.Entry<K, V>> {

    protected final int fetchSize;
    protected final boolean prefetchValues;

    public AbstractCachePartitionIterable(int fetchSize, boolean prefetchValues) {
        this.fetchSize = fetchSize;
        this.prefetchValues = prefetchValues;
    }

    @Override
    @Nonnull
    public abstract Iterator<Cache.Entry<K, V>> iterator();
}
