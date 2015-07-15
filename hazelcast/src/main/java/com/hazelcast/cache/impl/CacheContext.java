/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds some specific informations for per cache in the node and shared by all partitions of that cache on the node.
 */
public class CacheContext {

    private final AtomicInteger cacheEntryListenerCount = new AtomicInteger(0);
    private final AtomicInteger invalidationListenerCount = new AtomicInteger(0);

    public int getCacheEntryListenerCount() {
        return cacheEntryListenerCount.get();
    }

    public void increaseCacheEntryListenerCount() {
        cacheEntryListenerCount.incrementAndGet();
    }

    public void decreaseCacheEntryListenerCount() {
        cacheEntryListenerCount.decrementAndGet();
    }

    public void resetCacheEntryListenerCount() {
        cacheEntryListenerCount.set(0);
    }

    public int getInvalidationListenerCount() {
        return invalidationListenerCount.get();
    }

    public void increaseInvalidationListenerCount() {
        invalidationListenerCount.incrementAndGet();
    }

    public void decreaseInvalidationListenerCount() {
        invalidationListenerCount.decrementAndGet();
    }

    public void resetInvalidationListenerCount() {
        invalidationListenerCount.set(0);
    }

    @Override
    public String toString() {
        return "CacheContext{"
                + "cacheEntryListenerCount=" + cacheEntryListenerCount.get()
                + ", invalidationListenerCount=" + invalidationListenerCount.get()
                + '}';
    }

}
