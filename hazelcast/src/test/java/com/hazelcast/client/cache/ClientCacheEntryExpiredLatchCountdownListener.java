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

package com.hazelcast.client.cache;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import java.util.concurrent.CountDownLatch;

public class ClientCacheEntryExpiredLatchCountdownListener<K, V> implements javax.cache.event.CacheEntryExpiredListener<K, V> {

    private static final transient CountDownLatch EXPIRED_LATCH = new CountDownLatch(2);

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
            throws CacheEntryListenerException {
        for (CacheEntryEvent<? extends K, ? extends V> event : cacheEntryEvents) {
            System.err.println("ClientCacheEntryExpiredLatchCountdownListener expired: " + event.getKey());
        }
        EXPIRED_LATCH.countDown();
    }

    public static CountDownLatch getExpiredLatch() {
        return EXPIRED_LATCH;
    }
}
