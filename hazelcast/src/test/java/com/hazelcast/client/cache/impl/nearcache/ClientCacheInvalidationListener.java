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

package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.internal.serialization.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

class ClientCacheInvalidationListener
        extends CacheAddNearCacheInvalidationListenerCodec.AbstractEventHandler
        implements EventHandler<ClientMessage> {

    private final AtomicLong invalidationCount = new AtomicLong();
    private final List<String> singleInvalidationEventsLog = new ArrayList<>();

    public long getInvalidationCount() {
        return invalidationCount.get();
    }

    public void resetInvalidationCount() {
        synchronized (singleInvalidationEventsLog) {
            singleInvalidationEventsLog.clear();
        }
        invalidationCount.set(0);
    }

    static ClientCacheInvalidationListener createInvalidationEventHandler(ICache clientCache) {
        ClientCacheInvalidationListener invalidationListener = new ClientCacheInvalidationListener();
        ((NearCachedClientCacheProxy) clientCache).addNearCacheInvalidationListener(invalidationListener);

        return invalidationListener;
    }

    @Override
    public void handleCacheInvalidationEvent(String name, Data key, UUID sourceUuid, UUID partitionUuid, long sequence) {
        synchronized (singleInvalidationEventsLog) {
            singleInvalidationEventsLog.add(name + ":" + sourceUuid + ":" + partitionUuid + ":" + sequence);
        }
        invalidationCount.incrementAndGet();
    }

    @Override
    public void handleCacheBatchInvalidationEvent(String name, Collection<Data> keys, Collection<UUID> sourceUuids,
                                                  Collection<UUID> partitionUuids, Collection<Long> sequences) {
        invalidationCount.addAndGet(keys.size());
    }

    // used in tests
    List<String> getSingleInvalidationEventsLog() {
        synchronized (singleInvalidationEventsLog) {
            return new ArrayList<>(singleInvalidationEventsLog);
        }
    }
}
