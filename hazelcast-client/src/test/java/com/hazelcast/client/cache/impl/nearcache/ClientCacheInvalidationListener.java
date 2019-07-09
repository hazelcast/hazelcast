/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.cache.impl.NearCachedClientCacheProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

class ClientCacheInvalidationListener
        extends CacheAddNearCacheInvalidationListenerCodec.AbstractEventHandler
        implements EventHandler<ClientMessage> {

    private final AtomicLong invalidationCount = new AtomicLong();
    private final List<String> singleInvalidationEventsLog = Collections.synchronizedList(new ArrayList<>());

    public long getInvalidationCount() {
        return invalidationCount.get();
    }

    public void resetInvalidationCount() {
        singleInvalidationEventsLog.clear();
        invalidationCount.set(0);
    }

    @Override
    public void beforeListenerRegister() {
    }

    @Override
    public void onListenerRegister() {
    }

    static ClientCacheInvalidationListener createInvalidationEventHandler(ICache clientCache) {
        ClientCacheInvalidationListener invalidationListener = new ClientCacheInvalidationListener();
        ((NearCachedClientCacheProxy) clientCache).addNearCacheInvalidationListener(invalidationListener);

        return invalidationListener;
    }

    @Override
    public void handleCacheInvalidationEventV10(String name, Data key, String sourceUuid) {
        invalidationCount.incrementAndGet();
    }

    @Override
    public void handleCacheInvalidationEventV14(String name, Data key, String sourceUuid, UUID partitionUuid, long sequence) {
        singleInvalidationEventsLog.add(name + ":" + sourceUuid + ":" + partitionUuid + ":" + sequence);
        invalidationCount.incrementAndGet();
    }

    @Override
    public void handleCacheBatchInvalidationEventV10(String name, Collection<Data> keys, Collection<String> sourceUuids) {
        invalidationCount.addAndGet(keys.size());
    }

    @Override
    public void handleCacheBatchInvalidationEventV14(String name, Collection<Data> keys, Collection<String> sourceUuids,
                                                     Collection<UUID> partitionUuids, Collection<Long> sequences) {
        invalidationCount.addAndGet(keys.size());
    }

    List<String> getSingleInvalidationEventsLog() {
        return singleInvalidationEventsLog;
    }
}
