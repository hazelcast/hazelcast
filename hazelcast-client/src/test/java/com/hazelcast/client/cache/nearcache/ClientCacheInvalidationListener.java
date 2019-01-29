/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cache.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.cache.impl.NearCachedClientCacheProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.internal.nearcache.NearCacheInvalidationListener;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ClientCacheInvalidationListener
        extends CacheAddNearCacheInvalidationListenerCodec.AbstractEventHandler
        implements NearCacheInvalidationListener, EventHandler<ClientMessage> {

    private final AtomicLong invalidationCount = new AtomicLong();

    @Override
    public long getInvalidationCount() {
        return invalidationCount.get();
    }

    @Override
    public void resetInvalidationCount() {
        invalidationCount.set(0);
    }

    @Override
    public void handle(String name, Data key, String sourceUuid, UUID partitionUuid, long sequence) {
        invalidationCount.incrementAndGet();
    }

    @Override
    public void handle(String name, Collection<Data> keys, Collection<String> sourceUuids,
                       Collection<UUID> partitionUuids, Collection<Long> sequences) {
        invalidationCount.addAndGet(keys.size());
    }

    @Override
    public void beforeListenerRegister() {
    }

    @Override
    public void onListenerRegister() {
    }

    public static NearCacheInvalidationListener createInvalidationEventHandler(ICache clientCache) {
        EventHandler invalidationListener = new ClientCacheInvalidationListener();
        ((NearCachedClientCacheProxy) clientCache).addNearCacheInvalidationListener(invalidationListener);

        return (NearCacheInvalidationListener) invalidationListener;
    }
}
