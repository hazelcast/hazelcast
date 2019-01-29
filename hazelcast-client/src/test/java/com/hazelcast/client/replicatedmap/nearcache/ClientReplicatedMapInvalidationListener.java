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

package com.hazelcast.client.replicatedmap.nearcache;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddNearCacheEntryListenerCodec;
import com.hazelcast.client.proxy.ClientReplicatedMapProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.internal.nearcache.NearCacheInvalidationListener;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.atomic.AtomicLong;

class ClientReplicatedMapInvalidationListener
        extends ReplicatedMapAddNearCacheEntryListenerCodec.AbstractEventHandler
        implements NearCacheInvalidationListener, EventHandler<ClientMessage> {

    private final AtomicLong invalidationCount = new AtomicLong();
    private final ReplicatedMap clientMap;

    private ClientReplicatedMapInvalidationListener(ReplicatedMap clientMap) {
        this.clientMap = clientMap;
    }

    @Override
    public long getInvalidationCount() {
        return invalidationCount.get();
    }

    @Override
    public void resetInvalidationCount() {
        invalidationCount.set(0);
    }

    @Override
    public void handle(Data dataKey, Data value, Data oldValue, Data mergingValue, int eventType, String uuid,
                       int numberOfAffectedEntries) {
        EntryEventType entryEventType = EntryEventType.getByType(eventType);
        switch (entryEventType) {
            case ADDED:
            case REMOVED:
            case UPDATED:
            case EVICTED:
                invalidationCount.incrementAndGet();
                break;
            case CLEAR_ALL:
                invalidationCount.addAndGet(clientMap.size());
                break;
            default:
                throw new IllegalArgumentException("Not a known event type " + entryEventType);
        }
    }

    @Override
    public void beforeListenerRegister() {
    }

    @Override
    public void onListenerRegister() {
    }

    static NearCacheInvalidationListener createInvalidationEventHandler(ReplicatedMap clientMap) {
        NearCacheInvalidationListener invalidationListener = new ClientReplicatedMapInvalidationListener(clientMap);
        ((ClientReplicatedMapProxy) clientMap).addNearCacheInvalidationListener((EventHandler) invalidationListener);

        return invalidationListener;
    }
}
