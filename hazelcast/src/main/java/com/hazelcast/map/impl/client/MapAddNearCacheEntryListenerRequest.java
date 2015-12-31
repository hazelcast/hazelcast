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

package com.hazelcast.map.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.nearcache.BatchNearCacheInvalidation;
import com.hazelcast.map.impl.nearcache.Invalidation;
import com.hazelcast.map.impl.nearcache.InvalidationListener;
import com.hazelcast.map.impl.nearcache.SingleNearCacheInvalidation;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.map.impl.nearcache.NonStopInvalidator.getOrderKey;


/**
 * Request for adding {@link com.hazelcast.core.EntryListener} for near cache operations.
 */
public class MapAddNearCacheEntryListenerRequest extends MapAddEntryListenerRequest {

    public MapAddNearCacheEntryListenerRequest() {
    }

    public MapAddNearCacheEntryListenerRequest(String name, boolean includeValue) {
        super(name, includeValue, INVALIDATION.getType());
    }

    @Override
    public int getClassId() {
        return MapPortableHook.ADD_NEAR_CACHE_ENTRY_LISTENER;
    }

    @Override
    protected Object newMapListener(ClientEndpoint endpoint) {
        return new ClientNearCacheInvalidationListenerImpl(name, endpoint, getCallId());
    }

    private static class ClientNearCacheInvalidationListenerImpl implements InvalidationListener {

        private final long callId;
        private final String mapName;
        private final ClientEndpoint endpoint;

        ClientNearCacheInvalidationListenerImpl(String mapName, ClientEndpoint endpoint, long callId) {
            this.mapName = mapName;
            this.endpoint = endpoint;
            this.callId = callId;
        }

        @Override
        public void onInvalidate(Invalidation event) {
            if (!endpoint.isAlive()) {
                return;
            }

            event = getFilteredEventOrNull(event);
            sendEvent(event);
        }

        protected Invalidation getFilteredEventOrNull(Invalidation event) {

            if (event instanceof BatchNearCacheInvalidation) {
                return newEventOrNull(event);
            }

            if (!endpoint.getUuid().equals(event.getSourceUuid())) {
                return event;
            }

            return null;
        }

        protected BatchNearCacheInvalidation newEventOrNull(Invalidation event) {
            List<SingleNearCacheInvalidation> invalidations = ((BatchNearCacheInvalidation) event).getInvalidations();

            List<SingleNearCacheInvalidation> newList = null;
            for (SingleNearCacheInvalidation invalidation : invalidations) {
                if (!endpoint.getUuid().equals(invalidation.getSourceUuid())) {
                    if (newList == null) {
                        newList = new ArrayList<SingleNearCacheInvalidation>(invalidations.size());
                    }
                    newList.add(invalidation);
                }
            }
            return newList == null ? null : new BatchNearCacheInvalidation(mapName, newList);
        }

        protected void sendEvent(Invalidation event) {
            if (event == null) {
                return;
            }
            Object partitionKey = getOrderKey(mapName, event);
            endpoint.sendEvent(partitionKey, event, callId);
        }
    }
}
