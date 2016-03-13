/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.nearcache.BatchNearCacheInvalidation;
import com.hazelcast.map.impl.nearcache.CleaningNearCacheInvalidation;
import com.hazelcast.map.impl.nearcache.Invalidation;
import com.hazelcast.map.impl.nearcache.InvalidationListener;
import com.hazelcast.map.impl.nearcache.SingleNearCacheInvalidation;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;

import java.util.List;

import static com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec.encodeIMapBatchInvalidationEvent;
import static com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec.encodeIMapInvalidationEvent;
import static com.hazelcast.map.impl.nearcache.BatchInvalidator.getKeysExcludingSource;
import static com.hazelcast.util.CollectionUtil.isEmpty;

public class MapAddNearCacheEntryListenerMessageTask
        extends AbstractMapAddEntryListenerMessageTask<MapAddNearCacheEntryListenerCodec.RequestParameters> {

    public MapAddNearCacheEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected boolean isLocalOnly() {
        return parameters.localOnly;
    }

    @Override
    protected ClientMessage encodeEvent(Data keyData, Data newValueData, Data oldValueData,
                                        Data meringValueData, int type, String uuid, int numberOfAffectedEntries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    protected MapAddNearCacheEntryListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddNearCacheEntryListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddNearCacheEntryListenerCodec.encodeResponse((String) response);
    }

    @Override
    protected Object newMapListener() {
        return new ClientNearCacheInvalidationListenerImpl();
    }

    @Override
    protected EventFilter getEventFilter() {
        return new EventListenerFilter(parameters.listenerFlags, TrueEventFilter.INSTANCE);
    }

    private final class ClientNearCacheInvalidationListenerImpl implements InvalidationListener {

        ClientNearCacheInvalidationListenerImpl() {
        }

        @Override
        public void onInvalidate(Invalidation event) {
            if (!endpoint.isAlive()) {
                return;
            }

            sendEvent(event);
        }

        private void sendEvent(Invalidation event) {
            if (event instanceof BatchNearCacheInvalidation) {
                List<Data> keys = getKeysExcludingSource(((BatchNearCacheInvalidation) event), endpoint.getUuid());
                if (!isEmpty(keys)) {
                    sendClientMessage(parameters.name, encodeIMapBatchInvalidationEvent(keys));
                }
            } else if (!endpoint.getUuid().equals(event.getSourceUuid())) {
                if (event instanceof SingleNearCacheInvalidation) {
                    Data key = ((SingleNearCacheInvalidation) event).getKey();
                    sendClientMessage(key, encodeIMapInvalidationEvent(key));
                } else if (event instanceof CleaningNearCacheInvalidation) {
                    sendClientMessage(parameters.name, encodeIMapInvalidationEvent(null));
                }
            }
        }
    }
}
