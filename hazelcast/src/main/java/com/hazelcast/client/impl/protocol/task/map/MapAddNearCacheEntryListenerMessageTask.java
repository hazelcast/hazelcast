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
import com.hazelcast.map.impl.nearcache.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.map.impl.nearcache.invalidation.Invalidation;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationListener;
import com.hazelcast.map.impl.nearcache.invalidation.SingleNearCacheInvalidation;
import com.hazelcast.map.impl.nearcache.invalidation.UuidFilter;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec.encodeIMapBatchInvalidationEvent;
import static com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec.encodeIMapInvalidationEvent;
import static com.hazelcast.internal.nearcache.impl.invalidation.InvalidationUtils.NO_SEQUENCE;

/**
 * Deprecated class is here to provide backward compatibility.
 *
 * @see MapAddNearCacheInvalidationListenerMessageTask
 */
@Deprecated
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
        return new EventListenerFilter(parameters.listenerFlags, new UuidFilter(endpoint.getUuid()));
    }

    private final class ClientNearCacheInvalidationListenerImpl implements InvalidationListener {

        private ClientNearCacheInvalidationListenerImpl() {
        }

        private void sendEvent(Invalidation invalidation) {
            if (invalidation instanceof BatchNearCacheInvalidation) {
                List<Data> keys = getKeys((BatchNearCacheInvalidation) invalidation);
                sendClientMessage(parameters.name, encodeIMapBatchInvalidationEvent(keys, Collections.<String>emptyList(),
                        Collections.<UUID>emptyList(), Collections.<Long>emptyList()));

                return;
            }

            if (!getEndpoint().getUuid().equals(invalidation.getSourceUuid())) {
                if (invalidation instanceof SingleNearCacheInvalidation) {
                    sendClientMessage(parameters.name,
                            encodeIMapInvalidationEvent(invalidation.getKey(), invalidation.getSourceUuid(),
                                    invalidation.getPartitionUuid(), NO_SEQUENCE));
                }

                return;
            }
        }

        private List<Data> getKeys(BatchNearCacheInvalidation invalidation) {
            List<Invalidation> invalidations = invalidation.getInvalidations();
            List<Data> keys = new ArrayList<Data>(invalidations.size());
            for (Invalidation single : invalidations) {
                keys.add(single.getKey());
            }
            return keys;
        }

        @Override
        public void onInvalidate(Invalidation invalidation) {
            if (!endpoint.isAlive()) {
                return;
            }
            sendEvent(invalidation);
        }

    }
}
