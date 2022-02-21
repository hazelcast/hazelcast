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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.nearcache.invalidation.UuidFilter;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.eventservice.EventFilter;

import java.util.List;
import java.util.UUID;

/**
 * Clients which are AWARE of eventual consistent Near Cache should call this task.
 *
 * @see Pre38MapAddNearCacheEntryListenerMessageTask
 */
public class MapAddNearCacheInvalidationListenerMessageTask
        extends AbstractMapAddEntryListenerMessageTask<MapAddNearCacheInvalidationListenerCodec.RequestParameters> {

    public MapAddNearCacheInvalidationListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected boolean isLocalOnly() {
        return parameters.localOnly;
    }

    @Override
    protected ClientMessage encodeEvent(Data keyData, Data newValueData, Data oldValueData,
                                        Data meringValueData, int type, UUID uuid, int numberOfAffectedEntries) {
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
    protected MapAddNearCacheInvalidationListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddNearCacheInvalidationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddNearCacheInvalidationListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    protected Object newMapListener() {
        UUID uuid = nodeEngine.getLocalMember().getUuid();
        long correlationId = clientMessage.getCorrelationId();
        return new NearCacheInvalidationListener(endpoint, uuid, correlationId);
    }

    @Override
    protected EventFilter getEventFilter() {
        return new EventListenerFilter(parameters.listenerFlags, new UuidFilter(endpoint.getUuid()));
    }

    private final class NearCacheInvalidationListener extends AbstractMapClientNearCacheInvalidationListener {

        NearCacheInvalidationListener(ClientEndpoint endpoint, UUID localMemberUuid, long correlationId) {
            super(endpoint, localMemberUuid, correlationId);
        }

        @Override
        protected ClientMessage encodeBatchInvalidation(String name, List<Data> keys, List<UUID> sourceUuids,
                                                        List<UUID> partitionUuids, List<Long> sequences) {
            return MapAddNearCacheInvalidationListenerCodec.encodeIMapBatchInvalidationEvent(keys, sourceUuids,
                    partitionUuids, sequences);
        }

        @Override
        protected ClientMessage encodeSingleInvalidation(String name, Data key, UUID sourceUuid,
                                                         UUID partitionUuid, long sequence) {
            return MapAddNearCacheInvalidationListenerCodec.encodeIMapInvalidationEvent(key, sourceUuid, partitionUuid, sequence);
        }

        @Override
        protected void sendMessageWithOrderKey(ClientMessage clientMessage, Object orderKey) {
            sendClientMessage(orderKey, clientMessage);
        }

        @Override
        protected boolean canSendInvalidation(Invalidation invalidation) {
            return true;
        }
    }
}
