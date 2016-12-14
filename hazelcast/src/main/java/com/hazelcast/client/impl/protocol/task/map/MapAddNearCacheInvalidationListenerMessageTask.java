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
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationListener;
import com.hazelcast.map.impl.nearcache.invalidation.UuidFilter;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec.encodeIMapBatchInvalidationEvent;
import static com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec.encodeIMapInvalidationEvent;

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
    protected MapAddNearCacheInvalidationListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddNearCacheInvalidationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddNearCacheInvalidationListenerCodec.encodeResponse((String) response);
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

        @Override
        public void onInvalidate(Invalidation invalidation) {
            if (!endpoint.isAlive()) {
                return;
            }

            String mapName = invalidation.getName();
            Object orderKey = mapName;
            ClientMessage message;

            if (invalidation instanceof BatchNearCacheInvalidation) {
                ExtractedParams params = extractParams(((BatchNearCacheInvalidation) invalidation));
                message = encodeIMapBatchInvalidationEvent(params.keys, params.sourceUuids,
                        params.partitionUuids, params.sequences);
            } else {
                Data key = invalidation.getKey();
                message = encodeIMapInvalidationEvent(key, invalidation.getSourceUuid(),
                        invalidation.getPartitionUuid(), invalidation.getSequence());
                orderKey = key;
            }

            sendClientMessage(orderKey, message);
        }

        private ExtractedParams extractParams(BatchNearCacheInvalidation batch) {
            List<Invalidation> invalidations = batch.getInvalidations();

            int size = invalidations.size();
            List<Data> keys = new ArrayList<Data>(size);
            List<String> sourceUuids = new ArrayList<String>(size);
            List<UUID> partitionUuids = new ArrayList<UUID>(size);
            List<Long> sequences = new ArrayList<Long>(size);

            for (Invalidation invalidation : invalidations) {
                keys.add(invalidation.getKey());
                sourceUuids.add(invalidation.getSourceUuid());
                partitionUuids.add(invalidation.getPartitionUuid());
                sequences.add(invalidation.getSequence());
            }

            return new ExtractedParams(keys, sourceUuids, partitionUuids, sequences);
        }

        private final class ExtractedParams {
            private final List<Data> keys;
            private final List<String> sourceUuids;
            private final List<UUID> partitionUuids;
            private final List<Long> sequences;

            public ExtractedParams(List<Data> keys, List<String> sourceUuids,
                                   List<UUID> partitionUuids, List<Long> sequences) {
                this.keys = keys;
                this.sourceUuids = sourceUuids;
                this.partitionUuids = partitionUuids;
                this.sequences = sequences;
            }
        }
    }
}
