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
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationHandler;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationListener;
import com.hazelcast.map.impl.nearcache.invalidation.SingleNearCacheInvalidation;
import com.hazelcast.map.impl.nearcache.invalidation.UuidFilter;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec.encodeIMapBatchInvalidationEvent;
import static com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec.encodeIMapInvalidationEvent;

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

    private final class ClientNearCacheInvalidationListenerImpl implements InvalidationListener, InvalidationHandler {

        private ClientNearCacheInvalidationListenerImpl() {
        }

        @Override
        public void onInvalidate(Invalidation invalidation) {
            if (!endpoint.isAlive()) {
                return;
            }

            invalidation.consumedBy(this);
        }

        @Override
        public void handle(BatchNearCacheInvalidation batchNearCacheInvalidation) {
            List<Data> keys = getKeysOrNull(batchNearCacheInvalidation);

            if (keys == null) {
                sendClientMessage(parameters.name, encodeIMapInvalidationEvent(null));
            } else {
                sendClientMessage(parameters.name, encodeIMapBatchInvalidationEvent(keys));
            }
        }

        /**
         * Returns null if invalidation is caused by a clear event, otherwise returns key-list to invalidate.
         * Returning null is a special case and represents a near-cache clear event.
         *
         * @see SingleNearCacheInvalidation
         */
        private List<Data> getKeysOrNull(BatchNearCacheInvalidation batch) {
            List<SingleNearCacheInvalidation> invalidations = batch.getInvalidations();

            List<Data> keyList = null;
            for (SingleNearCacheInvalidation invalidation : invalidations) {
                Data key = invalidation.getKey();
                // if key is null, it means this is a clear event and no need to process
                // other invalidations.
                if (key == null) {
                    return null;
                }

                if (keyList == null) {
                    keyList = new ArrayList<Data>(invalidations.size());
                }
                keyList.add(key);
            }

            assert keyList != null;

            return keyList;
        }


        @Override
        public void handle(SingleNearCacheInvalidation singleNearCacheInvalidation) {
            Data key = singleNearCacheInvalidation.getKey();

            if (key == null) {
                sendClientMessage(parameters.name, encodeIMapInvalidationEvent(null));
            } else {
                sendClientMessage(key, encodeIMapInvalidationEvent(key));
            }
        }

    }
}
