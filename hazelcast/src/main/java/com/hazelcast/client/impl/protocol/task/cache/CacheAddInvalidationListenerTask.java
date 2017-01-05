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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.SingleNearCacheInvalidation;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NotifiableEventListener;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec.encodeCacheBatchInvalidationEvent;
import static com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec.encodeCacheInvalidationEvent;

public class CacheAddInvalidationListenerTask
        extends AbstractCallableMessageTask<CacheAddInvalidationListenerCodec.RequestParameters> {

    public CacheAddInvalidationListenerTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        final ClientEndpoint endpoint = getEndpoint();
        CacheService cacheService = getService(CacheService.SERVICE_NAME);
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(parameters.name);
        CacheInvalidationEventListener listener = new CacheInvalidationEventListener(endpoint, cacheContext);
        String registrationId =
                cacheService.addInvalidationListener(parameters.name, listener, parameters.localOnly);
        endpoint.addListenerDestroyAction(CacheService.SERVICE_NAME, parameters.name, registrationId);
        return registrationId;
    }

    private final class CacheInvalidationEventListener implements CacheEventListener, NotifiableEventListener<CacheService> {

        private final ClientEndpoint endpoint;
        private final CacheContext cacheContext;

        /**
         * This listener is called by member and in the listener we are sending invalidations to client.
         * `batchOrderKey` is used by clients'-striped-executor to find a worker for processing invalidation.
         * By using `batchOrderKey` we are putting all invalidations coming from the same member into the same workers' queue.
         * So if there is more than one member all members will have their own worker to process invalidations. This provides
         * more granular processing.
         */
        private final int batchOrderKey = nodeEngine.getLocalMember().hashCode();

        private CacheInvalidationEventListener(ClientEndpoint endpoint, CacheContext cacheContext) {
            this.endpoint = endpoint;
            this.cacheContext = cacheContext;
        }

        @Override
        public void handleEvent(Object eventObject) {
            if (!endpoint.isAlive() || !(eventObject instanceof Invalidation)) {
                return;
            }

            sendInvalidation(((Invalidation) eventObject));
        }

        private void sendInvalidation(Invalidation invalidation) {
            if (invalidation instanceof BatchNearCacheInvalidation) {
                ExtractedParams params = extractParams(((BatchNearCacheInvalidation) invalidation));
                ClientMessage message = encodeCacheBatchInvalidationEvent(invalidation.getName(), params.keys,
                        params.sourceUuids, params.partitionUuids, params.sequences);

                sendClientMessage(batchOrderKey, message);
                return;
            }

            if (invalidation instanceof SingleNearCacheInvalidation) {
                ClientMessage message = encodeCacheInvalidationEvent(invalidation.getName(), invalidation.getKey(),
                        invalidation.getSourceUuid(), invalidation.getPartitionUuid(), invalidation.getSequence());

                sendClientMessage(invalidation.getKey(), message);
                return;
            }

            throw new IllegalArgumentException("Unknown invalidation message type " + invalidation);
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

        @Override
        public void onRegister(CacheService cacheService, String serviceName,
                               String topic, EventRegistration registration) {
            cacheContext.increaseInvalidationListenerCount();
        }

        @Override
        public void onDeregister(CacheService cacheService, String serviceName,
                                 String topic, EventRegistration registration) {
            cacheContext.decreaseInvalidationListenerCount();
        }

    }

    @Override
    protected CacheAddInvalidationListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddInvalidationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddInvalidationListenerCodec.encodeResponse((String) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

}
