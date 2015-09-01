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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.cache.impl.event.InternalCachePartitionLostListenerAdapter;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;

import java.security.Permission;

public class CacheAddPartitionLostListenerMessageTask
        extends AbstractCallableMessageTask<CacheAddPartitionLostListenerCodec.RequestParameters> {


    public CacheAddPartitionLostListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        final ClientEndpoint endpoint = getEndpoint();

        final CachePartitionLostListener listener = new CachePartitionLostListener() {
            @Override
            public void partitionLost(CachePartitionLostEvent event) {
                if (endpoint.isAlive()) {
                    ClientMessage eventMessage =
                            CacheAddPartitionLostListenerCodec.encodeCachePartitionLostEvent(event.getPartitionId(),
                                   event.getMember().getUuid());
                    sendClientMessage(null, eventMessage);
                }
            }
        };

        final InternalCachePartitionLostListenerAdapter listenerAdapter =
                new InternalCachePartitionLostListenerAdapter(listener);
        final EventFilter filter = new CachePartitionLostEventFilter();
        final CacheService service = getService(CacheService.SERVICE_NAME);
        final EventRegistration registration = service.getNodeEngine().
                getEventService().registerListener(AbstractCacheService.SERVICE_NAME,
                parameters.name, filter, listenerAdapter);
        final String registrationId = registration.getId();
        endpoint.addListenerDestroyAction(CacheService.SERVICE_NAME, parameters.name, registrationId);
        return registrationId;

    }

    @Override
    protected CacheAddPartitionLostListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddPartitionLostListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddPartitionLostListenerCodec.encodeResponse((String) response);
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return "addCachePartitionLostListener";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
