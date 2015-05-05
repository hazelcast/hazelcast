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

import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.CacheEventSet;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AddListenerResultParameters;
import com.hazelcast.client.impl.protocol.parameters.CacheAddEntryListenerParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;

import java.security.Permission;
import java.util.Set;

/**
 * Client request which registers an event listener on behalf of the client and delegates the received events
 * back to client.
 *
 * @see CacheService#registerListener(String, CacheEventListener)
 */
public class CacheAddEntryListenerMessageTask
        extends AbstractCallableMessageTask<CacheAddEntryListenerParameters> {

    public CacheAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() {
        final ClientEndpoint endpoint = getEndpoint();
        final CacheService service = getService(CacheService.SERVICE_NAME);
        CacheEventListener entryListener = new CacheEventListener() {
            @Override
            public void handleEvent(Object eventObject) {
                if (endpoint.isAlive()) {
                    Data partitionKey = getPartitionKey(eventObject);
                    endpoint.sendEvent(partitionKey, eventObject, clientMessage.getCorrelationId());
                }
            }
        };
        final String registrationId = service.registerListener(parameters.name, entryListener);
        return AddListenerResultParameters.encode(registrationId);
    }

    private Data getPartitionKey(Object eventObject) {
        Data partitionKey = null;
        if (eventObject instanceof CacheEventSet) {
            Set<CacheEventData> events = ((CacheEventSet) eventObject).getEvents();
            if (events.size() > 1) {
                partitionKey = new DefaultData();
            } else if (events.size() == 1) {
                partitionKey = events.iterator().next().getDataKey();
            }
        } else if (eventObject instanceof CacheEventData) {
            partitionKey = ((CacheEventData) eventObject).getDataKey();
        }
        return partitionKey;
    }

    @Override
    protected CacheAddEntryListenerParameters decodeClientMessage(ClientMessage clientMessage) {
        return null;
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
