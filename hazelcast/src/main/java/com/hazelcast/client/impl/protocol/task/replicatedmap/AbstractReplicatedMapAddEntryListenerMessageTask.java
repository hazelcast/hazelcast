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

package com.hazelcast.client.impl.protocol.task.replicatedmap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedEntryEventFilter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedQueryEventFilter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import java.security.Permission;

public abstract class AbstractReplicatedMapAddEntryListenerMessageTask<Parameter>
        extends AbstractCallableMessageTask<Parameter>
        implements EntryListener<Object, Object> {

    public AbstractReplicatedMapAddEntryListenerMessageTask(ClientMessage clientMessage, Node node,
                                                            Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        ReplicatedMapService replicatedMapService = getService(ReplicatedMapService.SERVICE_NAME);
        String registrationId;
        Predicate predicate = getPredicate();
        if (predicate == null) {
            registrationId = replicatedMapService.addEventListener(this,
                    new ReplicatedEntryEventFilter(getKey()), getDistributedObjectName());
        } else {
            registrationId = replicatedMapService.addEventListener(this,
                    new ReplicatedQueryEventFilter(getKey(), predicate), getDistributedObjectName());
        }
        endpoint.setListenerRegistration(ReplicatedMapService.SERVICE_NAME, getDistributedObjectName(), registrationId);
        return registrationId;
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return "addEntryListener";
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_LISTEN);
    }

    public abstract Predicate getPredicate();

    public abstract Data getKey();

    private void handleEvent(EntryEvent<Object, Object> event) {
        if (endpoint.isAlive()) {
            Data key = serializationService.toData(event.getKey());
            Data newValue = serializationService.toData(event.getValue());
            Data oldValue = serializationService.toData(event.getOldValue());
            Data mergingValue = serializationService.toData(event.getMergingValue());

            ClientMessage clientMessage = encodeEvent(key
                    , newValue, oldValue, mergingValue, event.getEventType().getType(),
                    event.getMember().getUuid(), 1);
            sendClientMessage(key, clientMessage);
        }
    }

    protected abstract ClientMessage encodeEvent(Data key, Data newValue, Data oldValue,
                                                 Data mergingValue, int type, String uuid, int numberOfAffectedEntries);

    @Override
    public void entryAdded(EntryEvent<Object, Object> event) {
        handleEvent(event);
    }

    @Override
    public void entryRemoved(EntryEvent<Object, Object> event) {
        handleEvent(event);
    }

    @Override
    public void entryUpdated(EntryEvent<Object, Object> event) {
        handleEvent(event);
    }

    @Override
    public void entryEvicted(EntryEvent<Object, Object> event) {
        handleEvent(event);
    }

    @Override
    public void mapEvicted(MapEvent event) {
        // TODO handle this event
    }

    @Override
    public void mapCleared(MapEvent event) {
        // TODO handle this event
    }
}
