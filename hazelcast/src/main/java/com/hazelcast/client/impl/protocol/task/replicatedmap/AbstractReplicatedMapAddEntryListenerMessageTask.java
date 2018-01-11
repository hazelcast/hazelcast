/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEventPublishingService;
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
        ReplicatedMapService service = getService(ReplicatedMapService.SERVICE_NAME);
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        String registrationId;
        Predicate predicate = getPredicate();
        if (predicate == null) {
            registrationId = eventPublishingService.addEventListener(this,
                    new ReplicatedEntryEventFilter(getKey()), getDistributedObjectName());
        } else {
            registrationId = eventPublishingService.addEventListener(this,
                    new ReplicatedQueryEventFilter(getKey(), predicate), getDistributedObjectName());
        }
        endpoint.addListenerDestroyAction(ReplicatedMapService.SERVICE_NAME, getDistributedObjectName(), registrationId);
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

    protected abstract boolean isLocalOnly();

    private void handleEvent(EntryEvent<Object, Object> event) {
        if (!shouldSendEvent(event)) {
            return;
        }

            Data key = serializationService.toData(event.getKey());
            Data newValue = serializationService.toData(event.getValue());
            Data oldValue = serializationService.toData(event.getOldValue());
            Data mergingValue = serializationService.toData(event.getMergingValue());

            ClientMessage clientMessage = encodeEvent(key
                    , newValue, oldValue, mergingValue, event.getEventType().getType(),
                    event.getMember().getUuid(), 1);
            sendClientMessage(key, clientMessage);
    }

    private void handleMapEvent(MapEvent event) {
        if (!shouldSendEvent(event)) {
            return;
        }

            ClientMessage clientMessage = encodeEvent(null
                    , null, null, null, event.getEventType().getType(),
                    event.getMember().getUuid(), event.getNumberOfEntriesAffected());
            sendClientMessage(null, clientMessage);
    }

    private boolean shouldSendEvent(IMapEvent event) {
        if (!endpoint.isAlive()) {
            return false;
        }

        Member originatedMember = event.getMember();
        if (isLocalOnly() && !nodeEngine.getLocalMember().equals(originatedMember)) {
            //if listener is registered local only, do not let the events originated from other members pass through
            return false;
        }
        return true;
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
        handleMapEvent(event);
    }
}
