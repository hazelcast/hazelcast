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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapEventJournalSubscribeCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.journal.MapEventJournalSubscribeOperation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

/**
 * Performs the initial subscription to the map event journal.
 * This includes retrieving the event journal sequences of the
 * oldest and newest event in the journal.
 *
 * @see com.hazelcast.map.impl.proxy.MapProxyImpl#subscribeToEventJournal
 * @see com.hazelcast.map.impl.journal.MapEventJournalSubscribeOperation
 * @since 3.9
 */
public class MapEventJournalSubscribeTask
        extends AbstractMapPartitionMessageTask<String> {

    public MapEventJournalSubscribeTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new MapEventJournalSubscribeOperation(parameters);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return MapEventJournalSubscribeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        final EventJournalInitialSubscriberState state = (EventJournalInitialSubscriberState) response;
        return MapEventJournalSubscribeCodec.encodeResponse(state.getOldestSequence(), state.getNewestSequence());
    }

    @Override
    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(parameters, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return "subscribeToEventJournal";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{getPartitionId()};
    }
}
