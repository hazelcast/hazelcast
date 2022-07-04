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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.journal.CacheEventJournalSubscribeOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalSubscribeCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;


/**
 * Performs the initial subscription to the cache event journal.
 * This includes retrieving the event journal sequences of the
 * oldest and newest event in the journal.
 *
 * @see com.hazelcast.cache.impl.CacheProxy#subscribeToEventJournal
 * @see CacheEventJournalSubscribeOperation
 * @since 3.9
 */
public class CacheEventJournalSubscribeTask
        extends AbstractCacheMessageTask<String> {

    public CacheEventJournalSubscribeTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheEventJournalSubscribeOperation(parameters);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return CacheEventJournalSubscribeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        final EventJournalInitialSubscriberState state = (EventJournalInitialSubscriberState) response;
        return CacheEventJournalSubscribeCodec.encodeResponse(state.getOldestSequence(), state.getNewestSequence());
    }

    @Override
    public final String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new CachePermission(parameters, ActionConstants.ACTION_LISTEN);
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
