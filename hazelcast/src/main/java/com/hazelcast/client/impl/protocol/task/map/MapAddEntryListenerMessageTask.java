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
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.eventservice.EventFilter;

import java.util.UUID;

public class MapAddEntryListenerMessageTask
        extends AbstractMapAddEntryListenerMessageTask<MapAddEntryListenerCodec.RequestParameters> {

    public MapAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected EventFilter getEventFilter() {
        EntryEventFilter eventFilter = new EntryEventFilter(null, parameters.includeValue);
        return new EventListenerFilter(parameters.listenerFlags, eventFilter);
    }

    @Override
    protected boolean isLocalOnly() {
        return parameters.localOnly;
    }

    @Override
    protected ClientMessage encodeEvent(Data keyData, Data newValueData, Data oldValueData,
                                        Data meringValueData, int type, UUID uuid, int numberOfAffectedEntries) {
        return MapAddEntryListenerCodec.encodeEntryEvent(keyData, newValueData,
                oldValueData, meringValueData, type, uuid, numberOfAffectedEntries);
    }

    @Override
    protected MapAddEntryListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddEntryListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddEntryListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null, parameters.includeValue};
    }


    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
