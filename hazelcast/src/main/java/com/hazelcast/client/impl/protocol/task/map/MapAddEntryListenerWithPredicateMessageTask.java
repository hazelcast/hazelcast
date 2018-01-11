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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerWithPredicateCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.EventFilter;

public class MapAddEntryListenerWithPredicateMessageTask
        extends AbstractMapAddEntryListenerMessageTask<MapAddEntryListenerWithPredicateCodec.RequestParameters> {

    public MapAddEntryListenerWithPredicateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected EventFilter getEventFilter() {
        Predicate predicate = serializationService.toObject(parameters.predicate);
        QueryEventFilter eventFilter = new QueryEventFilter(parameters.includeValue, null, predicate);
        return new EventListenerFilter(parameters.listenerFlags, eventFilter);
    }

    @Override
    protected boolean isLocalOnly() {
        return parameters.localOnly;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    protected MapAddEntryListenerWithPredicateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddEntryListenerWithPredicateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddEntryListenerWithPredicateCodec.encodeResponse((String) response);
    }

    @Override
    protected ClientMessage encodeEvent(Data keyData, Data newValueData, Data oldValueData,
                                        Data meringValueData, int type, String uuid, int numberOfAffectedEntries) {
        return MapAddEntryListenerWithPredicateCodec.encodeEntryEvent(keyData, newValueData,
                oldValueData, meringValueData, type, uuid, numberOfAffectedEntries);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null, parameters.predicate, parameters.includeValue};
    }

}
