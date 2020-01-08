/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.MapEntriesWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.holder.AnchorDataListHolder;
import com.hazelcast.client.impl.protocol.codec.holder.PagingPredicateHolder;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.serialization.Data;

import java.util.List;
import java.util.Map;

public class MapEntriesWithPagingPredicateMessageTask
        extends AbstractMapQueryWithPagingPredicateMessageTask<MapEntriesWithPagingPredicateCodec.RequestParameters> {

    public MapEntriesWithPagingPredicateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected PagingPredicateHolder getPagingPredicateHolder() {
        return parameters.predicate;
    }

    @Override
    protected IterationType getIterationType() {
        return IterationType.ENTRY;
    }

    @Override
    protected MapEntriesWithPagingPredicateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapEntriesWithPagingPredicateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Map.Entry<List<Map.Entry<Integer, Map.Entry>>, List<Map.Entry<Data, Data>>> result =
                (Map.Entry<List<Map.Entry<Integer, Map.Entry>>, List<Map.Entry<Data, Data>>>) response;
        AnchorDataListHolder anchorDataListHolder = AnchorDataListHolder.of(result.getKey(), serializationService);
        return MapEntriesWithPagingPredicateCodec.encodeResponse(result.getValue(), anchorDataListHolder);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.predicate};
    }


    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "entrySet";
    }

}

