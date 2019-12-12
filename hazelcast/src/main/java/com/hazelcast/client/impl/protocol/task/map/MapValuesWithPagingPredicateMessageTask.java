/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.MapKeySetWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.holder.AnchorDataListHolder;
import com.hazelcast.client.impl.protocol.codec.holder.PagingPredicateHolder;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MapValuesWithPagingPredicateMessageTask
        extends AbstractMapQueryWithPagingPredicateMessageTask<MapValuesWithPagingPredicateCodec.RequestParameters> {

    public MapValuesWithPagingPredicateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object reduce(Collection<QueryResultRow> result) {
        PagingPredicateImpl pagingPredicate = (PagingPredicateImpl) getPredicate();
        List<Map.Entry<Data, Data>> entriesData = getSortedPageEntries(result, pagingPredicate);

        List<Data> valueList = new ArrayList<>(entriesData.size());
        entriesData.forEach(entry -> valueList.add(entry.getValue()));

        return new AbstractMap.SimpleImmutableEntry(pagingPredicate.getAnchorList(), valueList);
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
    protected MapValuesWithPagingPredicateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapValuesWithPagingPredicateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Map.Entry<List<Map.Entry<Integer, Map.Entry>>, List<Data>> result =
                (Map.Entry<List<Map.Entry<Integer, Map.Entry>>, List<Data>>) response;
        AnchorDataListHolder anchorDataListHolder = AnchorDataListHolder.of(result.getKey(), serializationService);
        return MapKeySetWithPagingPredicateCodec.encodeResponse(result.getValue(), anchorDataListHolder);
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
        return "values";
    }
}
